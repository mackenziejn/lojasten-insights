import os
import sqlite3
import tempfile
import pytest


SCHEMA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'db', 'schema.sql'))


def create_db_from_schema(tmp_path):
    db_path = str(tmp_path / 'test_vendas.db')
    with sqlite3.connect(db_path) as conn:
        conn.executescript(open(SCHEMA_PATH, 'r', encoding='utf-8').read())
    return db_path


def test_no_more_than_two_sellers(tmp_path):
    db = create_db_from_schema(tmp_path)
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    # setup loja and three vendedores
    cur.execute("INSERT INTO lojas(codigo_loja, nome_loja) VALUES ('L1','Loja 1')")
    cur.executemany("INSERT INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)",
                    [('V1', 'Vendedor 1'), ('V2', 'Vendedor 2'), ('V3', 'Vendedor 3')])

    # assign two sellers (should succeed)
    cur.execute("INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES ('L1','V1')")
    cur.execute("INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES ('L1','V2')")
    cur.execute("SELECT COUNT(*) FROM loja_vendedor WHERE codigo_loja='L1'")
    assert cur.fetchone()[0] == 2

    # attempt to add a third seller -> expect failure from trigger
    with pytest.raises(sqlite3.DatabaseError) as exc:
        cur.execute("INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES ('L1','V3')")
    msg = str(exc.value)
    assert ('no máximo 2 vendedores' in msg
        or 'Cada loja pode ter no máximo 2 vendedores' in msg
        or 'Loja está finalizada' in msg)

    conn.close()


def test_sale_requires_assigned_seller(tmp_path):
    db = create_db_from_schema(tmp_path)
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    # setup loja and vendedores
    cur.execute("INSERT INTO lojas(codigo_loja, nome_loja) VALUES ('L1','Loja 1')")
    cur.execute("INSERT INTO vendedores(codigo_vendedor, nome_vendedor) VALUES ('V1','Vendedor 1')")
    cur.execute("INSERT INTO vendedores(codigo_vendedor, nome_vendedor) VALUES ('V2','Vendedor 2')")
    cur.execute("INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES ('L1','V1')")

    # product
    cur.execute("INSERT INTO produtos(codigo_produto, nome_produto, valor_produto) VALUES ('P1','Produto 1', 10.0)")

    # insert a valid sale with V1 -> should succeed
    cur.execute("INSERT INTO vendas(id_cliente,nome_cliente,data_nascimento,cpf,codigo_produto,quantidade,data_venda,data_compra,codigo_loja,codigo_vendedor,valor_produto) VALUES (2,'C2','1990-01-01','0002','P1',1,'2025-09-25','2025-09-25','L1','V1',10.0)")
    cur.execute("SELECT COUNT(*) FROM vendas WHERE codigo_loja='L1' AND codigo_vendedor='V1'")
    assert cur.fetchone()[0] == 1

    # attempt to insert a sale where vendedor V2 is not assigned to L1 -> fail
    with pytest.raises(sqlite3.DatabaseError) as exc:
        cur.execute("INSERT INTO vendas(id_cliente,nome_cliente,data_nascimento,cpf,codigo_produto,quantidade,data_venda,data_compra,codigo_loja,codigo_vendedor,valor_produto) VALUES (1,'C','1990-01-01','0001','P1',1,'2025-09-25','2025-09-25','L1','V2',10.0)")
    assert 'Vendedor não está atribuído à loja informada' in str(exc.value)

    conn.close()


def test_deletion_blocked_when_finalized_and_unlock_allows(tmp_path):
    db = create_db_from_schema(tmp_path)
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    # setup loja and two vendedores
    cur.execute("INSERT INTO lojas(codigo_loja, nome_loja) VALUES ('L1','Loja 1')")
    cur.executemany("INSERT INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)",
                    [('V1', 'Vendedor 1'), ('V2', 'Vendedor 2')])

    # assign both (should finalize store)
    cur.execute("INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES ('L1','V1')")
    cur.execute("INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES ('L1','V2')")
    cur.execute("SELECT sellers_finalized FROM lojas WHERE codigo_loja='L1'")
    assert cur.fetchone()[0] == 1

    # attempt to delete mapping while finalized -> should fail
    with pytest.raises(sqlite3.DatabaseError) as exc:
        cur.execute("DELETE FROM loja_vendedor WHERE codigo_loja='L1' AND codigo_vendedor='V1'")
    assert 'finalizada' in str(exc.value) or 'finalizada; não' in str(exc.value)

    # unlock and delete should succeed
    cur.execute("UPDATE lojas SET sellers_finalized=0 WHERE codigo_loja='L1'")
    cur.execute("DELETE FROM loja_vendedor WHERE codigo_loja='L1' AND codigo_vendedor='V1'")
    cur.execute("SELECT COUNT(*) FROM loja_vendedor WHERE codigo_loja='L1'")
    assert cur.fetchone()[0] == 1

    conn.close()
