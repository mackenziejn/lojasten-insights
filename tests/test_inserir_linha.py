import os
import sqlite3
import tempfile
import json
import shutil
import pytest

from src import db_utils


@pytest.fixture
def temp_env(tmp_path, monkeypatch):
    # prepare a temp DB and reports dir
    db_file = tmp_path / 'vendas_test.db'
    reports_dir = tmp_path / 'reports'
    reports_dir.mkdir()

    # copy schema.sql to tmp and create DB
    schema_src = os.path.join('data', 'db', 'schema.sql')
    tmp_schema = tmp_path / 'schema.sql'
    shutil.copy(schema_src, tmp_schema)

    monkeypatch.setenv('PYTHONUNBUFFERED', '1')
    # override constants in db_utils
    monkeypatch.setattr(db_utils, 'DB_PATH', str(db_file))
    monkeypatch.setattr(db_utils, 'DUPLICATE_LOG', str(reports_dir / 'duplicates.log'))

    # create DB from schema
    db_utils.criar_tabela(schema_path=str(tmp_schema))

    return {
        'db': str(db_file),
        'reports': str(reports_dir)
    }


def test_missing_fields_defaults_and_insert(temp_env):
    # ensure inserir_linha fills defaults and inserts
    data = {
        'nome_cliente': 'Test User',
        'cpf': '000.000.000-00',
        'codigo_produto': 'P001',
        'quantidade': 1,
        'codigo_loja': 'LTEST',
        'codigo_vendedor': 'VTEST'
    }

    # create referenced loja and vendedor and map
    conn = sqlite3.connect(db_utils.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO lojas(codigo_loja, nome_loja) VALUES (?,?)", ('LTEST', 'Loja Test'))
    cur.execute("INSERT OR IGNORE INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)", ('VTEST', 'Vendedor Test'))
    cur.execute("INSERT OR IGNORE INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES (?,?)", ('LTEST', 'VTEST'))
    cur.execute("INSERT OR IGNORE INTO produtos(codigo_produto, nome_produto, valor_produto) VALUES (?,?,?)", ('P001', 'Produto 1', 10.0))
    conn.commit()
    conn.close()

    db_utils.inserir_linha(data)

    conn = sqlite3.connect(db_utils.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT cpf, codigo_loja, codigo_vendedor, data_venda FROM vendas WHERE cpf = ?", ('000.000.000-00',))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == '000.000.000-00'
    assert row[1] == 'LTEST'
    assert row[2] in ('VTEST',)
    # data_venda should have a default value (ISO date string)
    assert row[3] is not None and len(row[3]) > 0


def test_duplicate_cpf_logged_and_skipped(temp_env):
    # insert initial row
    initial = {
        'nome_cliente': 'Alice', 'cpf': '111.111.111-11', 'codigo_produto': 'P002', 'quantidade': 1,
        'codigo_loja': 'L2', 'codigo_vendedor': 'V2'
    }
    conn = sqlite3.connect(db_utils.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO lojas(codigo_loja, nome_loja) VALUES (?,?)", ('L2', 'Loja 2'))
    cur.execute("INSERT OR IGNORE INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)", ('V2', 'Vend 2'))
    cur.execute("INSERT OR IGNORE INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES (?,?)", ('L2', 'V2'))
    cur.execute("INSERT OR IGNORE INTO produtos(codigo_produto, nome_produto, valor_produto) VALUES (?,?,?)", ('P002', 'Produto 2', 5.0))
    conn.commit()
    conn.close()

    db_utils.inserir_linha(initial)
    # attempt to insert duplicate CPF
    db_utils.inserir_linha(initial)

    # check duplicates log
    with open(db_utils.DUPLICATE_LOG, 'r', encoding='utf-8') as f:
        lines = f.read().strip().splitlines()
    assert any('111.111.111-11' in ln for ln in lines)


def test_fallback_mapping(temp_env):
    # create loja with a different mapped vendedor
    conn = sqlite3.connect(db_utils.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO lojas(codigo_loja, nome_loja) VALUES (?,?)", ('LFB', 'Loja FB'))
    cur.execute("INSERT OR IGNORE INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)", ('V_A', 'A'))
    cur.execute("INSERT OR IGNORE INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)", ('V_B', 'B'))
    cur.execute("INSERT OR IGNORE INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES (?,?)", ('LFB', 'V_A'))
    cur.execute("INSERT OR IGNORE INTO produtos(codigo_produto, nome_produto, valor_produto) VALUES (?,?,?)", ('PX', 'Produto X', 1.0))
    conn.commit()
    conn.close()

    # attempt to insert a venda that references V_B (not mapped). The function should fallback to V_A
    # lock the loja so that inserting a new loja_vendedor mapping is blocked by the trigger
    conn = sqlite3.connect(db_utils.DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE lojas SET sellers_finalized = 1 WHERE codigo_loja = ?", ('LFB',))
    conn.commit()
    conn.close()

    data = {'nome_cliente': 'Fallback', 'cpf': '222.222.222-22', 'codigo_produto': 'PX', 'quantidade': 1, 'codigo_loja': 'LFB', 'codigo_vendedor': 'V_B'}
    db_utils.inserir_linha(data)

    conn = sqlite3.connect(db_utils.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT codigo_vendedor FROM vendas WHERE cpf = ?", ('222.222.222-22',))
    row = cur.fetchone()
    assert row is not None
    # inserted vendedor should be the mapped one V_A
    assert row[0] == 'V_A'
