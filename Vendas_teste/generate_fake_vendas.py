import sqlite3
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker('pt_BR')

def generate_fake_vendas(num_records=100):
    conn = sqlite3.connect('data/db/vendas.db')
    cursor = conn.cursor()

    # Delete existing vendas data
    cursor.execute("DELETE FROM vendas;")
    conn.commit()

    # Get existing data
    cursor.execute("SELECT codigo_loja, nome_loja FROM lojas WHERE ativo = 1")
    lojas = cursor.fetchall()
    cursor.execute("SELECT codigo_vendedor, nome_vendedor FROM vendedores WHERE ativo = 1")
    vendedores = cursor.fetchall()
    cursor.execute("SELECT codigo_produto, nome_produto, valor_produto FROM produtos WHERE ativo = 1")
    produtos = cursor.fetchall()

    # Get valid loja_vendedor mappings
    cursor.execute("""
        SELECT lv.codigo_loja, lv.codigo_vendedor 
        FROM loja_vendedor lv 
        JOIN lojas l ON lv.codigo_loja = l.codigo_loja 
        JOIN vendedores v ON lv.codigo_vendedor = v.codigo_vendedor 
        WHERE l.ativo = 1 AND v.ativo = 1 AND lv.ativo = 1
    """)
    mappings = cursor.fetchall()

    records = []
    insert_columns = [
        'id_cliente', 'nome_cliente', 'data_nascimento', 'rg', 'cpf', 'endereco', 'numero', 'complemento',
        'bairro', 'cidade', 'estado', 'cep', 'telefone', 'codigo_produto', 'nome_produto',
        'quantidade', 'valor_produto', 'data_venda', 'data_compra', 'forma_pagamento',
        'codigo_loja', 'nome_loja', 'codigo_vendedor', 'nome_vendedor', 'status_venda', 'observacoes'
    ]

    for _ in range(num_records):
        # Select valid loja-vendedor pair
        if not mappings:
            continue
        loja_vend_pair = random.choice(mappings)
        codigo_loja, codigo_vendedor = loja_vend_pair
        loja_info = next((l for l in lojas if l[0] == codigo_loja), (codigo_loja, ''))
        vendedor_info = next((v for v in vendedores if v[0] == codigo_vendedor), (codigo_vendedor, ''))
        nome_loja, nome_vendedor = loja_info[1], vendedor_info[1]

        # Select product
        if not produtos:
            continue
        prod_info = random.choice(produtos)
        codigo_produto, nome_produto, valor_produto = prod_info
        quantidade = random.randint(1, 10)
        forma_pagamento = random.choice(['Cartão de Débito', 'Cartão de Crédito', 'Dinheiro', 'PIX', 'Boleto'])

        # Fake cliente data
        nome_cliente = fake.name()
        data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%d/%m/%Y')
        rg = fake.rg()
        cpf = fake.cpf()
        endereco = fake.street_address()
        numero = str(random.randint(1, 1000))
        complemento = fake.building_number()
        bairro = fake.neighborhood()
        cidade = fake.city()
        estado = random.choice(['SP', 'RJ', 'MG', 'RS', 'PR'])
        cep = fake.postcode().replace('-', '')
        telefone = fake.phone_number()

        # Dates
        data_compra = fake.date_between(start_date='-1y', end_date='today').strftime('%d/%m/%Y')
        data_venda = data_compra  # Same day for simplicity

        id_cliente = random.randint(1, 10000)  # Fake client ID

        # Prepare values in schema order
        values = (
            id_cliente,
            nome_cliente,
            data_nascimento,
            rg,
            cpf,
            endereco,
            numero,
            complemento,
            bairro,
            cidade,
            estado,
            cep,
            telefone,
            codigo_produto,
            nome_produto,
            quantidade,
            valor_produto,
            data_venda,
            data_compra,
            forma_pagamento,
            codigo_loja,
            nome_loja,
            codigo_vendedor,
            nome_vendedor,
            'CONCLUIDA',
            ''
        )

        record = {
            'id_cliente': id_cliente,
            'nome_cliente': nome_cliente,
            'data_nascimento': data_nascimento,
            'rg': rg,
            'cpf': cpf,
            'endereco': endereco,
            'numero': numero,
            'complemento': complemento,
            'bairro': bairro,
            'cidade': cidade,
            'estado': estado,
            'cep': cep,
            'telefone': telefone,
            'codigo_produto': codigo_produto,
            'nome_produto': nome_produto,
            'quantidade': quantidade,
            'valor_produto': valor_produto,
            'forma_pagamento': forma_pagamento,
            'codigo_loja': codigo_loja,
            'nome_loja': nome_loja,
            'codigo_vendedor': codigo_vendedor,
            'nome_vendedor': nome_vendedor,
            'data_venda': data_venda,
            'data_compra': data_compra,
            'status_venda': 'CONCLUIDA',
            'observacoes': ''
        }
        records.append(record)

        # Insert into DB
        placeholders = ', '.join('?' * len(insert_columns))
        columns_str = ', '.join(insert_columns)
        insert_sql = f"INSERT INTO vendas ({columns_str}) VALUES ({placeholders})"
        cursor.execute(insert_sql, values)

    conn.commit()

    # Create CSV
    df = pd.DataFrame(records)
    raw_path = 'data/raw/vendas.csv'
    archived_path = 'data/archived/vendas.csv'

    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/archived', exist_ok=True)

    df.to_csv(raw_path, index=False)
    df.to_csv(archived_path, index=False)

    conn.close()
    print(f"Generated and inserted {len(records)} fake vendas into DB. CSV saved to {raw_path} and {archived_path}.")

if __name__ == "__main__":
    generate_fake_vendas(100)
