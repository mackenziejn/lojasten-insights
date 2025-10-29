# src/populate.py
from faker import Faker
import random
import os
import pandas as pd
import unicodedata
import re
import sqlite3
import psycopg2
from datetime import datetime

# Inicializa Faker
fake = Faker("pt_BR")

# Paths
CSV_PATH = os.path.join("data", "processed", "vendas_fake.csv")

# Cria diretórios caso não existam
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

# Configurações do Supabase (ATUALIZE A SENHA!)
SUPABASE_CONFIG = {
    "host": "db.azczqeoyncpgqtxgdazp.supabase.co",
    "database": "postgres", 
    "user": "postgres",
    "password": "Laurinha250520",  # ⚠️ ATUALIZE PARA SUA SENHA REAL!
    "port": "5432"
}

# Funções auxiliares
def remove_acentos(texto):
    texto = unicodedata.normalize('NFKD', texto)
    texto = texto.encode('ASCII', 'ignore').decode('utf-8')
    return texto

def somente_numeros(texto):
    return re.sub(r'\D', '', texto)

def popular_ambos_bancos():
    """Popula tanto SQLite local quanto Supabase PostgreSQL"""
    
    # Primeiro popula SQLite local
    if popular_sqlite():
        # Depois popula Supabase (se SQLite funcionou)
        popular_supabase()

def popular_sqlite():
    """Popula apenas o SQLite local"""
    try:
        # Cria banco SQLite temporário para evitar problemas com triggers
        temp_db_path = "data/db/vendas_temp.db"
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)
        
        temp_conn = sqlite3.connect(temp_db_path)
        cursor = temp_conn.cursor()

        print("🗑 Preparando SQLite local...")

        # Cria schema CORRETO com 29 colunas (igual ao seu banco real)
        cursor.executescript("""
            CREATE TABLE produtos (
                codigo_produto TEXT PRIMARY KEY,
                nome_produto TEXT NOT NULL,
                valor_produto REAL NOT NULL CHECK(valor_produto >= 0)
            );
            
            CREATE TABLE lojas (
                codigo_loja TEXT PRIMARY KEY,
                nome_loja TEXT NOT NULL
            );
            
            CREATE TABLE vendedores (
                codigo_vendedor TEXT PRIMARY KEY,
                nome_vendedor TEXT NOT NULL
            );
            
            CREATE TABLE vendas (
                id_venda INTEGER PRIMARY KEY AUTOINCREMENT,
                id_cliente INTEGER NOT NULL,
                nome_cliente TEXT NOT NULL,
                data_nascimento TEXT,
                rg TEXT,
                cpf TEXT NOT NULL,
                endereco TEXT,
                numero TEXT,
                complemento TEXT,
                bairro TEXT,
                cidade TEXT,
                estado TEXT,
                cep TEXT,
                telefone TEXT,
                codigo_produto TEXT NOT NULL,
                nome_produto TEXT,
                quantidade INTEGER NOT NULL CHECK(quantidade > 0),
                valor_produto REAL,
                data_venda TEXT NOT NULL,
                data_compra TEXT NOT NULL,
                forma_pagamento TEXT,
                codigo_loja TEXT NOT NULL,
                nome_loja TEXT,
                codigo_vendedor TEXT NOT NULL,
                nome_vendedor TEXT,
                status_venda TEXT,
                observacoes TEXT,
                data_importacao TIMESTAMP,
                data_registro TIMESTAMP
            );
            
            CREATE TABLE loja_vendedor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_loja TEXT NOT NULL,
                codigo_vendedor TEXT NOT NULL,
                UNIQUE(codigo_loja, codigo_vendedor)
            );
        """)

        # Dados fixos
        vendedores = [
            ("V001", "Carlos Silva"),
            ("V002", "Maria Oliveira"),
            ("V003", "João Souza"),
            ("V004", "Antonio Santos"),
            ("V005", "Barone Mendes"),
            ("V006", "Thiago Silva"),
            ("V007", "Mackenzie Nogueira")
        ]
        lojas = [
            ("L001", "Loja Centro"),
            ("L002", "Loja Shopping"),
            ("L003", "Loja Bairro")
        ]
        produtos = [
            ("P001", "Notebook", 3500.0),
            ("P002", "Smartphone", 2500.0),
            ("P003", "Tablet", 1800.0),
            ("P004", "Monitor", 950.0),
            ("P005", "Teclado", 150.0),
            ("P006", "Caixa de Som", 200.0),
            ("P007", "Mouse", 80.0),
            ("P008", "Impressora", 600.0)
        ]

        # Dicionários para mapear códigos para nomes
        produtos_dict = {p[0]: p[1] for p in produtos}
        lojas_dict = {l[0]: l[1] for l in lojas}
        vendedores_dict = {v[0]: v[1] for v in vendedores}

        # Insere dados fixos
        cursor.executemany("INSERT OR IGNORE INTO vendedores (codigo_vendedor, nome_vendedor) VALUES (?, ?)", vendedores)
        cursor.executemany("INSERT OR IGNORE INTO lojas (codigo_loja, nome_loja) VALUES (?, ?)", lojas)
        cursor.executemany("INSERT OR IGNORE INTO produtos (codigo_produto, nome_produto, valor_produto) VALUES (?, ?, ?)", produtos)

        # Mapeia vendedores para lojas
        mapeamentos = [
            ("L001", "V001"), ("L001", "V002"),
            ("L002", "V003"), ("L002", "V004"), ("L002", "V007"),
            ("L003", "V005"), ("L003", "V006"),
        ]

        cursor.executemany("INSERT OR IGNORE INTO loja_vendedor (codigo_loja, codigo_vendedor) VALUES (?, ?)", mapeamentos)

        # Constrói um dicionário auxiliar para escolher vendedores válidos por loja
        vendedores_por_loja = {}
        for loja_code, vend_code in mapeamentos:
            vendedores_por_loja.setdefault(loja_code, []).append(vend_code)

        # Popula vendas
        vendas_lista = []
        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for id_cliente in range(1, 121):
            nome_cliente = remove_acentos(fake.name())
            data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime("%d/%m/%Y")
            rg = str(fake.random_int(1000000, 9999999))
            cpf = somente_numeros(fake.cpf())
            endereco = fake.street_address()
            numero = fake.building_number()
            complemento = ""
            bairro = fake.bairro()
            cidade = "São Paulo"
            estado = "SP"
            cep = fake.postcode()
            telefone = somente_numeros(fake.phone_number())

            codigo_produto, nome_produto, valor_produto = random.choice(produtos)
            quantidade = random.randint(1, 5)
            data_venda = fake.date_between(start_date="-2y", end_date="today").strftime("%d/%m/%Y")
            data_compra = data_venda
            forma_pagamento = remove_acentos(random.choice(["Boleto", "Dinheiro", "Cartao Credito", "Cartao Debito", "Pix"]))
            codigo_loja = random.choice(lojas)[0]
            candidatos = vendedores_por_loja.get(codigo_loja) or [random.choice(vendedores)[0]]
            codigo_vendedor = random.choice(candidatos)

            # Insere no banco SQLite - CORRETO: 29 colunas
            cursor.execute("""
                INSERT INTO vendas (
                    id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, bairro, cidade, estado, cep, telefone,
                    codigo_produto, nome_produto, quantidade, valor_produto, data_venda, data_compra, forma_pagamento, 
                    codigo_loja, nome_loja, codigo_vendedor, nome_vendedor, status_venda, observacoes, data_importacao, data_registro
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento,
                bairro, cidade, estado, cep, telefone, codigo_produto, nome_produto, quantidade, valor_produto, 
                data_venda, data_compra, forma_pagamento, codigo_loja, lojas_dict[codigo_loja], 
                codigo_vendedor, vendedores_dict[codigo_vendedor], "CONCLUIDA", "", data_atual, data_atual
            ))

            vendas_lista.append({
                "id_cliente": id_cliente,
                "nome_cliente": nome_cliente,
                "data_nascimento": data_nascimento,
                "rg": rg,
                "cpf": cpf,
                "endereco": endereco,
                "numero": numero,
                "complemento": complemento,
                "bairro": bairro,
                "cidade": cidade,
                "estado": estado,
                "cep": cep,
                "telefone": telefone,
                "codigo_produto": codigo_produto,
                "nome_produto": nome_produto,
                "quantidade": quantidade,
                "valor_produto": valor_produto,
                "data_venda": data_venda,
                "data_compra": data_compra,
                "forma_pagamento": forma_pagamento,
                "codigo_loja": codigo_loja,
                "nome_loja": lojas_dict[codigo_loja],
                "codigo_vendedor": codigo_vendedor,
                "nome_vendedor": vendedores_dict[codigo_vendedor],
                "status_venda": "CONCLUIDA",
                "observacoes": "",
                "data_importacao": data_atual,
                "data_registro": data_atual
            })

        # Adiciona vendas específicas para V007 em L002
        for i in range(1, 6):
            id_cliente = 100 + i
            nome_cliente = remove_acentos(fake.name())
            data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime("%d/%m/%Y")
            rg = str(fake.random_int(1000000, 9999999))
            cpf = somente_numeros(fake.cpf())
            endereco = fake.street_address()
            numero = fake.building_number()
            complemento = ""
            bairro = fake.bairro()
            cidade = "São Paulo"
            estado = "SP"
            cep = fake.postcode()
            telefone = somente_numeros(fake.phone_number())

            codigo_produto, nome_produto, valor_produto = random.choice(produtos)
            quantidade = random.randint(1, 5)
            data_venda = fake.date_between(start_date="-2y", end_date="today").strftime("%d/%m/%Y")
            data_compra = data_venda
            forma_pagamento = remove_acentos(random.choice(["Boleto", "Dinheiro", "Cartao Credito", "Cartao Debito", "Pix"]))
            codigo_loja = "L002"
            codigo_vendedor = "V007"

            cursor.execute("""
                INSERT INTO vendas (
                    id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, bairro, cidade, estado, cep, telefone,
                    codigo_produto, nome_produto, quantidade, valor_produto, data_venda, data_compra, forma_pagamento, 
                    codigo_loja, nome_loja, codigo_vendedor, nome_vendedor, status_venda, observacoes, data_importacao, data_registro
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento,
                bairro, cidade, estado, cep, telefone, codigo_produto, nome_produto, quantidade, valor_produto, 
                data_venda, data_compra, forma_pagamento, codigo_loja, lojas_dict[codigo_loja], 
                codigo_vendedor, vendedores_dict[codigo_vendedor], "CONCLUIDA", "", data_atual, data_atual
            ))

            vendas_lista.append({
                "id_cliente": id_cliente,
                "nome_cliente": nome_cliente,
                "data_nascimento": data_nascimento,
                "rg": rg,
                "cpf": cpf,
                "endereco": endereco,
                "numero": numero,
                "complemento": complemento,
                "bairro": bairro,
                "cidade": cidade,
                "estado": estado,
                "cep": cep,
                "telefone": telefone,
                "codigo_produto": codigo_produto,
                "nome_produto": nome_produto,
                "quantidade": quantidade,
                "valor_produto": valor_produto,
                "data_venda": data_venda,
                "data_compra": data_compra,
                "forma_pagamento": forma_pagamento,
                "codigo_loja": codigo_loja,
                "nome_loja": lojas_dict[codigo_loja],
                "codigo_vendedor": codigo_vendedor,
                "nome_vendedor": vendedores_dict[codigo_vendedor],
                "status_venda": "CONCLUIDA",
                "observacoes": "",
                "data_importacao": data_atual,
                "data_registro": data_atual
            })

        # Commit e fechamento
        temp_conn.commit()
        temp_conn.close()

        # Salva CSV
        df_csv = pd.DataFrame(vendas_lista)
        df_csv.to_csv(CSV_PATH, index=False, sep=";")
        
        print(f"✅ SQLite local populado com {len(vendas_lista)} vendas fakes.")
        print(f"✅ CSV gerado em: {CSV_PATH}")
        
        # Remove o banco temporário
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)
            
        return True

    except Exception as e:
        print(f"❌ Erro ao popular SQLite: {e}")
        return False

def popular_supabase():
    """Popula apenas o Supabase PostgreSQL"""
    try:
        print("🗑 Tentando conectar ao Supabase...")
        
        # Conecta diretamente ao Supabase
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG["host"],
            database=SUPABASE_CONFIG["database"],
            user=SUPABASE_CONFIG["user"],
            password=SUPABASE_CONFIG["password"],
            port=SUPABASE_CONFIG["port"]
        )
        cursor = conn.cursor()

        print("✅ Conectado ao Supabase, populando dados...")

        # Dados fixos (mesmos do SQLite)
        vendedores = [
            ("V001", "Carlos Silva"),
            ("V002", "Maria Oliveira"),
            ("V003", "João Souza"),
            ("V004", "Antonio Santos"),
            ("V005", "Barone Mendes"),
            ("V006", "Thiago Silva"),
            ("V007", "Mackenzie Nogueira")
        ]
        lojas = [
            ("L001", "Loja Centro"),
            ("L002", "Loja Shopping"),
            ("L003", "Loja Bairro")
        ]
        produtos = [
            ("P001", "Notebook", 3500.0),
            ("P002", "Smartphone", 2500.0),
            ("P003", "Tablet", 1800.0),
            ("P004", "Monitor", 950.0),
            ("P005", "Teclado", 150.0),
            ("P006", "Caixa de Som", 200.0),
            ("P007", "Mouse", 80.0),
            ("P008", "Impressora", 600.0)
        ]

        # Limpa tabelas no Supabase
        cursor.execute("TRUNCATE TABLE vendas, loja_vendedor, usuarios, produtos, lojas, vendedores RESTART IDENTITY CASCADE")

        # Insere dados fixos
        cursor.executemany("INSERT INTO vendedores (codigo_vendedor, nome_vendedor) VALUES (%s, %s) ON CONFLICT (codigo_vendedor) DO NOTHING", vendedores)
        cursor.executemany("INSERT INTO lojas (codigo_loja, nome_loja) VALUES (%s, %s) ON CONFLICT (codigo_loja) DO NOTHING", lojas)
        cursor.executemany("INSERT INTO produtos (codigo_produto, nome_produto, valor_produto) VALUES (%s, %s, %s) ON CONFLICT (codigo_produto) DO NOTHING", produtos)

        # Mapeia vendedores para lojas
        mapeamentos = [
            ("L001", "V001"), ("L001", "V002"),
            ("L002", "V003"), ("L002", "V004"), ("L002", "V007"),
            ("L003", "V005"), ("L003", "V006"),
        ]

        cursor.executemany("INSERT INTO loja_vendedor (codigo_loja, codigo_vendedor) VALUES (%s, %s) ON CONFLICT (codigo_loja, codigo_vendedor) DO NOTHING", mapeamentos)

        # Popula vendas (mesmos dados do SQLite)
        if os.path.exists(CSV_PATH):
            df_csv = pd.read_csv(CSV_PATH, sep=";")
            for _, row in df_csv.iterrows():
                cursor.execute("""
                    INSERT INTO vendas (
                        id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, bairro, cidade, estado, cep, telefone,
                        codigo_produto, nome_produto, quantidade, valor_produto, data_venda, data_compra, forma_pagamento, 
                        codigo_loja, nome_loja, codigo_vendedor, nome_vendedor, status_venda, observacoes, data_importacao, data_registro
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(row['id_cliente']), str(row['nome_cliente']), str(row['data_nascimento']), str(row['rg']), str(row['cpf']),
                    str(row['endereco']), str(row['numero']), str(row['complemento']), str(row['bairro']), str(row['cidade']),
                    str(row['estado']), str(row['cep']), str(row['telefone']), str(row['codigo_produto']), str(row['nome_produto']),
                    int(row['quantidade']), float(row['valor_produto']), str(row['data_venda']), str(row['data_compra']),
                    str(row['forma_pagamento']), str(row['codigo_loja']), str(row['nome_loja']), str(row['codigo_vendedor']),
                    str(row['nome_vendedor']), str(row['status_venda']), str(row['observacoes']), str(row['data_importacao']), str(row['data_registro'])
                ))

        # Commit e fechamento
        conn.commit()
        conn.close()

        print(f"✅ Supabase PostgreSQL populado com {len(df_csv)} vendas.")
        return True

    except Exception as e:
        print(f"❌ Erro ao popular Supabase: {e}")
        return False

if __name__ == "__main__":
    popular_ambos_bancos()