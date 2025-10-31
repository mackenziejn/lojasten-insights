# src/populate.py
from faker import Faker
import random
import os
import pandas as pd
import unicodedata
import re
import sqlite3
import psycopg2
from datetime import datetime, timedelta
import logging
import json
import sys

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Inicializa Faker
fake = Faker("pt_BR")

# Paths
CSV_PATH = os.path.join("data", "processed", "vendas_fake.csv")

# Cria diretórios caso não existam
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
os.makedirs("data/db", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

# Configurações do Supabase
SUPABASE_CONFIG = {
    "host": "db.azczqeoyncpgqtxgdazp.supabase.co",
    "database": "postgres", 
    "user": "postgres",
    "password": os.getenv('SUPABASE_DB_PASSWORD', 'Laurinha250520'),  # ⚠️ ATUALIZE A SENHA!
    "port": "5432"
}

# Funções auxiliares
def remove_acentos(texto):
    """Remove acentos de textos"""
    if not texto or pd.isna(texto):
        return ""
    texto = unicodedata.normalize('NFKD', str(texto))
    texto = texto.encode('ASCII', 'ignore').decode('utf-8')
    return texto

def somente_numeros(texto):
    """Mantém apenas números"""
    if not texto or pd.isna(texto):
        return ""
    return re.sub(r'\D', '', str(texto))

def testar_conexao_supabase():
    """Testa a conexão com o Supabase"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        conn.close()
        logger.info(f"✅ Conexão com Supabase bem-sucedida: {version.split(',')[0]}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro na conexão com Supabase: {e}")
        return False

def criar_schema_supabase():
    """Cria o schema básico no Supabase"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        # Tabela produtos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                codigo_produto TEXT PRIMARY KEY,
                nome_produto TEXT NOT NULL,
                valor_produto REAL NOT NULL CHECK(valor_produto >= 0)
            )
        """)
        
        # Tabela lojas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lojas (
                codigo_loja TEXT PRIMARY KEY,
                nome_loja TEXT NOT NULL
            )
        """)
        
        # Tabela vendedores
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vendedores (
                codigo_vendedor TEXT PRIMARY KEY,
                nome_vendedor TEXT NOT NULL
            )
        """)
        
        # Tabela usuarios
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                login TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                nome TEXT NOT NULL,
                loja TEXT NOT NULL,
                codigo_vendedor TEXT,
                permissions TEXT NOT NULL,
                ativo BOOLEAN DEFAULT true
            )
        """)
        
        # Tabela vendas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vendas (
                id_venda SERIAL PRIMARY KEY,
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
            )
        """)
        
        # Tabela loja_vendedor
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS loja_vendedor (
                id SERIAL PRIMARY KEY,
                codigo_loja TEXT NOT NULL,
                codigo_vendedor TEXT NOT NULL,
                UNIQUE(codigo_loja, codigo_vendedor)
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("✅ Schema criado no Supabase")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar schema no Supabase: {e}")
        return False

def criar_schema_sqlite():
    """Cria o schema no SQLite"""
    try:
        db_path = "data/db/vendas.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Tabela produtos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                codigo_produto TEXT PRIMARY KEY,
                nome_produto TEXT NOT NULL,
                valor_produto REAL NOT NULL CHECK(valor_produto >= 0)
            )
        """)
        
        # Tabela lojas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lojas (
                codigo_loja TEXT PRIMARY KEY,
                nome_loja TEXT NOT NULL
            )
        """)
        
        # Tabela vendedores
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vendedores (
                codigo_vendedor TEXT PRIMARY KEY,
                nome_vendedor TEXT NOT NULL
            )
        """)
        
        # Tabela usuarios
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                login TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                nome TEXT NOT NULL,
                loja TEXT NOT NULL,
                codigo_vendedor TEXT,
                permissions TEXT NOT NULL,
                ativo BOOLEAN DEFAULT 1
            )
        """)
        
        # Tabela vendas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vendas (
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
            )
        """)
        
        # Tabela loja_vendedor
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS loja_vendedor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_loja TEXT NOT NULL,
                codigo_vendedor TEXT NOT NULL,
                UNIQUE(codigo_loja, codigo_vendedor)
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("✅ Schema criado no SQLite")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar schema no SQLite: {e}")
        return False

def popular_usuarios_supabase():
    """Popula usuários no Supabase"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        usuarios = [
            ("admin", "senha123", "admin", "Administrador do Sistema", "Todas lojas", None, 
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": true, "upload_csv": true, "configurar_usuarios": true, "acesso_total": true}'),
            
            ("csilva", "csilva1976", "manager", "Carlos Silva", "Loja Centro", "V001",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}'),
            
            ("maoliveira", "maoliveira1980", "user", "Maria Oliveira", "Loja Centro", "V002",
             '{"ver_filtros": false, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}'),
            
            ("josouza", "josouza1986", "user", "João Souza", "Loja Shopping", "V003",
             '{"ver_filtros": false, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}'),
            
            ("antonios", "antonios1977", "manager", "Antonio Santos", "Loja Shopping", "V004",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}'),
            
            ("baronem", "baronem1990", "user", "Barone Mendes", "Loja Bairro", "V005",
             '{"ver_filtros": false, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}'),
            
            ("thiagoc", "thiagoc123", "manager", "Thiago Costa", "Loja Bairro", "V006",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}'),
            
            ("mnogueira", "mnogueira123", "user", "Mackenzie Nogueira", "Loja Shopping", "V007",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}')
        ]
        
        for usuario in usuarios:
            cursor.execute("""
                INSERT INTO usuarios (login, password, role, nome, loja, codigo_vendedor, permissions)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (login) DO UPDATE SET
                    password = EXCLUDED.password,
                    role = EXCLUDED.role,
                    nome = EXCLUDED.nome,
                    loja = EXCLUDED.loja,
                    codigo_vendedor = EXCLUDED.codigo_vendedor,
                    permissions = EXCLUDED.permissions
            """, usuario)
        
        conn.commit()
        conn.close()
        logger.info(f"✅ {len(usuarios)} usuários populados no Supabase")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao popular usuários no Supabase: {e}")
        return False

def popular_usuarios_sqlite():
    """Popula usuários no SQLite"""
    try:
        conn = sqlite3.connect("data/db/vendas.db")
        cursor = conn.cursor()
        
        usuarios = [
            ("admin", "senha123", "admin", "Administrador do Sistema", "Todas lojas", None, 
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": true, "upload_csv": true, "configurar_usuarios": true, "acesso_total": true}', 1),
            
            ("csilva", "csilva1976", "manager", "Carlos Silva", "Loja Centro", "V001",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}', 1),
            
            ("maoliveira", "maoliveira1980", "user", "Maria Oliveira", "Loja Centro", "V002",
             '{"ver_filtros": false, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}', 1),
            
            ("josouza", "josouza1986", "user", "João Souza", "Loja Shopping", "V003",
             '{"ver_filtros": false, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}', 1),
            
            ("antonios", "antonios1977", "manager", "Antonio Santos", "Loja Shopping", "V004",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}', 1),
            
            ("baronem", "baronem1990", "user", "Barone Mendes", "Loja Bairro", "V005",
             '{"ver_filtros": false, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}', 1),
            
            ("thiagoc", "thiagoc123", "manager", "Thiago Costa", "Loja Bairro", "V006",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": true, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}', 1),
            
            ("mnogueira", "mnogueira123", "user", "Mackenzie Nogueira", "Loja Shopping", "V007",
             '{"ver_filtros": true, "ver_indicadores": true, "ver_graficos": true, "executar_pipeline": false, "analisar_todas_lojas": false, "upload_csv": false, "configurar_usuarios": false, "acesso_total": false}', 1)
        ]
        
        for usuario in usuarios:
            cursor.execute("""
                INSERT OR REPLACE INTO usuarios (login, password, role, nome, loja, codigo_vendedor, permissions, ativo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, usuario)
        
        conn.commit()
        conn.close()
        logger.info(f"✅ {len(usuarios)} usuários populados no SQLite")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao popular usuários no SQLite: {e}")
        return False

def popular_sqlite(quantidade_vendas=100):
    """Popula apenas o SQLite local"""
    try:
        logger.info("🗑 Preparando SQLite local...")
        
        # Conectar ao SQLite
        conn = sqlite3.connect("data/db/vendas.db")
        cursor = conn.cursor()

        # Dados fixos
        vendedores = [
            ("V001", "Carlos Silva"),
            ("V002", "Maria Oliveira"),
            ("V003", "João Souza"),
            ("V004", "Antonio Santos"),
            ("V005", "Barone Mendes"),
            ("V006", "Thiago Costa"),
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

        # Limpar tabelas existentes
        cursor.execute("DELETE FROM vendas")
        cursor.execute("DELETE FROM loja_vendedor")
        cursor.execute("DELETE FROM produtos")
        cursor.execute("DELETE FROM lojas")
        cursor.execute("DELETE FROM vendedores")

        # Inserir dados fixos
        cursor.executemany("INSERT INTO vendedores (codigo_vendedor, nome_vendedor) VALUES (?, ?)", vendedores)
        cursor.executemany("INSERT INTO lojas (codigo_loja, nome_loja) VALUES (?, ?)", lojas)
        cursor.executemany("INSERT INTO produtos (codigo_produto, nome_produto, valor_produto) VALUES (?, ?, ?)", produtos)

        # Dicionários para mapear códigos para nomes
        produtos_dict = {p[0]: p[1] for p in produtos}
        lojas_dict = {l[0]: l[1] for l in lojas}
        vendedores_dict = {v[0]: v[1] for v in vendedores}

        # Mapeia vendedores para lojas
        mapeamentos = [
            ("L001", "V001"), ("L001", "V002"),
            ("L002", "V003"), ("L002", "V004"), ("L002", "V007"),
            ("L003", "V005"), ("L003", "V006"),
        ]

        cursor.executemany("INSERT INTO loja_vendedor (codigo_loja, codigo_vendedor) VALUES (?, ?)", mapeamentos)

        # Constrói um dicionário auxiliar para escolher vendedores válidos por loja
        vendedores_por_loja = {}
        for loja_code, vend_code in mapeamentos:
            vendedores_por_loja.setdefault(loja_code, []).append(vend_code)

        # Popula vendas
        vendas_lista = []
        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for id_cliente in range(1, quantidade_vendas + 1):
            nome_cliente = remove_acentos(fake.name())
            data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime("%d/%m/%Y")
            rg = str(fake.random_int(1000000, 9999999))
            cpf = somente_numeros(fake.cpf())
            endereco = fake.street_address()
            numero = fake.building_number()
            complemento = ""
            bairro = remove_acentos(fake.bairro())
            cidade = "São Paulo"
            estado = "SP"
            cep = fake.postcode()
            telefone = somente_numeros(fake.phone_number())

            codigo_produto, nome_produto, valor_produto = random.choice(produtos)
            quantidade = random.randint(1, 5)
            
            # Data aleatória nos últimos 2 anos
            data_venda = fake.date_between(start_date="-2y", end_date="today").strftime("%d/%m/%Y")
            data_compra = data_venda
            
            forma_pagamento = remove_acentos(random.choice(["Boleto", "Dinheiro", "Cartão Credito", "Cartão Debito", "Pix"]))
            codigo_loja = random.choice(lojas)[0]
            candidatos = vendedores_por_loja.get(codigo_loja) or [random.choice(vendedores)[0]]
            codigo_vendedor = random.choice(candidatos)

            # Inserir no banco SQLite
            cursor.execute("""
                INSERT INTO vendas (
                    id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, bairro, cidade, estado, cep, telefone,
                    codigo_produto, nome_produto, quantidade, valor_produto, data_venda, data_compra, forma_pagamento, 
                    codigo_loja, nome_loja, codigo_vendedor, nome_vendedor, status_venda, observacoes, data_importacao, data_registro
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        conn.commit()
        conn.close()

        # Salva CSV
        df_csv = pd.DataFrame(vendas_lista)
        df_csv.to_csv(CSV_PATH, index=False, sep=";")
        
        logger.info(f"✅ SQLite local populado com {len(vendas_lista)} vendas")
        logger.info(f"✅ CSV gerado em: {CSV_PATH}")
            
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao popular SQLite: {e}")
        return False

def popular_supabase(quantidade_vendas=100):
    """Popula apenas o Supabase PostgreSQL"""
    try:
        if not testar_conexao_supabase():
            logger.error("❌ Não foi possível conectar ao Supabase")
            return False
            
        logger.info("🗑 Conectando e populando Supabase...")
        
        # Conecta ao Supabase
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()

        # Dados fixos (mesmos do SQLite)
        vendedores = [
            ("V001", "Carlos Silva"),
            ("V002", "Maria Oliveira"),
            ("V003", "João Souza"),
            ("V004", "Antonio Santos"),
            ("V005", "Barone Mendes"),
            ("V006", "Thiago Costa"),
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

        # Limpar tabelas no Supabase
        cursor.execute("DELETE FROM vendas")
        cursor.execute("DELETE FROM loja_vendedor")
        cursor.execute("DELETE FROM produtos")
        cursor.execute("DELETE FROM lojas")
        cursor.execute("DELETE FROM vendedores")

        # Insere dados fixos
        cursor.executemany("INSERT INTO vendedores (codigo_vendedor, nome_vendedor) VALUES (%s, %s)", vendedores)
        cursor.executemany("INSERT INTO lojas (codigo_loja, nome_loja) VALUES (%s, %s)", lojas)
        cursor.executemany("INSERT INTO produtos (codigo_produto, nome_produto, valor_produto) VALUES (%s, %s, %s)", produtos)

        # Mapeia vendedores para lojas
        mapeamentos = [
            ("L001", "V001"), ("L001", "V002"),
            ("L002", "V003"), ("L002", "V004"), ("L002", "V007"),
            ("L003", "V005"), ("L003", "V006"),
        ]

        cursor.executemany("INSERT INTO loja_vendedor (codigo_loja, codigo_vendedor) VALUES (%s, %s)", mapeamentos)

        # Dicionários para mapear códigos para nomes
        produtos_dict = {p[0]: p[1] for p in produtos}
        lojas_dict = {l[0]: l[1] for l in lojas}
        vendedores_dict = {v[0]: v[1] for v in vendedores}

        # Constrói um dicionário auxiliar para escolher vendedores válidos por loja
        vendedores_por_loja = {}
        for loja_code, vend_code in mapeamentos:
            vendedores_por_loja.setdefault(loja_code, []).append(vend_code)

        # Popula vendas
        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        vendas_count = 0

        for id_cliente in range(1, quantidade_vendas + 1):
            nome_cliente = remove_acentos(fake.name())
            data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime("%d/%m/%Y")
            rg = str(fake.random_int(1000000, 9999999))
            cpf = somente_numeros(fake.cpf())
            endereco = fake.street_address()
            numero = fake.building_number()
            complemento = ""
            bairro = remove_acentos(fake.bairro())
            cidade = "São Paulo"
            estado = "SP"
            cep = fake.postcode()
            telefone = somente_numeros(fake.phone_number())

            codigo_produto, nome_produto, valor_produto = random.choice(produtos)
            quantidade = random.randint(1, 5)
            
            # Data aleatória nos últimos 2 anos
            data_venda = fake.date_between(start_date="-2y", end_date="today").strftime("%d/%m/%Y")
            data_compra = data_venda
            
            forma_pagamento = remove_acentos(random.choice(["Boleto", "Dinheiro", "Cartão Credito", "Cartão Debito", "Pix"]))
            codigo_loja = random.choice(lojas)[0]
            candidatos = vendedores_por_loja.get(codigo_loja) or [random.choice(vendedores)[0]]
            codigo_vendedor = random.choice(candidatos)

            try:
                cursor.execute("""
                    INSERT INTO vendas (
                        id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, bairro, cidade, estado, cep, telefone,
                        codigo_produto, nome_produto, quantidade, valor_produto, data_venda, data_compra, forma_pagamento, 
                        codigo_loja, nome_loja, codigo_vendedor, nome_vendedor, status_venda, observacoes, data_importacao, data_registro
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento,
                    bairro, cidade, estado, cep, telefone, codigo_produto, nome_produto, quantidade, valor_produto, 
                    data_venda, data_compra, forma_pagamento, codigo_loja, lojas_dict[codigo_loja], 
                    codigo_vendedor, vendedores_dict[codigo_vendedor], "CONCLUIDA", "", data_atual, data_atual
                ))
                vendas_count += 1
            except Exception as e:
                logger.warning(f"⚠️ Erro ao inserir venda {id_cliente}: {e}")
                continue

        # Commit e fechamento
        conn.commit()
        conn.close()

        logger.info(f"✅ Supabase PostgreSQL populado com {vendas_count} vendas")
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao popular Supabase: {e}")
        return False

def popular_ambos_bancos(quantidade_vendas=100):
    """Popula tanto SQLite local quanto Supabase PostgreSQL"""
    
    logger.info("🚀 Iniciando população de dados em ambos os bancos...")
    
    # Primeiro popula SQLite local
    if popular_sqlite(quantidade_vendas):
        logger.info("✅ SQLite populado com sucesso!")
        
        # Depois popula Supabase (se SQLite funcionou)
        if testar_conexao_supabase():
            popular_supabase(quantidade_vendas)
            popular_usuarios_supabase()
        else:
            logger.error("❌ Não foi possível conectar ao Supabase")
    else:
        logger.error("❌ Falha ao popular SQLite")

def main():
    """Função principal para execução via CLI"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Popula bancos de dados com dados de exemplo')
    parser.add_argument('--quantidade', type=int, default=100, help='Quantidade de vendas a gerar')
    parser.add_argument('--sqlite-only', action='store_true', help='Popular apenas SQLite')
    parser.add_argument('--supabase-only', action='store_true', help='Popular apenas Supabase')
    parser.add_argument('--usuarios-only', action='store_true', help='Popular apenas usuários')
    
    args = parser.parse_args()
    
    if args.usuarios_only:
        logger.info("👥 Populando apenas usuários...")
        popular_usuarios_sqlite()
        if testar_conexao_supabase():
            popular_usuarios_supabase()
        return
    
    if args.sqlite_only:
        logger.info("🗃️ Populando apenas SQLite...")
        popular_sqlite(args.quantidade)
        popular_usuarios_sqlite()
    elif args.supabase_only:
        logger.info("☁️ Populando apenas Supabase...")
        if testar_conexao_supabase():
            popular_supabase(args.quantidade)
            popular_usuarios_supabase()
        else:
            logger.error("❌ Supabase não disponível")
    else:
        logger.info("🔄 Populando ambos os bancos...")
        popular_ambos_bancos(args.quantidade)

if __name__ == "__main__":
    # Criar schemas primeiro
    criar_schema_sqlite()
    if testar_conexao_supabase():
        criar_schema_supabase()
    
    # Popular bancos
    main()