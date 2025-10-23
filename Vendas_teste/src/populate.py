# src/populate.py
import sqlite3
from faker import Faker
import random
import os
import pandas as pd
import unicodedata
import re

# Inicializa Faker
fake = Faker("pt_BR")

# Paths
DB_PATH = os.path.join("data", "db", "vendas.db")
SCHEMA_PATH = os.path.join("data", "db", "schema.sql")
CSV_PATH = os.path.join("data", "processed", "vendas_fake.csv")

# Cria diretórios caso não existam
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

# Remove banco antigo
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("🗑 Banco antigo removido.")

# Conecta ao banco
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Executa schema
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    cursor.executescript(f.read())
print("✅ Schema executado com sucesso.")

# Funções auxiliares
def remove_acentos(texto):
    texto = unicodedata.normalize('NFKD', texto)
    texto = texto.encode('ASCII', 'ignore').decode('utf-8')
    return texto

def somente_numeros(texto):
    return re.sub(r'\D', '', texto)

# Dados fixos
vendedores = [
    ("V001", "Carlos Silva"),
    ("V002", "Maria Oliveira"),
    ("V003", "João Souza"),
    ("V004", "Antonio Souza"),
    ("V005", "Barone Mendes"),
    ("V006", "Thiago Curse")
]
lojas = [
    ("L001", "Loja Centro", 0),
    ("L002", "Loja Shopping", 0),
    ("L003", "Loja Bairro", 0)
]
produtos = [
    ("P001", "Notebook", 3500.0),
    ("P002", "Smartphone", 2500.0),
    ("P003", "Fone Bluetooth", 300.0),
    ("P004", "Monitor", 1200.0),
    ("P005", "Teclado Mecanico", 450.0),
    ("P006", "Smart Watch", 250.0)
]

# Insere dados fixos
cursor.executemany("INSERT INTO vendedores VALUES (?, ?)", vendedores)
# lojas agora tem 3 colunas (codigo_loja, nome_loja, sellers_finalized)
cursor.executemany("INSERT INTO lojas VALUES (?, ?, ?)", lojas)
cursor.executemany("INSERT INTO produtos VALUES (?, ?, ?)", produtos)

# Mapeia vendedores para lojas (2 por loja) - isso é necessário para que inserts em vendas
# passem pelo trigger que exige vendedor atribuído à loja
mapeamentos = [
    ("L001", "V001"), ("L001", "V002"),
    ("L002", "V003"), ("L002", "V004"),
    ("L003", "V005"), ("L003", "V006"),
]
cursor.executemany("INSERT INTO loja_vendedor (codigo_loja, codigo_vendedor) VALUES (?, ?)", mapeamentos)

# Constrói um dicionário auxiliar para escolher vendedores válidos por loja
vendedores_por_loja = {}
for loja_code, vend_code in mapeamentos:
    vendedores_por_loja.setdefault(loja_code, []).append(vend_code)

# Popula vendas
vendas_lista = []

for id_cliente in range(1, 101):
    nome_cliente = remove_acentos(fake.name())
    data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime("%d/%m/%Y")
    rg = str(fake.random_int(1000000, 9999999))
    cpf = somente_numeros(fake.cpf())
    endereco = fake.street_address()
    numero = fake.building_number()
    complemento = ""
    bairro = fake.bairro()
    cidade = fake.city()
    estado = fake.estado_sigla()
    cep = fake.postcode()
    telefone = somente_numeros(fake.phone_number())

    codigo_produto, _, valor_produto = random.choice(produtos)
    quantidade = random.randint(1, 5)
    data_venda = fake.date_between(start_date="-2y", end_date="today").strftime("%d/%m/%Y")
    data_compra = data_venda
    forma_pagamento = remove_acentos(random.choice(["Dinheiro", "Cartao Credito", "Cartao Debito", "Pix"]))
    # lojas tem a forma (codigo_loja, nome_loja, sellers_finalized)
    codigo_loja = random.choice(lojas)[0]
    # Seleciona um vendedor que pertença à loja (garante trigger)
    candidatos = vendedores_por_loja.get(codigo_loja) or [random.choice(vendedores)[0]]
    codigo_vendedor = random.choice(candidatos)

    # Insere no banco
    cursor.execute("""
        INSERT INTO vendas (
            id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, bairro, cidade, estado, cep, telefone,
            codigo_produto, quantidade, data_venda, data_compra, forma_pagamento, codigo_loja, codigo_vendedor
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento,
        bairro, cidade, estado, cep, telefone, codigo_produto, quantidade, data_venda, data_compra,
        forma_pagamento, codigo_loja, codigo_vendedor
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
        "quantidade": quantidade,
        "data_venda": data_venda,
        "data_compra": data_compra,
        "forma_pagamento": forma_pagamento,
        "codigo_loja": codigo_loja,
        "codigo_vendedor": codigo_vendedor,
        "valor_produto": valor_produto
    })

# Commit e fechamento
conn.commit()
conn.close()
print("✅ Banco populado com 100 vendas fakes.")

# Salva CSV
df_csv = pd.DataFrame(vendas_lista)
df_csv.to_csv(CSV_PATH, index=False, sep=";")
print(f"✅ CSV gerado em: {CSV_PATH}")
