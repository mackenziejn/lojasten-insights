# criar_tabelas_supabase.py
import psycopg2

# Configurações do Supabase
SUPABASE_CONFIG = {
    "host": "db.azczqeoyncpgqtxgdazp.supabase.co",
    "database": "postgres", 
    "user": "postgres",
    "password": "Laurinha250520",  # ⚠️ SUA SENHA AQUI
    "port": "5432"
}

def criar_tabelas_supabase():
    try:
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG["host"],
            database=SUPABASE_CONFIG["database"],
            user=SUPABASE_CONFIG["user"],
            password=SUPABASE_CONFIG["password"],
            port=SUPABASE_CONFIG["port"]
        )
        cursor = conn.cursor()

        print("🗄️ Criando tabelas no Supabase...")

        # Cria todas as tabelas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                codigo_produto TEXT PRIMARY KEY,
                nome_produto TEXT NOT NULL,
                valor_produto REAL NOT NULL CHECK(valor_produto >= 0)
            );

            CREATE TABLE IF NOT EXISTS lojas (
                codigo_loja TEXT PRIMARY KEY,
                nome_loja TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS vendedores (
                codigo_vendedor TEXT PRIMARY KEY,
                nome_vendedor TEXT NOT NULL
            );

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
                data_registro TIMESTAMP,
                FOREIGN KEY(codigo_produto) REFERENCES produtos(codigo_produto),
                FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja),
                FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor)
            );

            CREATE TABLE IF NOT EXISTS loja_vendedor (
                id SERIAL PRIMARY KEY,
                codigo_loja TEXT NOT NULL,
                codigo_vendedor TEXT NOT NULL,
                UNIQUE(codigo_loja, codigo_vendedor),
                FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja),
                FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor)
            );

            CREATE TABLE IF NOT EXISTS usuarios (
                login TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                nome TEXT NOT NULL,
                loja TEXT NOT NULL,
                codigo_vendedor TEXT,
                permissions TEXT NOT NULL,
                ativo INTEGER DEFAULT 1
            );
        """)

        conn.commit()
        conn.close()
        print("✅ Tabelas criadas com sucesso no Supabase!")
        return True

    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")
        return False

if __name__ == "__main__":
    criar_tabelas_supabase()
