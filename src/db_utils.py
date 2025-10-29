# src/db_utils.py
import sqlite3
try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
import os
import pandas as pd
import logging
import json
from datetime import datetime
import csv
import streamlit as st

# Configurar logger
logger = logging.getLogger('app')

# Paths
DUPLICATE_LOG = os.path.join("data", "reports", "duplicates.log")
DUPLICATE_CSV = os.path.join("data", "reports", "duplicates.csv")
DB_PATH = os.path.join("data", "db", "vendas.db")

def get_db_connection():
    """
    Retorna conexão com o banco - prioriza PostgreSQL (Supabase), fallback para SQLite
    """
    # Tentar PostgreSQL (Supabase) primeiro
    if HAS_PSYCOPG2:
        try:
            # Verificar se temos as credenciais do banco na estrutura correta
            if "database" in st.secrets and all(key in st.secrets["database"] for key in ["host", "database", "user", "password", "port"]):
                db_config = st.secrets["database"]
                conn = psycopg2.connect(
                    host=db_config["host"],
                    database=db_config["database"],
                    user=db_config["user"],
                    password=db_config["password"],
                    port=db_config["port"]
                )
                logger.info("✅ Conectado ao PostgreSQL (Supabase)")
                return conn, 'postgresql'
        except Exception as e:
            logger.warning(f"⚠️ PostgreSQL não disponível: {e}")

    # Fallback para SQLite
    try:
        # Garantir que o diretório existe
        db_dir = os.path.dirname(DB_PATH)
        os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(DB_PATH)
        logger.info("✅ Conectado ao SQLite local")
        return conn, 'sqlite'
    except Exception as e:
        logger.error(f"❌ Erro ao conectar com SQLite: {e}")
        raise

def execute_query(conn, db_type, query, params=None):
    """Executa query de forma compatível com ambos os bancos"""
    try:
        if db_type == 'postgresql':
            if params:
                conn.cursor().execute(query, params)
            else:
                conn.cursor().execute(query)
        else:  # sqlite
            if params:
                conn.execute(query, params)
            else:
                conn.execute(query)
        return True
    except Exception as e:
        logger.error(f"❌ Erro na query: {e}")
        return False

def criar_tabela(schema_path=None):
    """
    Ensure database and tables exist - compatível com PostgreSQL e SQLite
    """
    conn, db_type = get_db_connection()
    
    try:
        # Verificar se tabelas já existem
        if db_type == 'postgresql':
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tabelas_existentes = [row[0] for row in cursor.fetchall()]
        else:  # sqlite
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tabelas_existentes = [row[0] for row in cursor.fetchall()]
        
        # Se já existem tabelas, não recriar
        if tabelas_existentes:
            logger.info(f'✅ Banco já contém {len(tabelas_existentes)} tabelas')
            conn.close()
            return True
            
        # Criar tabelas (schema básico para ambos)
        if db_type == 'postgresql':
            # Schema PostgreSQL
            schema = """
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
                quantidade INTEGER NOT NULL CHECK(quantidade > 0),
                data_venda TEXT NOT NULL,
                data_compra TEXT NOT NULL,
                forma_pagamento TEXT,
                codigo_loja TEXT NOT NULL,
                nome_vendedor TEXT,
                codigo_vendedor TEXT NOT NULL,
                FOREIGN KEY(codigo_produto) REFERENCES produtos(codigo_produto),
                FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja),
                FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor)
            );

            CREATE TABLE loja_vendedor (
                id SERIAL PRIMARY KEY,
                codigo_loja TEXT NOT NULL,
                codigo_vendedor TEXT NOT NULL,
                UNIQUE(codigo_loja, codigo_vendedor),
                FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja),
                FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor)
            );

            CREATE TABLE usuarios (
                login TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                nome TEXT NOT NULL,
                loja TEXT NOT NULL,
                codigo_vendedor TEXT,
                permissions TEXT NOT NULL,
                ativo INTEGER DEFAULT 1
            );
            """
        else:
            # Schema SQLite (seu original)
            schema = """
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
                quantidade INTEGER NOT NULL CHECK(quantidade > 0),
                data_venda TEXT NOT NULL,
                data_compra TEXT NOT NULL,
                forma_pagamento TEXT,
                codigo_loja TEXT NOT NULL,
                nome_vendedor TEXT,
                codigo_vendedor TEXT NOT NULL,
                FOREIGN KEY(codigo_produto) REFERENCES produtos(codigo_produto),
                FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja),
                FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor)
            );
            
            CREATE TABLE IF NOT EXISTS loja_vendedor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            """
        
        # Executar schema
        if db_type == 'postgresql':
            conn.cursor().execute(schema)
        else:
            conn.executescript(schema)
        
        conn.commit()
        conn.close()
        logger.info('✅ Tabelas criadas com sucesso')
        return True
        
    except Exception as e:
        logger.error(f'❌ Erro ao criar tabelas: {e}')
        conn.close()
        return False

def inserir_linha(dados):
    """
    Insere uma linha no banco de vendas - COMPATÍVEL com pipeline.py
    Versão robusta com tratamento de erros para PostgreSQL e SQLite
    """
    conn, db_type = get_db_connection()
    
    try:
        # Parâmetros esperados pelo pipeline.py
        expected_params = [
            'id_cliente', 'nome_cliente', 'data_nascimento', 'rg', 'cpf',
            'endereco', 'numero', 'complemento', 'bairro', 'cidade', 'estado',
            'cep', 'telefone', 'codigo_produto', 'nome_produto', 'quantidade', 'valor_produto', 'data_venda',
            'data_compra', 'forma_pagamento', 'codigo_loja', 'codigo_vendedor', 'nome_vendedor'
        ]

        # Preparar parâmetros com valores padrão
        params = {}
        for key in expected_params:
            value = dados.get(key) if isinstance(dados, dict) else None
            # Valores padrão para campos obrigatórios
            if key == 'id_cliente' and (value is None or value == ''):
                value = 0
            elif key == 'quantidade' and (value is None or value == ''):
                value = 1
            elif key == 'valor_produto' and (value is None or value == ''):
                value = 0.0
            elif key == 'data_compra' and (value is None or value == '') and dados.get('data_venda'):
                value = dados.get('data_venda')
            elif key in ['nome_cliente', 'cpf', 'codigo_produto', 'nome_produto', 'codigo_loja', 'codigo_vendedor', 'nome_vendedor']:
                value = value or ''

            params[key] = value

        # Verificar duplicata de CPF (apenas para SQLite, PostgreSQL tem constraints)
        if db_type == 'sqlite':
            cpf_val = params.get('cpf')
            if cpf_val:
                try:
                    cursor = conn.cursor()
                    if db_type == 'postgresql':
                        cursor.execute("SELECT 1 FROM vendas WHERE cpf = %s LIMIT 1", (cpf_val,))
                    else:
                        cursor.execute("SELECT 1 FROM vendas WHERE cpf = ? LIMIT 1", (cpf_val,))
                    if cursor.fetchone():
                        # Log duplicate
                        log_duplicata(cpf_val, params.get('codigo_loja'), params.get('codigo_vendedor'))
                        logger.warning(f'CPF duplicado detectado: {cpf_val}')
                        conn.close()
                        return False
                except Exception as e:
                    logger.debug(f'Erro ao verificar CPF duplicado: {e}')

        # Inserir venda
        cursor = conn.cursor()
        if db_type == 'postgresql':
            cursor.execute("""
                INSERT INTO vendas (
                    id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, 
                    bairro, cidade, estado, cep, telefone, codigo_produto, quantidade, data_venda, 
                    data_compra, forma_pagamento, codigo_loja, nome_vendedor, codigo_vendedor
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                params['id_cliente'], params['nome_cliente'], params['data_nascimento'], params['rg'], 
                params['cpf'], params['endereco'], params['numero'], params['complemento'], 
                params['bairro'], params['cidade'], params['estado'], params['cep'], params['telefone'], 
                params['codigo_produto'], params['quantidade'], params['data_venda'], params['data_compra'], 
                params['forma_pagamento'], params['codigo_loja'], params.get('nome_vendedor', ''), 
                params['codigo_vendedor']
            ))
        else:
            cursor.execute("""
                INSERT INTO vendas (
                    id_cliente, nome_cliente, data_nascimento, rg, cpf, endereco, numero, complemento, 
                    bairro, cidade, estado, cep, telefone, codigo_produto, quantidade, data_venda, 
                    data_compra, forma_pagamento, codigo_loja, nome_vendedor, codigo_vendedor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                params['id_cliente'], params['nome_cliente'], params['data_nascimento'], params['rg'], 
                params['cpf'], params['endereco'], params['numero'], params['complemento'], 
                params['bairro'], params['cidade'], params['estado'], params['cep'], params['telefone'], 
                params['codigo_produto'], params['quantidade'], params['data_venda'], params['data_compra'], 
                params['forma_pagamento'], params['codigo_loja'], params.get('nome_vendedor', ''), 
                params['codigo_vendedor']
            ))
        
        conn.commit()
        conn.close()
        logger.info(f'✅ Venda inserida - CPF: {params.get("cpf")}, Loja: {params.get("codigo_loja")}')
        return True
        
    except Exception as e:
        logger.error(f'❌ Erro ao inserir venda: {e}')
        if conn:
            conn.close()
        return False

def log_duplicata(cpf, codigo_loja, codigo_vendedor):
    """Log de CPFs duplicados - função auxiliar para inserir_linha"""
    try:
        os.makedirs(os.path.dirname(DUPLICATE_LOG), exist_ok=True)
        
        # JSONL log
        entry = {
            'timestamp': datetime.now().isoformat(),
            'cpf': cpf,
            'codigo_loja': codigo_loja,
            'codigo_vendedor': codigo_vendedor
        }
        
        with open(DUPLICATE_LOG, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        
        # CSV log
        write_header = not os.path.exists(DUPLICATE_CSV)
        with open(DUPLICATE_CSV, 'a', newline='', encoding='utf-8') as csvf:
            writer = csv.writer(csvf)
            if write_header:
                writer.writerow(['timestamp', 'cpf', 'codigo_loja', 'codigo_vendedor'])
            writer.writerow([entry['timestamp'], entry['cpf'], entry['codigo_loja'], entry['codigo_vendedor']])
            
    except Exception as e:
        logger.error(f'Erro ao registrar duplicata: {e}')

def ensure_store_sellers_from_df(df):
    """
    Populate lojas, vendedores and loja_vendedor mappings based on a processed DataFrame.
    Versão segura que não causa erros se as relações já existem.
    """
    try:
        conn, db_type = get_db_connection()
        cursor = conn.cursor()

        # Insert unique vendedores from df
        if 'codigo_vendedor' in df.columns and 'nome_vendedor' in df.columns:
            vendedores = df[['codigo_vendedor', 'nome_vendedor']].drop_duplicates().values.tolist()
            for codigo_vendedor, nome_vendedor in vendedores:
                if codigo_vendedor and nome_vendedor:
                    try:
                        if db_type == 'postgresql':
                            cursor.execute(
                                "INSERT INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (%s,%s) ON CONFLICT (codigo_vendedor) DO NOTHING",
                                (codigo_vendedor, nome_vendedor)
                            )
                        else:
                            cursor.execute(
                                "INSERT OR IGNORE INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)",
                                (codigo_vendedor, nome_vendedor)
                            )
                    except Exception as e:
                        logger.debug(f"Vendedor já existe: {codigo_vendedor} - {e}")

        # Insert unique lojas from df
        if 'codigo_loja' in df.columns and 'nome_loja' in df.columns:
            lojas = df[['codigo_loja', 'nome_loja']].drop_duplicates().values.tolist()
            for codigo_loja, nome_loja in lojas:
                if codigo_loja and nome_loja:
                    try:
                        if db_type == 'postgresql':
                            cursor.execute(
                                "INSERT INTO lojas(codigo_loja, nome_loja) VALUES (%s,%s) ON CONFLICT (codigo_loja) DO NOTHING",
                                (codigo_loja, nome_loja)
                            )
                        else:
                            cursor.execute(
                                "INSERT OR IGNORE INTO lojas(codigo_loja, nome_loja) VALUES (?,?)",
                                (codigo_loja, nome_loja)
                            )
                    except Exception as e:
                        logger.debug(f"Loja já existe: {codigo_loja} - {e}")

        # Build store-seller relationships
        if 'codigo_loja' in df.columns and 'codigo_vendedor' in df.columns:
            loja_vendedor_pairs = df[['codigo_loja', 'codigo_vendedor']].drop_duplicates().values.tolist()
            for codigo_loja, codigo_vendedor in loja_vendedor_pairs:
                if codigo_loja and codigo_vendedor:
                    try:
                        if db_type == 'postgresql':
                            cursor.execute(
                                "INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES (%s,%s) ON CONFLICT (codigo_loja, codigo_vendedor) DO NOTHING",
                                (codigo_loja, codigo_vendedor)
                            )
                        else:
                            cursor.execute(
                                "INSERT OR IGNORE INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES (?,?)",
                                (codigo_loja, codigo_vendedor)
                            )
                    except Exception as e:
                        logger.debug(f"Relação já existe: {codigo_loja}-{codigo_vendedor} - {e}")

        conn.commit()
        conn.close()
        logger.info('✅ Lojas e vendedores sincronizados com sucesso')
        return True
        
    except Exception as e:
        logger.error(f'❌ Erro ao sincronizar lojas e vendedores: {e}')
        return False

def conectar():
    """Compatibilidade - retorna conexão e cursor"""
    conn, db_type = get_db_connection()
    if db_type == 'postgresql':
        return conn, conn.cursor()
    else:
        return conn, conn.cursor()

def fechar(conn):
    """Fecha a conexão"""
    if conn:
        conn.commit()
        conn.close()

def buscar_vendas(limit=None):
    """Retorna todas as vendas - compatível com ambos os bancos"""
    try:
        conn, db_type = get_db_connection()
        query = "SELECT v.* FROM vendas v ORDER BY v.data_venda DESC"
        if limit:
            query += f" LIMIT {limit}"
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        logger.info(f'📊 {len(df)} vendas carregadas do banco ({db_type})')
        return df
    except Exception as e:
        logger.error(f'❌ Erro ao buscar vendas: {e}')
        return pd.DataFrame()

def buscar_produtos():
    """Retorna todos os produtos"""
    try:
        conn, db_type = get_db_connection()
        df = pd.read_sql_query("SELECT * FROM produtos ORDER BY nome_produto", conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f'Erro ao buscar produtos: {e}')
        return pd.DataFrame()

def buscar_lojas():
    """Retorna todas as lojas"""
    try:
        conn, db_type = get_db_connection()
        df = pd.read_sql_query("SELECT * FROM lojas ORDER BY nome_loja", conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f'Erro ao buscar lojas: {e}')
        return pd.DataFrame()

def buscar_vendedores():
    """Retorna todos os vendedores"""
    try:
        conn, db_type = get_db_connection()
        df = pd.read_sql_query("SELECT * FROM vendedores ORDER BY nome_vendedor", conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f'Erro ao buscar vendedores: {e}')
        return pd.DataFrame()

def carregar_usuarios():
    """Carrega usuários do banco de dados"""
    try:
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        
        if db_type == 'postgresql':
            cursor.execute("SELECT login, password, role, nome, loja, codigo_vendedor, permissions, ativo FROM usuarios")
        else:
            cursor.execute("SELECT login, password, role, nome, loja, codigo_vendedor, permissions, ativo FROM usuarios")
        
        try:
            rows = cursor.fetchall()
        except Exception as e:
            if "no such column: ativo" in str(e):
                # Fallback query without ativo column
                if db_type == 'postgresql':
                    cursor.execute("SELECT login, password, role, nome, loja, codigo_vendedor, permissions FROM usuarios")
                else:
                    cursor.execute("SELECT login, password, role, nome, loja, codigo_vendedor, permissions FROM usuarios")
                rows = cursor.fetchall()
                # Add default ativo value
                rows = [row + (1,) for row in rows]
            else:
                raise
        
        conn.close()

        usuarios = {}
        for row in rows:
            login, password, role, nome, loja, codigo_vendedor, permissions_str, ativo = row
            try:
                permissions = json.loads(permissions_str)
            except:
                permissions = {
                    "ver_filtros": False,
                    "ver_indicadores": True,
                    "ver_graficos": True,
                    "executar_pipeline": False,
                    "analisar_todas_lojas": False,
                    "upload_csv": False
                }

            usuarios[login] = {
                "password": password,
                "role": role,
                "nome": nome,
                "loja": loja,
                "codigo_vendedor": codigo_vendedor,
                "permissions": permissions,
                "ativo": ativo if ativo is not None else True
            }

        logger.info(f'👥 {len(usuarios)} usuários carregados')
        return usuarios

    except Exception as e:
        logger.error(f'❌ Erro ao carregar usuários: {e}')
        return {}

def salvar_usuario(login, password, role, nome, loja, permissions, codigo_vendedor=None, ativo=True):
    """Salva ou atualiza um usuário no banco"""
    try:
        permissions_str = json.dumps(permissions)
        conn, db_type = get_db_connection()
        cursor = conn.cursor()

        # Se codigo_vendedor é fornecido, garantir que existe
        if codigo_vendedor:
            if db_type == 'postgresql':
                cursor.execute("INSERT INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (%s,%s) ON CONFLICT (codigo_vendedor) DO NOTHING",
                            (codigo_vendedor, nome))
            else:
                cursor.execute("INSERT OR IGNORE INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)",
                            (codigo_vendedor, nome))

        # Inserir ou atualizar usuário
        if db_type == 'postgresql':
            cursor.execute("""
                INSERT INTO usuarios (login, password, role, nome, loja, codigo_vendedor, permissions, ativo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (login) DO UPDATE SET
                password = EXCLUDED.password,
                role = EXCLUDED.role,
                nome = EXCLUDED.nome,
                loja = EXCLUDED.loja,
                codigo_vendedor = EXCLUDED.codigo_vendedor,
                permissions = EXCLUDED.permissions,
                ativo = EXCLUDED.ativo
            """, (login, password, role, nome, loja, codigo_vendedor, permissions_str, ativo))
        else:
            cursor.execute("""
                INSERT OR REPLACE INTO usuarios (login, password, role, nome, loja, codigo_vendedor, permissions, ativo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (login, password, role, nome, loja, codigo_vendedor, permissions_str, ativo))

        conn.commit()
        conn.close()
        logger.info(f'✅ Usuário {login} salvo/atualizado')
        return True

    except Exception as e:
        logger.error(f'❌ Erro ao salvar usuário {login}: {e}')
        return False

def deletar_usuario(login):
    """Deleta um usuário do banco"""
    try:
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        if db_type == 'postgresql':
            cursor.execute("DELETE FROM usuarios WHERE login = %s", (login,))
        else:
            cursor.execute("DELETE FROM usuarios WHERE login = ?", (login,))
        conn.commit()
        conn.close()
        logger.info(f'✅ Usuário {login} deletado')
        return True
    except Exception as e:
        logger.error(f'❌ Erro ao deletar usuário {login}: {e}')
        return False

def gerar_proximo_codigo_vendedor():
    """Gera o próximo código de vendedor sequencial (V001, V002, etc.)"""
    try:
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        
        if db_type == 'postgresql':
            cursor.execute("SELECT codigo_vendedor FROM vendedores WHERE codigo_vendedor LIKE 'V%'")
        else:
            cursor.execute("SELECT codigo_vendedor FROM vendedores WHERE codigo_vendedor LIKE 'V%'")
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return "V001"
        
        # Extrai os números
        nums = []
        for row in rows:
            if row[0] and row[0].startswith('V'):
                try:
                    num = int(row[0][1:])
                    nums.append(num)
                except ValueError:
                    pass
        
        if not nums:
            return "V001"
        
        next_num = max(nums) + 1
        return f"V{next_num:03d}"
        
    except Exception as e:
        logger.error(f'Erro ao gerar código vendedor: {e}')
        return "V001"

def verificar_estado_banco():
    """Verifica o estado atual do banco e retorna estatísticas"""
    try:
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        
        if db_type == 'postgresql':
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        
        tabelas = cursor.fetchall()
        
        estatisticas = {}
        for tabela in tabelas:
            table_name = tabela[0]
            if db_type == 'postgresql':
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            else:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            estatisticas[table_name] = count
        
        conn.close()
        
        logger.info("📊 Estatísticas do banco:")
        for tabela, count in estatisticas.items():
            logger.info(f"   - {tabela}: {count} registros")
        
        return estatisticas
        
    except Exception as e:
        logger.error(f'Erro ao verificar estado do banco: {e}')
        return {}

# Inicialização ao importar o módulo
logger.info('✅ Módulo db_utils carregado (PostgreSQL + SQLite)')