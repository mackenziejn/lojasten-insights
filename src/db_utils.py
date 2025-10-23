# src/db_utils.py
import sqlite3
import os
import pandas as pd
import logging
import json
from datetime import datetime
import csv

# Configurar logger
logger = logging.getLogger('app')

# Paths
DUPLICATE_LOG = os.path.join("data", "reports", "duplicates.log")
DUPLICATE_CSV = os.path.join("data", "reports", "duplicates.csv")
DB_PATH = os.path.join("data", "db", "vendas.db")

def criar_tabela(schema_path=None):
    """
    Ensure database and tables exist by executing schema.sql - apenas se necessário.
    Versão segura que não recria tabelas se já existem.
    """
    """Executa o schema.sql de forma segura"""
    if os.path.exists('schema.sql'):
        with sqlite3.connect(DB_PATH) as conn:
            with open('schema.sql', 'r') as f:
                schema_sql = f.read()
            conn.executescript(schema_sql)
            
    if schema_path is None:
        schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'schema.sql')
    
    # Ensure folder exists
    db_dir = os.path.dirname(DB_PATH)
    os.makedirs(db_dir, exist_ok=True)
    
    # Primeiro verifica se já existem tabelas
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tabelas_existentes = cursor.fetchall()
            
            # Se já existem tabelas, não executa o schema
            if tabelas_existentes:
                logger.info(f'✅ Banco já contém {len(tabelas_existentes)} tabelas, pulando criação')
                print(f"✅ Tabelas existentes: {[t[0] for t in tabelas_existentes]}")
                return True
    except Exception as e:
        logger.warning(f'Erro ao verificar tabelas existentes: {e}')
    
    # Só executa schema se o arquivo existe E se não há tabelas ou houve erro
    if os.path.exists(schema_path):
        try:
            with sqlite3.connect(DB_PATH) as conn:
                sql = open(schema_path, 'r', encoding='utf-8').read()
                conn.executescript(sql)
                logger.info('✅ Schema executado com sucesso')
                return True
        except Exception as e:
            logger.error(f'❌ Erro ao executar schema: {e}')
            # Tenta criar tabelas básicas como fallback
            return criar_tabelas_basicas()
    else:
        logger.warning(f'⚠️ Schema não encontrado em {schema_path}')
        return criar_tabelas_basicas()

def criar_tabelas_basicas():
    """Cria tabelas básicas se o schema completo falhar"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Tabela de produtos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                codigo_produto TEXT PRIMARY KEY,
                nome_produto TEXT NOT NULL,
                valor_produto REAL NOT NULL CHECK(valor_produto >= 0)
            )
        """)
        
        # Tabela de lojas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lojas (
                codigo_loja TEXT PRIMARY KEY,
                nome_loja TEXT NOT NULL
            )
        """)
        
        # Tabela de vendedores
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vendedores (
                codigo_vendedor TEXT PRIMARY KEY,
                nome_vendedor TEXT NOT NULL
            )
        """)
        
        # Tabela principal de vendas
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
            )
        """)
        
        # Tabela de relações loja-vendedor
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS loja_vendedor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_loja TEXT NOT NULL,
                codigo_vendedor TEXT NOT NULL,
                UNIQUE(codigo_loja, codigo_vendedor),
                FOREIGN KEY(codigo_loja) REFERENCES lojas(codigo_loja),
                FOREIGN KEY(codigo_vendedor) REFERENCES vendedores(codigo_vendedor)
            )
        """)
        
        # Tabela de usuários
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                login TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                nome TEXT NOT NULL,
                loja TEXT NOT NULL,
                codigo_vendedor TEXT,
                permissions TEXT NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info('✅ Tabelas básicas criadas com sucesso')
        return True
        
    except Exception as e:
        logger.error(f'❌ Erro ao criar tabelas básicas: {e}')
        return False

def ensure_store_sellers_from_df(df):
    """
    Populate lojas, vendedores and loja_vendedor mappings based on a processed DataFrame.
    Versão segura que não causa erros se as relações já existem.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Insert unique vendedores from df
        if 'codigo_vendedor' in df.columns and 'nome_vendedor' in df.columns:
            vendedores = df[['codigo_vendedor', 'nome_vendedor']].drop_duplicates().values.tolist()
            for codigo_vendedor, nome_vendedor in vendedores:
                if codigo_vendedor and nome_vendedor:
                    try:
                        cur.execute(
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
                        cur.execute(
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
                        cur.execute(
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
    """Retorna conexão e cursor para o banco de dados SQLite"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    return conn, cursor

def fechar(conn):
    """Fecha a conexão com o banco"""
    if conn:
        conn.commit()
        conn.close()

def inserir_linha(dados):
    """
    Insere uma linha no banco de vendas.
    Versão robusta com tratamento de erros.
    """
    conn, cursor = conectar()
    
    try:
        # Parâmetros esperados
        expected_params = [
            'id_cliente', 'nome_cliente', 'data_nascimento', 'rg', 'cpf', 
            'endereco', 'numero', 'complemento', 'bairro', 'cidade', 'estado', 
            'cep', 'telefone', 'codigo_produto', 'quantidade', 'data_venda', 
            'data_compra', 'forma_pagamento', 'codigo_loja', 'codigo_vendedor'
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
            elif key == 'data_compra' and (value is None or value == '') and dados.get('data_venda'):
                value = dados.get('data_venda')
            elif key in ['nome_cliente', 'cpf', 'codigo_produto', 'codigo_loja', 'codigo_vendedor']:
                value = value or ''
            
            params[key] = value

        # Verificar duplicata de CPF
        cpf_val = params.get('cpf')
        if cpf_val:
            try:
                cursor.execute("SELECT 1 FROM vendas WHERE cpf = ? LIMIT 1", (cpf_val,))
                if cursor.fetchone():
                    # Log duplicate
                    log_duplicata(cpf_val, params.get('codigo_loja'), params.get('codigo_vendedor'))
                    logger.warning(f'CPF duplicado detectado: {cpf_val}')
                    fechar(conn)
                    return False
            except Exception as e:
                logger.debug(f'Erro ao verificar CPF duplicado: {e}')

        # Inserir venda
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
        
        logger.info(f'✅ Venda inserida - CPF: {params.get("cpf")}, Loja: {params.get("codigo_loja")}')
        fechar(conn)
        return True
        
    except Exception as e:
        logger.error(f'❌ Erro ao inserir venda: {e}')
        fechar(conn)
        return False

def log_duplicata(cpf, codigo_loja, codigo_vendedor):
    """Log de CPFs duplicados"""
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

def buscar_vendas(limit=None, offset=0, chunk_size=1000):
    """
    Retorna vendas com os nomes dos produtos, lojas e vendedores
    Otimizado para memória com paginação e chunks
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Query base
        query_base = """
            SELECT v.*, p.nome_produto, l.nome_loja, vd.nome_vendedor, p.valor_produto
            FROM vendas v
            JOIN produtos p ON v.codigo_produto = p.codigo_produto
            JOIN lojas l ON v.codigo_loja = l.codigo_loja
            JOIN vendedores vd ON v.codigo_vendedor = vd.codigo_vendedor
        """

        # Contar total de registros
        count_query = f"SELECT COUNT(*) FROM ({query_base})"
        total = pd.read_sql_query(count_query, conn).iloc[0, 0]

        # Aplicar limite e offset se especificado
        if limit:
            query = f"{query_base} ORDER BY v.data_venda DESC LIMIT {limit} OFFSET {offset}"
        else:
            query = f"{query_base} ORDER BY v.data_venda DESC"

        # Otimizar dtypes para reduzir memória
        dtype_dict = {
            'id_cliente': 'int32',
            'quantidade': 'int32',
            'codigo_produto': 'category',
            'codigo_loja': 'category',
            'codigo_vendedor': 'category',
            'nome_produto': 'category',
            'nome_loja': 'category',
            'nome_vendedor': 'category',
            'forma_pagamento': 'category'
        }

        # Carregar em chunks se for muito grande
        if total > chunk_size:
            chunks = []
            for chunk_start in range(0, min(total, limit or total), chunk_size):
                chunk_query = f"{query_base} ORDER BY v.data_venda DESC LIMIT {chunk_size} OFFSET {chunk_start + offset}"
                chunk_df = pd.read_sql_query(chunk_query, conn, dtype=dtype_dict)
                chunks.append(chunk_df)
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_sql_query(query, conn, dtype=dtype_dict)

        conn.close()
        logger.info(f'📊 {len(df)} vendas carregadas do banco (total: {total})')
        return df

    except Exception as e:
        logger.error(f'❌ Erro ao buscar vendas: {e}')
        return pd.DataFrame()

def buscar_produtos():
    """Retorna todos os produtos"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM produtos ORDER BY nome_produto", conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f'Erro ao buscar produtos: {e}')
        return pd.DataFrame()

def buscar_lojas():
    """Retorna todas as lojas"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM lojas ORDER BY nome_loja", conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f'Erro ao buscar lojas: {e}')
        return pd.DataFrame()

def buscar_vendedores():
    """Retorna todos os vendedores"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM vendedores ORDER BY nome_vendedor", conn)
        conn.close()
        return df
    except Exception as e:
        logger.error(f'Erro ao buscar vendedores: {e}')
        return pd.DataFrame()

def carregar_usuarios():
    """Carrega usuários do banco de dados"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT login, password, role, nome, loja, codigo_vendedor, permissions, ativo FROM usuarios")
        rows = cur.fetchall()
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
                "ativo": ativo
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
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Se codigo_vendedor é fornecido, garantir que existe
        if codigo_vendedor:
            cur.execute("INSERT OR IGNORE INTO vendedores(codigo_vendedor, nome_vendedor) VALUES (?,?)",
                        (codigo_vendedor, nome))

        # Inserir ou atualizar usuário
        cur.execute("""
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
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM usuarios WHERE login = ?", (login,))
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
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # Busca o maior código existente
        cur.execute("SELECT codigo_vendedor FROM vendedores WHERE codigo_vendedor LIKE 'V%'")
        rows = cur.fetchall()
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
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # Contar tabelas
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tabelas = cur.fetchall()
        
        estatisticas = {}
        for tabela in tabelas:
            table_name = tabela[0]
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
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
logger.info('✅ Módulo db_utils carregado')