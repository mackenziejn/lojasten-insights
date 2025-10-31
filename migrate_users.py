# migrate_users.py
import sqlite3
import psycopg2
import json
import os
import sys
from datetime import datetime

# Configurações
SQLITE_DB = "data/db/vendas.db"
SUPABASE_CONFIG = {
    "host": "db.azczqeoyncpgqtxgdazp.supabase.co",
    "database": "postgres", 
    "user": "postgres",
    "password": os.getenv('SUPABASE_DB_PASSWORD', 'Laurinha250520'),  # ⚠️ ATUALIZE A SENHA!
    "port": "5432"
}

# Configurar logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def testar_conexao_supabase():
    """Testa a conexão com o Supabase"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        conn.close()
        logger.info(f"✅ Conexão com Supabase bem-sucedida")
        return True, "Conexão estabelecida"
    except Exception as e:
        logger.error(f"❌ Erro na conexão com Supabase: {e}")
        return False, str(e)

def criar_tabela_usuarios_supabase():
    """Cria a tabela usuarios no Supabase se não existir"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        # Verificar se tabela já existe
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'usuarios'
        """)
        
        if cursor.fetchone():
            logger.info("✅ Tabela 'usuarios' já existe no Supabase")
            conn.close()
            return True
        
        # Criar tabela usuarios no PostgreSQL
        cursor.execute("""
            CREATE TABLE usuarios (
                id SERIAL PRIMARY KEY,
                login VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                role VARCHAR(20) NOT NULL DEFAULT 'user',
                nome VARCHAR(100) NOT NULL,
                loja VARCHAR(100) NOT NULL,
                codigo_vendedor VARCHAR(50),
                permissions JSONB DEFAULT '{}',
                ativo BOOLEAN DEFAULT true,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                ultimo_login TIMESTAMP WITH TIME ZONE,
                data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("✅ Tabela 'usuarios' criada no Supabase")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela usuarios: {e}")
        return False

def carregar_usuarios_json():
    """Carrega usuários do arquivo JSON"""
    try:
        json_path = "users.json"
        if not os.path.exists(json_path):
            logger.error(f"❌ Arquivo {json_path} não encontrado")
            return {}
            
        with open(json_path, 'r', encoding='utf-8') as f:
            usuarios = json.load(f)
        
        logger.info(f"✅ {len(usuarios)} usuários carregados do JSON")
        return usuarios
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar JSON: {e}")
        return {}

def carregar_usuarios_sqlite():
    """Carrega usuários do SQLite local"""
    try:
        if not os.path.exists(SQLITE_DB):
            logger.error(f"❌ Banco SQLite não encontrado: {SQLITE_DB}")
            return []
            
        conn = sqlite3.connect(SQLITE_DB)
        cursor = conn.cursor()
        
        # Verificar se tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='usuarios'")
        if not cursor.fetchone():
            logger.error("❌ Tabela 'usuarios' não encontrada no SQLite")
            conn.close()
            return []
        
        # Buscar usuários
        cursor.execute("""
            SELECT login, password, role, nome, loja, codigo_vendedor, permissions, ativo 
            FROM usuarios
        """)
        usuarios = cursor.fetchall()
        conn.close()
        
        logger.info(f"✅ {len(usuarios)} usuários carregados do SQLite")
        return usuarios
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar usuários do SQLite: {e}")
        return []

def migrar_usuarios_sqlite_para_supabase():
    """Migra usuários do SQLite para o Supabase"""
    try:
        usuarios_sqlite = carregar_usuarios_sqlite()
        if not usuarios_sqlite:
            logger.warning("⚠️ Nenhum usuário encontrado no SQLite")
            return False
            
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        usuarios_migrados = 0
        erros = 0
        
        for usuario in usuarios_sqlite:
            try:
                if len(usuario) == 8:
                    login, password, role, nome, loja, codigo_vendedor, permissions_str, ativo = usuario
                else:
                    logger.warning(f"⚠️ Estrutura inválida do usuário: {usuario}")
                    continue
                
                # Tratar valores None
                codigo_vendedor = codigo_vendedor or ""
                ativo = bool(ativo) if ativo is not None else True
                role = role or "user"
                
                # Converter permissions
                try:
                    if permissions_str and isinstance(permissions_str, str):
                        permissions = json.loads(permissions_str)
                    else:
                        permissions = permissions_str or {}
                except:
                    permissions = {
                        "ver_filtros": True,
                        "ver_indicadores": True,
                        "ver_graficos": True,
                        "executar_pipeline": False,
                        "analisar_todas_lojas": False,
                        "upload_csv": False
                    }
                
                # Inserir no Supabase
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
                        ativo = EXCLUDED.ativo,
                        data_atualizacao = NOW()
                """, (login, password, role, nome, loja, codigo_vendedor, json.dumps(permissions), ativo))
                
                usuarios_migrados += 1
                logger.info(f"✅ Usuário migrado: {login} ({nome})")
                
            except Exception as user_error:
                erros += 1
                logger.error(f"❌ Erro ao migrar usuário {usuario[0]}: {user_error}")
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"🎉 Migração SQLite→Supabase concluída!")
        logger.info(f"📊 Usuários migrados: {usuarios_migrados}")
        logger.info(f"❌ Erros: {erros}")
        
        return usuarios_migrados > 0
        
    except Exception as e:
        logger.error(f"❌ Erro na migração SQLite→Supabase: {e}")
        return False

def migrar_usuarios_json_para_supabase():
    """Migra usuários diretamente do JSON para o Supabase"""
    try:
        usuarios_json = carregar_usuarios_json()
        if not usuarios_json:
            logger.error("❌ Nenhum usuário encontrado no JSON para migrar")
            return False
            
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        usuarios_migrados = 0
        erros = 0
        
        for login, dados in usuarios_json.items():
            try:
                password = dados.get('password', '')
                role = dados.get('role', 'user')
                nome = dados.get('nome', '')
                loja = dados.get('loja', '')
                codigo_vendedor = dados.get('codigo_vendedor', '')
                permissions = dados.get('permissions', {})
                ativo = dados.get('ativo', True)
                email = dados.get('email', '')
                
                # Inserir no Supabase
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
                        ativo = EXCLUDED.ativo,
                        data_atualizacao = NOW()
                """, (login, password, role, nome, loja, codigo_vendedor, json.dumps(permissions), ativo))
                
                usuarios_migrados += 1
                logger.info(f"✅ Usuário migrado do JSON: {login} ({nome})")
                
            except Exception as user_error:
                erros += 1
                logger.error(f"❌ Erro ao migrar usuário {login}: {user_error}")
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"🎉 Migração JSON→Supabase concluída!")
        logger.info(f"📊 Usuários migrados: {usuarios_migrados}")
        logger.info(f"❌ Erros: {erros}")
        
        return usuarios_migrados > 0
        
    except Exception as e:
        logger.error(f"❌ Erro na migração JSON→Supabase: {e}")
        return False

def migrar_usuarios_para_sqlite():
    """Migra usuários do JSON para o SQLite (fallback)"""
    try:
        usuarios_json = carregar_usuarios_json()
        if not usuarios_json:
            logger.error("❌ Nenhum usuário encontrado no JSON")
            return False
            
        # Garantir que o diretório existe
        os.makedirs(os.path.dirname(SQLITE_DB), exist_ok=True)
        
        conn = sqlite3.connect(SQLITE_DB)
        cursor = conn.cursor()
        
        # Criar tabela se não existir
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
        
        usuarios_migrados = 0
        erros = 0
        
        for login, dados in usuarios_json.items():
            try:
                password = dados.get('password', '')
                role = dados.get('role', 'user')
                nome = dados.get('nome', '')
                loja = dados.get('loja', '')
                codigo_vendedor = dados.get('codigo_vendedor', '')
                permissions = json.dumps(dados.get('permissions', {}))
                ativo = 1 if dados.get('ativo', True) else 0
                
                cursor.execute("""
                    INSERT OR REPLACE INTO usuarios 
                    (login, password, role, nome, loja, codigo_vendedor, permissions, ativo)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (login, password, role, nome, loja, codigo_vendedor, permissions, ativo))
                
                usuarios_migrados += 1
                logger.info(f"✅ Usuário migrado para SQLite: {login} ({nome})")
                
            except Exception as user_error:
                erros += 1
                logger.error(f"❌ Erro ao migrar usuário {login} para SQLite: {user_error}")
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"🎉 Migração para SQLite concluída!")
        logger.info(f"📊 Usuários migrados: {usuarios_migrados}")
        logger.info(f"❌ Erros: {erros}")
        
        return usuarios_migrados > 0
        
    except Exception as e:
        logger.error(f"❌ Erro na migração para SQLite: {e}")
        return False

def verificar_usuarios_supabase():
    """Verifica os usuários no Supabase"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        # Contar usuários
        cursor.execute("SELECT COUNT(*) as total FROM usuarios")
        total = cursor.fetchone()[0]
        
        # Listar usuários
        cursor.execute("""
            SELECT login, nome, role, loja, ativo, created_at 
            FROM usuarios 
            ORDER BY login
        """)
        usuarios = cursor.fetchall()
        
        logger.info(f"\n👥 USUÁRIOS NO SUPABASE (Total: {total}):")
        logger.info("-" * 80)
        
        for usuario in usuarios:
            login, nome, role, loja, ativo, created_at = usuario
            status = "✅ ATIVO" if ativo else "❌ INATIVO"
            logger.info(f"   👤 {login:<15} | {nome:<20} | {role:<10} | {loja:<15} | {status}")
        
        conn.close()
        return total
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar usuários no Supabase: {e}")
        return 0

def verificar_usuarios_sqlite():
    """Verifica os usuários no SQLite"""
    try:
        if not os.path.exists(SQLITE_DB):
            logger.error(f"❌ Banco SQLite não encontrado: {SQLITE_DB}")
            return 0
            
        conn = sqlite3.connect(SQLITE_DB)
        cursor = conn.cursor()
        
        # Verificar se tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='usuarios'")
        if not cursor.fetchone():
            logger.error("❌ Tabela 'usuarios' não encontrada no SQLite")
            conn.close()
            return 0
        
        # Contar usuários
        cursor.execute("SELECT COUNT(*) as total FROM usuarios")
        total = cursor.fetchone()[0]
        
        # Listar usuários
        cursor.execute("SELECT login, nome, role, loja, ativo FROM usuarios ORDER BY login")
        usuarios = cursor.fetchall()
        
        logger.info(f"\n👥 USUÁRIOS NO SQLITE (Total: {total}):")
        logger.info("-" * 80)
        
        for usuario in usuarios:
            login, nome, role, loja, ativo = usuario
            status = "✅ ATIVO" if ativo else "❌ INATIVO"
            logger.info(f"   👤 {login:<15} | {nome:<20} | {role:<10} | {loja:<15} | {status}")
        
        conn.close()
        return total
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar usuários no SQLite: {e}")
        return 0

def testar_autenticacao():
    """Testa a autenticação de usuários"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        logger.info("\n🔐 TESTE DE AUTENTICAÇÃO:")
        logger.info("-" * 50)
        
        # Testar com admin
        cursor.execute("SELECT login, password, nome, role, ativo FROM usuarios WHERE login = 'admin'")
        admin = cursor.fetchone()
        
        if admin:
            login, password, nome, role, ativo = admin
            logger.info(f"   ✅ ADMIN: {login} | {nome} | {role} | {'✅ ATIVO' if ativo else '❌ INATIVO'}")
            logger.info(f"   🔑 Senha: {password}")
        else:
            logger.error("   ❌ Usuário admin não encontrado")
        
        # Testar com csilva
        cursor.execute("SELECT login, password, nome, role, ativo FROM usuarios WHERE login = 'csilva'")
        csilva = cursor.fetchone()
        
        if csilva:
            login, password, nome, role, ativo = csilva
            logger.info(f"   ✅ CSILVA: {login} | {nome} | {role} | {'✅ ATIVO' if ativo else '❌ INATIVO'}")
        else:
            logger.error("   ❌ Usuário csilva não encontrado")
        
        conn.close()
        return admin is not None
        
    except Exception as e:
        logger.error(f"❌ Erro ao testar autenticação: {e}")
        return False

def limpar_usuarios_supabase():
    """Limpa todos os usuários do Supabase (CUIDADO!)"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM usuarios")
        conn.commit()
        conn.close()
        
        logger.info("✅ Todos os usuários removidos do Supabase")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao limpar usuários: {e}")
        return False

def mostrar_estatisticas():
    """Mostra estatísticas dos bancos"""
    logger.info("\n📊 ESTATÍSTICAS DOS BANCOS:")
    logger.info("=" * 50)
    
    # Supabase
    total_supabase = verificar_usuarios_supabase()
    
    # SQLite
    total_sqlite = verificar_usuarios_sqlite()
    
    logger.info(f"\n📈 RESUMO:")
    logger.info(f"   ✅ Supabase: {total_supabase} usuários")
    logger.info(f"   ✅ SQLite: {total_sqlite} usuários")
    
    return total_supabase, total_sqlite

def main():
    """Função principal"""
    if len(sys.argv) > 1:
        comando = sys.argv[1].lower()
        
        if comando == "verificar":
            mostrar_estatisticas()
            
        elif comando == "testar":
            testar_autenticacao()
            
        elif comando == "sqlite":
            logger.info("🔄 Migrando do SQLite para Supabase...")
            criar_tabela_usuarios_supabase()
            migrar_usuarios_sqlite_para_supabase()
            verificar_usuarios_supabase()
            
        elif comando == "json":
            logger.info("🔄 Migrando do JSON para Supabase...")
            criar_tabela_usuarios_supabase()
            migrar_usuarios_json_para_supabase()
            verificar_usuarios_supabase()
            
        elif comando == "sqlite-only":
            logger.info("🔄 Migrando para SQLite...")
            migrar_usuarios_para_sqlite()
            verificar_usuarios_sqlite()
            
        elif comando == "ambos":
            logger.info("🔄 Migrando para ambos os bancos...")
            migrar_usuarios_para_sqlite()
            criar_tabela_usuarios_supabase()
            migrar_usuarios_json_para_supabase()
            mostrar_estatisticas()
            
        elif comando == "limpar":
            logger.warning("⚠️  LIMPANDO TODOS OS USUÁRIOS DO SUPABASE!")
            confirmacao = input("❓ Tem certeza? (digite 'SIM' para confirmar): ")
            if confirmacao.upper() == "SIM":
                limpar_usuarios_supabase()
            else:
                logger.info("❌ Operação cancelada")
                
        elif comando == "ajuda" or comando == "help":
            mostrar_ajuda()
            
        else:
            logger.error("❌ Comando inválido")
            mostrar_ajuda()
    else:
        # Comando padrão: migração inteligente
        logger.info("🚀 INICIANDO MIGRAÇÃO INTELIGENTE...")
        
        # Testar conexão com Supabase
        sucesso, mensagem = testar_conexao_supabase()
        if not sucesso:
            logger.error("❌ Não foi possível conectar ao Supabase")
            logger.info("📦 Migrando apenas para SQLite...")
            migrar_usuarios_para_sqlite()
            verificar_usuarios_sqlite()
            return
        
        # Criar tabela no Supabase
        criar_tabela_usuarios_supabase()
        
        # Tentar migrar do SQLite primeiro
        if not migrar_usuarios_sqlite_para_supabase():
            logger.info("🔄 Fallback: migrando do JSON...")
            migrar_usuarios_json_para_supabase()
        
        # Também garantir SQLite local
        migrar_usuarios_para_sqlite()
        
        # Mostrar estatísticas finais
        mostrar_estatisticas()
        testar_autenticacao()

def mostrar_ajuda():
    """Mostra ajuda dos comandos"""
    logger.info("\n🎯 COMANDOS DISPONÍVEIS:")
    logger.info("=" * 50)
    logger.info("   python migrate_users.py           # Migração inteligente (padrão)")
    logger.info("   python migrate_users.py verificar # Verificar usuários em ambos")
    logger.info("   python migrate_users.py testar    # Testar autenticação")
    logger.info("   python migrate_users.py sqlite    # SQLite → Supabase")
    logger.info("   python migrate_users.py json      # JSON → Supabase") 
    logger.info("   python migrate_users.py sqlite-only # JSON → SQLite")
    logger.info("   python migrate_users.py ambos     # Para ambos bancos")
    logger.info("   python migrate_users.py limpar    # Limpar Supabase (CUIDADO!)")
    logger.info("   python migrate_users.py ajuda     # Mostrar esta ajuda")
    logger.info("\n💡 DICA: Configure a senha do Supabase na variável SUPABASE_DB_PASSWORD")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🔄 MIGRADOR DE USUÁRIOS - SQLite ↔ Supabase")
    logger.info("=" * 60)
    
    main()
    
    logger.info("\n🎉 Processo concluído!")