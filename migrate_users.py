# migrar_usuarios.py
import sqlite3
import psycopg2
import json

# Configurações
SQLITE_DB = "data/db/vendas.db"
SUPABASE_CONFIG = {
    "host": "db.azczqeoyncpgqtxgdazp.supabase.co",
    "database": "postgres", 
    "user": "postgres",
    "password": "Laurinha250520",  # ⚠️ ATUALIZE A SENHA!
    "port": "5432"
}

def migrar_usuarios():
    try:
        # Conectar ao SQLite local
        sqlite_conn = sqlite3.connect(SQLITE_DB)
        sqlite_cursor = sqlite_conn.cursor()
        
        # Conectar ao Supabase
        supabase_conn = psycopg2.connect(**SUPABASE_CONFIG)
        supabase_cursor = supabase_conn.cursor()
        
        print("🔍 Buscando usuários no SQLite local...")
        
        # Buscar usuários do SQLite
        sqlite_cursor.execute("""
            SELECT login, password, role, nome, loja, codigo_vendedor, permissions, ativo 
            FROM usuarios
        """)
        usuarios = sqlite_cursor.fetchall()
        
        print(f"📋 Encontrados {len(usuarios)} usuários para migrar")
        
        # Migrar cada usuário para o Supabase
        for usuario in usuarios:
            login, password, role, nome, loja, codigo_vendedor, permissions_str, ativo = usuario
            
            # Converter permissions de string para JSON se necessário
            try:
                if isinstance(permissions_str, str):
                    permissions = json.loads(permissions_str)
                else:
                    permissions = permissions_str
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
            supabase_cursor.execute("""
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
            """, (login, password, role, nome, loja, codigo_vendedor, json.dumps(permissions), ativo))
            
            print(f"✅ Usuário migrado: {login} ({nome})")
        
        # Commit e fechar conexões
        supabase_conn.commit()
        sqlite_conn.close()
        supabase_conn.close()
        
        print(f"🎉 Migração concluída! {len(usuarios)} usuários migrados para o Supabase.")
        return True
        
    except Exception as e:
        print(f"❌ Erro na migração: {e}")
        return False

def verificar_usuarios_supabase():
    """Verifica os usuários no Supabase"""
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("SELECT login, nome, role, loja, ativo FROM usuarios")
        usuarios = cursor.fetchall()
        
        print("\n👥 Usuários no Supabase:")
        for usuario in usuarios:
            print(f"   - {usuario[0]}: {usuario[1]} ({usuario[2]}) - Loja: {usuario[3]} - Ativo: {usuario[4]}")
        
        conn.close()
        return len(usuarios)
        
    except Exception as e:
        print(f"❌ Erro ao verificar usuários: {e}")
        return 0

if __name__ == "__main__":
    # Migrar usuários
    if migrar_usuarios():
        # Verificar resultado
        total = verificar_usuarios_supabase()
        print(f"\n📊 Total de usuários no Supabase: {total}")
