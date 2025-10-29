import psycopg2

try:
    conn = psycopg2.connect(
        host="db.azczqeoyncpgqtxgdazp.supabase.co",
        database="postgres",
        user="postgres",
        password="Laurinha250520",  # ⚠️ COLOCAR SUA SENHA REAL AQUI!
        port=5432
    )
    print("✅ Conexão bem-sucedida!")
    
    cursor = conn.cursor()
    cursor.execute("SELECT current_database(), current_user, version();")
    resultado = cursor.fetchone()
    print(f"Database: {resultado[0]}")
    print(f"User: {resultado[1]}")
    print(f"PostgreSQL: {resultado[2]}")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Erro: {e}")
