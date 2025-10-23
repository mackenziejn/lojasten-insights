#!/bin/bash
set -e

echo "🚀 Iniciando setup do projeto LojasTen Insights..."

# Criar diretórios necessários
echo "📁 Criando diretórios..."
mkdir -p data/db
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/reports
mkdir -p data/logs
mkdir -p data/users

# Verificar se o banco já existe
if [ ! -f "data/db/vendas.db" ]; then
    echo "🗄️ Criando banco de dados..."
    python3 -c "
import sqlite3
import os

# Criar banco
conn = sqlite3.connect('data/db/vendas.db')
cursor = conn.cursor()

# Executar schema se existir
if os.path.exists('schema.sql'):
    with open('schema.sql', 'r', encoding='utf-8') as f:
        cursor.executescript(f.read())

conn.commit()
conn.close()
print('✅ Banco criado com sucesso!')
"
else
    echo "✅ Banco já existe"
fi

# Criar usuários padrão se não existirem
if [ ! -f "data/users.json" ]; then
    echo "👥 Criando usuários padrão..."
    cat > data/users.json << 'EOF'
{
    "admin": {
        "password": "senha123",
        "role": "admin",
        "nome": "Administrador",
        "loja": "Todas",
        "permissions": {
            "ver_filtros": true,
            "ver_indicadores": true,
            "ver_graficos": true,
            "executar_pipeline": true,
            "analisar_todas_lojas": true,
            "upload_csv": true
        },
        "ativo": true
    },
    "mackenzie": {
        "password": "vendas2025",
        "role": "gerente",
        "nome": "Mackenzie Gerente",
        "loja": "Todas",
        "permissions": {
            "ver_filtros": true,
            "ver_indicadores": true,
            "ver_graficos": true,
            "executar_pipeline": true,
            "analisar_todas_lojas": true,
            "upload_csv": false
        },
        "ativo": true
    }
}
EOF
    echo "✅ Usuários criados"
else
    echo "✅ Usuários já existem"
fi

echo "🎉 Setup concluído! O app está pronto para uso."
