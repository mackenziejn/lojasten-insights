#!/usr/bin/env bash

set -euo pipefail

BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 Iniciando pipeline de vendas... (base: $BASEDIR)"

VENV_DIR="$BASEDIR/venv_vendas"
# Ativa ambiente virtual (se estiver usando venv_vendas)
if [ -d "$VENV_DIR" ]; then
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    echo "✅ Ambiente virtual ativado (venv_vendas)."
fi

# Run DB migrations (idempotent)
MIGRATION_SCRIPT="$BASEDIR/src/migrations/ensure_loja_vendedor.py"
DB_PATH="$BASEDIR/data/db/vendas.db"
SCHEMA_PATH="$BASEDIR/data/db/schema.sql"

if [ -f "$MIGRATION_SCRIPT" ]; then
    echo "🔧 Aplicando migração de banco de dados (loja_vendedor)..."
    # Prefer venv python if available
    if [ -x "$VENV_DIR/bin/python" ]; then
        PYTHON="$VENV_DIR/bin/python"
    else
        PYTHON="python3"
    fi
    "$PYTHON" "$MIGRATION_SCRIPT" --db "$DB_PATH" --schema "$SCHEMA_PATH" || true
    echo "✅ Migração aplicada (ou já estava atualizada)."
else
    echo "ℹ️ Migration script not found at $MIGRATION_SCRIPT; skipping DB migration step."
fi


# Ensure dependencies are installed in venv (only if requirements.txt exists)
REQ_FILE="$BASEDIR/requirements.txt"
if [ -f "$REQ_FILE" ]; then
    echo "📦 Checking required Python packages..."
    # Use selected PYTHON (set earlier) or fallback
    : ${PYTHON:="python3"}
    # Try importing a commonly used package to see if deps likely installed
    if ! "$PYTHON" -c "import pandas" >/dev/null 2>&1; then
        echo "⚠️ Some packages are missing in the environment; installing from requirements.txt"
        "$PYTHON" -m pip install --upgrade pip
        "$PYTHON" -m pip install -r "$REQ_FILE"
    else
        echo "✅ Required Python packages appear to be installed."
    fi
else
    echo "ℹ️ No requirements.txt found at $REQ_FILE; skipping package install check."
fi

# Executa o script principal using the resolved PYTHON
echo "🔁 Iniciando main.py"
: ${PYTHON:="python3"}
"$PYTHON" "$BASEDIR/main.py"

echo "✅ Pipeline finalizada."
