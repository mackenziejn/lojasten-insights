#!/usr/bin/env python3
"""
Migration script to ensure loja_vendedor relationship and triggers are present
and that the lojas table has a sellers_finalized column.

This script is idempotent: it will add the column if missing, drop and recreate
relevant triggers, execute the SQL in data/db/schema.sql to create the
junction table and triggers, and set sellers_finalized=1 for stores that
already have 2 sellers.

Usage:
    python src/migrations/ensure_loja_vendedor.py [--db /path/to/vendas.db] [--schema path/to/schema.sql]

Defaults assume the repository layout and will use:
    data/db/vendas.db
    data/db/schema.sql

"""
import argparse
import os
import sqlite3
import sys


DEFAULT_DB = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'db', 'vendas.db'))
DEFAULT_SCHEMA = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'db', 'schema.sql'))


def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def table_exists(conn, table_name):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cur.fetchone() is not None


def column_exists(conn, table_name, column_name):
    cur = conn.execute(f"PRAGMA table_info('{table_name}')")
    cols = [row[1] for row in cur.fetchall()]
    return column_name in cols


def drop_triggers(conn, trigger_names):
    for trg in trigger_names:
        try:
            conn.execute(f"DROP TRIGGER IF EXISTS {trg};")
            print(f"Dropped trigger if existed: {trg}")
        except sqlite3.DatabaseError as e:
            print(f"Warning: could not drop trigger {trg}: {e}")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default=DEFAULT_DB, help='Path to vendas.db')
    parser.add_argument('--schema', default=DEFAULT_SCHEMA, help='Path to schema.sql')
    args = parser.parse_args(argv)

    db_path = os.path.abspath(args.db)
    schema_path = os.path.abspath(args.schema)

    if not os.path.exists(schema_path):
        print(f"schema.sql not found at {schema_path}")
        sys.exit(1)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"Using DB: {db_path}")
    print(f"Using schema: {schema_path}")

    schema_sql = read_file(schema_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute('PRAGMA foreign_keys = ON;')
        conn.isolation_level = None  # autocommit for executescript sections

        # Ensure lojas table exists; if not, applying schema will create everything
        if not table_exists(conn, 'lojas'):
            print('lojas table not found; applying full schema.sql to create base tables/triggers')
            conn.executescript(schema_sql)
        else:
            # If lojas exists but column missing, add it
            if not column_exists(conn, 'lojas', 'sellers_finalized'):
                print('Adding sellers_finalized column to lojas')
                conn.execute("ALTER TABLE lojas ADD COLUMN sellers_finalized INTEGER NOT NULL DEFAULT 0;")
            else:
                print('lojas.sellers_finalized column already exists')

            # Drop triggers that we will recreate to ensure updated definitions
            triggers_to_manage = [
                'trg_loja_vendedor_before_insert',
                'trg_loja_vendedor_after_insert',
                'trg_loja_vendedor_before_delete',
                'trg_vendas_vendedor_must_belong_to_loja',
                'trg_vendas_vendedor_must_belong_to_loja_update',
            ]
            drop_triggers(conn, triggers_to_manage)

            # Execute schema SQL to create junction table and triggers (idempotent where possible)
            print('Executing schema.sql to (re)create tables/triggers')
            conn.executescript(schema_sql)

        # After schema applied, ensure sellers_finalized flag is set for stores with exactly 2 sellers
        if table_exists(conn, 'loja_vendedor') and table_exists(conn, 'lojas'):
            print('Updating lojas.sellers_finalized where a store has 2 sellers')
            conn.execute("BEGIN;")
            conn.execute("UPDATE lojas SET sellers_finalized = 1 WHERE codigo_loja IN (SELECT codigo_loja FROM loja_vendedor GROUP BY codigo_loja HAVING COUNT(*) = 2);")
            conn.execute("COMMIT;")

        print('Migration completed successfully')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
