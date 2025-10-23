import sqlite3
import argparse
import os
import logging

DB = os.path.join('data', 'db', 'vendas.db')
logger = logging.getLogger(__name__)


def unlock_loja(codigo_loja, force=False):
    """Unlock a loja (set sellers_finalized = 0). If force is False, require confirmation.

    Returns True on success, False otherwise.
    """
    if not force:
        resp = input(f"Are you sure you want to unlock loja '{codigo_loja}'? This allows changing assigned sellers. [y/N]: ")
        if resp.strip().lower() != 'y':
            logger.info('Aborting unlock for loja %s', codigo_loja)
            return False

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT codigo_loja, sellers_finalized FROM lojas WHERE codigo_loja = ?", (codigo_loja,))
    row = cur.fetchone()
    if not row:
        logger.error('Loja not found: %s', codigo_loja)
        conn.close()
        return False

    cur.execute("UPDATE lojas SET sellers_finalized = 0 WHERE codigo_loja = ?", (codigo_loja,))
    conn.commit()
    conn.close()
    logger.info('Loja %s unlocked (sellers_finalized = 0)', codigo_loja)
    return True


def list_mappings(codigo_loja=None):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    if codigo_loja:
        cur.execute("SELECT codigo_loja, codigo_vendedor FROM loja_vendedor WHERE codigo_loja = ?", (codigo_loja,))
    else:
        cur.execute("SELECT codigo_loja, codigo_vendedor FROM loja_vendedor ORDER BY codigo_loja")
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        print(r[0], r[1])


def lock_loja(codigo_loja, force=False):
    if not force:
        resp = input(f"Are you sure you want to lock loja '{codigo_loja}'? This will prevent seller changes. [y/N]: ")
        if resp.strip().lower() != 'y':
            logger.info('Aborting lock for loja %s', codigo_loja)
            return False
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT codigo_loja FROM lojas WHERE codigo_loja = ?", (codigo_loja,))
    if not cur.fetchone():
        logger.error('Loja not found: %s', codigo_loja)
        conn.close()
        return False
    cur.execute("UPDATE lojas SET sellers_finalized = 1 WHERE codigo_loja = ?", (codigo_loja,))
    conn.commit()
    conn.close()
    logger.info('Loja %s locked (sellers_finalized = 1)', codigo_loja)
    return True


def reassign_seller(codigo_loja, old_vendedor, new_vendedor, force=False):
    """Replace old_vendedor with new_vendedor for a loja. Performs validations.

    If the loja is locked, operation will be aborted unless force=True.
    """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    # check loja
    cur.execute("SELECT sellers_finalized FROM lojas WHERE codigo_loja = ?", (codigo_loja,))
    row = cur.fetchone()
    if not row:
        logger.error('Loja not found: %s', codigo_loja)
        conn.close()
        return False
    finalized = row[0]
    if finalized == 1 and not force:
        logger.error('Loja %s is finalized; unlock or use --force to reassign', codigo_loja)
        conn.close()
        return False

    # ensure new_vendedor exists
    cur.execute("SELECT 1 FROM vendedores WHERE codigo_vendedor = ?", (new_vendedor,))
    if not cur.fetchone():
        logger.error('New vendedor not found: %s', new_vendedor)
        conn.close()
        return False

    # perform reassignment: delete old mapping (if exists) and insert new mapping (if not present)
    cur.execute("DELETE FROM loja_vendedor WHERE codigo_loja = ? AND codigo_vendedor = ?", (codigo_loja, old_vendedor))
    cur.execute("INSERT OR IGNORE INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES (?,?)", (codigo_loja, new_vendedor))
    conn.commit()
    conn.close()
    logger.info('Reassigned loja %s: %s -> %s', codigo_loja, old_vendedor, new_vendedor)
    return True


def main():
    parser = argparse.ArgumentParser(description='Admin utilities for vendas DB')
    sub = parser.add_subparsers(dest='cmd')

    unlock = sub.add_parser('unlock', help='Unlock a loja to allow modifying its sellers')
    unlock.add_argument('codigo_loja')
    unlock.add_argument('--yes', action='store_true', help='Confirm without prompting')

    lock = sub.add_parser('lock', help='Lock a loja to prevent seller changes')
    lock.add_argument('codigo_loja')
    lock.add_argument('--yes', action='store_true')

    listp = sub.add_parser('list-mappings', help='List loja->vendedor mappings')
    listp.add_argument('--codigo_loja', nargs='?', default=None)

    reassign = sub.add_parser('reassign-seller', help='Reassign seller for a loja')
    reassign.add_argument('codigo_loja')
    reassign.add_argument('old_vendedor')
    reassign.add_argument('new_vendedor')
    reassign.add_argument('--force', action='store_true')
    export = sub.add_parser('export-mappings', help='Export loja->vendedor mappings to CSV')
    export.add_argument('--out', default='data/reports/mappings_export.csv')

    bulk = sub.add_parser('bulk-assign', help='Assign a vendedor to multiple lojas')
    bulk.add_argument('vendedor')
    bulk.add_argument('lojas', nargs='+')

    args = parser.parse_args()
    if args.cmd == 'unlock':
        success = unlock_loja(args.codigo_loja, force=args.yes)
        if not success:
            raise SystemExit(1)
    elif args.cmd == 'lock':
        success = lock_loja(args.codigo_loja, force=args.yes)
        if not success:
            raise SystemExit(1)
    elif args.cmd == 'list-mappings':
        list_mappings(args.codigo_loja)
    elif args.cmd == 'reassign-seller':
        success = reassign_seller(args.codigo_loja, args.old_vendedor, args.new_vendedor, force=args.force)
        if not success:
            raise SystemExit(1)
    elif args.cmd == 'export-mappings':
        out = args.out if hasattr(args, 'out') else 'data/reports/mappings_export.csv'
        conn = sqlite3.connect(DB)
        cur = conn.cursor()
        cur.execute("SELECT codigo_loja, codigo_vendedor FROM loja_vendedor ORDER BY codigo_loja")
        rows = cur.fetchall()
        conn.close()
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, 'w', encoding='utf-8') as f:
            f.write('codigo_loja,codigo_vendedor\n')
            for r in rows:
                f.write(f"{r[0]},{r[1]}\n")
        logger.info('Exported mappings to %s', out)
    elif args.cmd == 'bulk-assign':
        # bulk-assign <codigo_vendedor> <codigo_loja1> [codigo_loja2 ...]
        vendedor = args.vendedor
        lojas = args.lojas
        conn = sqlite3.connect(DB)
        cur = conn.cursor()
        # ensure vendedor exists
        cur.execute("SELECT 1 FROM vendedores WHERE codigo_vendedor = ?", (vendedor,))
        if not cur.fetchone():
            logger.error('Vendedor not found: %s', vendedor)
            conn.close()
            raise SystemExit(1)
        assigned = 0
        for loja in lojas:
            # ensure loja exists
            cur.execute("INSERT OR IGNORE INTO lojas(codigo_loja, nome_loja) VALUES (?,?)", (loja, loja))
            try:
                cur.execute("INSERT INTO loja_vendedor(codigo_loja, codigo_vendedor) VALUES (?,?)", (loja, vendedor))
                assigned += 1
            except sqlite3.DatabaseError:
                # trigger may block if loja finalized or max sellers reached
                logger.warning('Could not assign vendedor %s to loja %s (may be finalized or full)', vendedor, loja)
        conn.commit()
        conn.close()
        logger.info('Bulk assign completed. Assigned to %d lojas', assigned)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
