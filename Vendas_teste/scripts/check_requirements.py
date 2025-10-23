#!/usr/bin/env python3
"""Check that essential runtime packages can be imported.

Reads a short mapping of package names -> import names and verifies imports.
Exits with code 1 if any imports fail.
"""
import sys
import os
import csv
import json
import argparse

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
REQ = os.path.join(ROOT, 'requirements.txt')

# Common mapping from pip package name to importable module names
COMMON = {
    'plotly': ['plotly'],
    'streamlit': ['streamlit'],
    'reportlab': ['reportlab'],
    'pillow': ['PIL'],
    'pandas': ['pandas'],
    'faker': ['faker'],
}

def parse_requirements(path):
    pkgs = []
    if not os.path.exists(path):
        return pkgs
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # take the package name part before any comparison operator
            for sep in ['>=', '==', '<=', '~=', '<', '>']:
                if sep in line:
                    line = line.split(sep, 1)[0]
                    break
            pkgs.append(line.strip())
    return pkgs

def check_imports(pkgs):
    missing = []
    for pkg in pkgs:
        key = pkg.lower()
        import_names = COMMON.get(key, [key.replace('-', '_')])
        ok = False
        for name in import_names:
            try:
                __import__(name)
                ok = True
                break
            except Exception:
                continue
        if not ok:
            missing.append(pkg)
    return missing

def main():
    parser = argparse.ArgumentParser(description='Check importable packages')
    parser.add_argument('--json', action='store_true', help='Output machine-readable JSON')
    args = parser.parse_args()

    pkgs = parse_requirements(REQ)
    # Only check a subset of important/third-party packages to keep the check fast
    to_check = [p for p in pkgs if p.split('#')[0].strip().lower() in COMMON]
    missing = check_imports(to_check)
    if missing:
        if args.json:
            print(json.dumps({'missing': missing}))
        else:
            print('Missing importable packages:', ', '.join(missing))
        sys.exit(1)
    if args.json:
        print(json.dumps({'missing': []}))
    else:
        print('All required imports present for the checked packages.')

if __name__ == '__main__':
    main()
