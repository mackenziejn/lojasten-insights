#!/usr/bin/env python3
"""Aggregate duplicates.csv daily and write a summary CSV.

Produces: data/reports/duplicates_summary_<YYYYMMDD>.csv
"""
import csv
import datetime
import os
from collections import Counter, defaultdict

REPORTS = os.path.join('data', 'reports')
os.makedirs(REPORTS, exist_ok=True)

INPUT = os.path.join(REPORTS, 'duplicates.csv')

def read_duplicates(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def aggregate(rows):
    # group by date (yyyy-mm-dd)
    per_date = defaultdict(list)
    for r in rows:
        ts = r.get('timestamp')
        date = ts.split('T')[0] if ts else 'unknown'
        per_date[date].append(r)

    summaries = []
    for date, items in per_date.items():
        total = len(items)
        by_cpf = Counter(i['cpf'] for i in items if i.get('cpf'))
        top_cpfs = ';'.join(f"{cpf}:{cnt}" for cpf, cnt in by_cpf.most_common(5))
        lojas = Counter(i['codigo_loja'] for i in items if i.get('codigo_loja'))
        top_lojas = ';'.join(f"{l}:{c}" for l, c in lojas.most_common(5))
        summaries.append({'date': date, 'total_duplicates': total, 'top_cpfs': top_cpfs, 'top_lojas': top_lojas})
    return summaries

def write_summary(summaries):
    if not summaries:
        print('No duplicates to summarize')
        return
    hoje = datetime.date.today().strftime('%Y%m%d')
    out = os.path.join(REPORTS, f'duplicates_summary_{hoje}.csv')
    with open(out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'total_duplicates', 'top_cpfs', 'top_lojas'])
        writer.writeheader()
        for s in summaries:
            writer.writerow(s)
    print('Wrote summary to', out)

def main():
    rows = read_duplicates(INPUT)
    summaries = aggregate(rows)
    write_summary(summaries)

if __name__ == '__main__':
    main()
