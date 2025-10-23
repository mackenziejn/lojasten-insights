import os
import csv
import tempfile
import shutil

from scripts.aggregate_duplicates import read_duplicates, aggregate, write_summary


def test_aggregate_writes_summary(tmp_path, monkeypatch):
    reports = tmp_path / 'data' / 'reports'
    reports.mkdir(parents=True)
    dup_file = reports / 'duplicates.csv'
    # create a small duplicates file
    rows = [
        {'timestamp': '2025-09-25T10:00:00', 'cpf': '111.111.111-11', 'codigo_loja': 'L1', 'codigo_vendedor': 'V1'},
        {'timestamp': '2025-09-25T10:05:00', 'cpf': '222.222.222-22', 'codigo_loja': 'L2', 'codigo_vendedor': 'V2'},
        {'timestamp': '2025-09-25T10:07:00', 'cpf': '111.111.111-11', 'codigo_loja': 'L1', 'codigo_vendedor': 'V1'},
    ]
    with open(dup_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['timestamp', 'cpf', 'codigo_loja', 'codigo_vendedor'])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    # monkeypatch the REPORTS dir inside the aggregate module
    import scripts.aggregate_duplicates as agg
    monkeypatch.setattr(agg, 'REPORTS', str(reports))

    read = read_duplicates(str(dup_file))
    assert len(read) == 3
    summary = aggregate(read)
    # should have at least one summary entry
    assert summary
    write_summary(summary)
    # check file written
    hoje = __import__('datetime').date.today().strftime('%Y%m%d')
    out = reports / f'duplicates_summary_{hoje}.csv'
    assert out.exists()
    with open(out, encoding='utf-8') as f:
        content = f.read()
        assert 'date' in content and 'total_duplicates' in content
