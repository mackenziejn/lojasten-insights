Daily aggregator cron example

Add the following line to the system crontab (edit with `crontab -e`) to run the aggregator every day at 00:05 and append logs to a file:

```
5 0 * * * cd /home/you/path/to/Vendas_teste && /usr/bin/env python3 scripts/aggregate_duplicates.py >> data/logs/aggregate_duplicates.log 2>&1
```

Replace `/home/you/path/to/Vendas_teste` with the absolute path to your project.

The script reads `data/reports/duplicates.csv` and writes `data/reports/duplicates_summary_<YYYYMMDD>.csv`.
