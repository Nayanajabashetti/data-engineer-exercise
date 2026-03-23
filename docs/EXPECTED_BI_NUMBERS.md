# Expected BI numbers (matches `sample_hit_data.tsv`)

Ground-truth from the bundled sample file (Adobe-style hits → last-touch search → purchase revenue):

| Metric | Value |
|--------|------:|
| **Total revenue** | **$730.00** |
| **google.com / ipod** | **$480.00** ($190 + $290) |
| **bing.com / zune** | **$250.00** |

Local check:

```bash
python3 -m src.main sample_hit_data.tsv
cat output/*_SearchKeywordPerformance.tab
```

**Static HTML preview** (same charts/styling as Airflow BI task; needs internet for Chart.js CDN):

- Open `docs/bi_report_preview_730.html` in a browser.

Regenerate after changing `sample_hit_data.tsv`:

```bash
python3 scripts/generate_bi_report_preview.py
```

## Why S3 BI sometimes showed $2,480

The **Airflow `generate_bi_reports`** task reads **whatever Parquet Glue wrote** under  
`curated/search_keyword/<ds>_SearchKeywordPerformance/` for that run. If landing data was **not** the sample above (e.g. older demo totals **$1,730 + $750**), the HTML will **not** match this table. Align **landing input** and **`ds`** with your test, or use the local preview file for an exact screenshot.
