# Golden path: pruning, small files, compaction

## Partition pruning (Glue)

`recursiveFileLookup` over `s3://bucket/landing/` (entire landing prefix) lists **all** objects under the prefix before Spark runs — fine for dev, risky at scale.

**Production:** pass optional Glue arguments so the job reads only the Hive paths it needs:

- `--partition_dt` — e.g. `2026-03-20` → reads `.../landing/dt=2026-03-20/**` (not the whole bucket).
- `--partition_hour` — optional, with `partition_dt` → `.../hour=14/**`.
- `--partition_minute` — optional, with `partition_hour` → `.../minute=45/**` (must be `00`, `15`, `30`, or `45`).

Late data: rows are still routed by **`hit_time_gmt`** into `dt` / `hour` / `minute` columns; path pruning limits **which files are read**, not how rows are attributed.

For **Glue Catalog** tables with partition columns, you can additionally use `push_down_predicate` on `create_dynamic_frame.from_catalog` (not required for raw path-based CSV reads).

## Small files (Lambda / Glue)

- **Lambda:** one Parquet (or one `.tab`) per invocation via `pyarrow` / a single writer — avoids many tiny `part-*.` files for that run.
- **Glue (aggregated output):** `coalesce(1)` before writing the **keyword aggregate** Parquet so the job emits a **single** data file under `output/<date>_SearchKeywordPerformance/` when the aggregate is modest in size. For very large result sets, tune `coalesce` / `repartition` instead of `1`.

## Compaction (janitor)

A **separate** scheduled Glue job can **daily** read yesterday’s partitioned raw data (e.g. under `--partitioned_hits_path`), merge small Parquet parts per partition, and write back with fewer/larger files. Keep that job independent from the main attribution job so failures are isolated.
