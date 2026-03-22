# Glue / Spark optimizations for large datasets

For **why small jobs still take ~60–90s** (executor wait, S3 listing, Glue bootstrap), see **`glue_runtime_latency.md`**.

The Glue job (`src/glue_job.py`) supports optional tuning for **large** landing files and **heavy** shuffles. Defaults stay small-job friendly (single curated Parquet file, no extra repartitioning).

## Parameters (Glue job arguments)

| Argument | Default | Purpose |
|----------|---------|---------|
| `--enable_large_job_optimizations` | `false` | When `true`, sets Spark **AQE** (adaptive query execution), skew join, and **shuffle partition** count (see `--shuffle_partitions`). |
| `--shuffle_partitions` | `200` | `spark.sql.shuffle.partitions` when large-optimizations are on. Raise for very large inputs (e.g. 400–800) if stages are under-partitioned; **profile** with Glue metrics. |
| `--curated_output_partitions` | `1` | **1** = `coalesce(1)` (one Parquet file under `.../<date>_SearchKeywordPerformance/`). **>1** = `repartition(n)` before write so the aggregate is split across multiple files (use when the **groupBy** result is huge). |
| `--visitor_repartition_partitions` | `0` | **0** = disabled. **>0** = `repartition(n, visitor_id)` **before** the attribution window. Helps parallelize the window shuffle on very large hit sets; start around **2–4×** total executor cores. |
| `--staging_repartition_partitions` | `0` | Only if `--partitioned_hits_path` is set. **>0** = repartition before writing silver Parquet. The job uses **`repartition(n, "dt", "hour", "minute")`** (same order as **`partitionBy`**) so shuffle keys align with Hive partition columns — more efficient than hash-only `repartition(n)`. |

Terraform wires these via `glue_*` variables in `terraform/variables.tf`. Airflow can override them with Variables of the same names (see `airflow/dags/search_keyword_pipeline_dag.py`).

## Behaviors already in the job

1. **Partition pruning** — `--partition_dt` / hour / minute narrow the S3 read path (avoid full-bucket scans).
2. **Persist `base` when staging is enabled** — If `--partitioned_hits_path` is set, `base` is **persisted** once so the landing CSV is not parsed twice (staging write + main pipeline).
3. **Single vs multi-file curated output** — Default `curated_output_partitions=1` minimizes small-file overhead for typical **small aggregate** row counts; increase only when needed.

## Suggested tuning order

1. **Scale Glue** — `glue_worker_count` / `worker_type` in Terraform (more executors + memory for big shuffles).
2. **Enable** `--enable_large_job_optimizations` and tune `--shuffle_partitions`.
3. **Set** `--visitor_repartition_partitions` if the window stage is slow or skewed (watch Spark UI / stage metrics).
4. **Raise** `--curated_output_partitions` only if the **final** Parquet write is huge (millions of distinct engine+keyword rows); otherwise keep **1**.

## Notes

- **DB sink** (`sync_db_sinks`): Still uses `collect()` on the **aggregated** `result` (small vs raw hits). If you ever have an enormous number of keyword rows, switch to JDBC batch write — separate from Spark tuning above.
- **Costs**: Higher partitions and more workers increase **DPU** charges; validate on a sample day before full backfills.
