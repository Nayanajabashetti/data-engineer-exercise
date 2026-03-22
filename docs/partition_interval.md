# Configurable partition interval (`minute=` buckets)

## Single source of truth

| Where | Setting |
|-------|---------|
| **Python** | `src/partition_time.py` — `DEFAULT_PARTITION_INTERVAL_MINUTES`, `PARTITION_INTERVAL_MINUTES` env, `get_minute_bucket()`, `partition_from_unix_seconds(..., interval_minutes=...)` |
| **Lambda** | Env `PARTITION_INTERVAL_MINUTES` (Terraform `partition_interval_minutes`) |
| **Glue** | Job argument `--partition_interval_minutes` (same Terraform variable) |

Changing the interval in **one** place (Terraform) updates Lambda and Glue together. The Glue script duplicates the bucket math with a comment to stay aligned with `partition_time.py` (Glue runs as a standalone script on S3).

## Medallion note

- **Landing**: keep raw files as-is (fine-grained or flat).
- **Staging**: Parquet hits use `partitionBy(dt, hour, minute)` where `minute` follows the configured interval.
- **Curated**: Aggregates are keyed by run date; path layout stays consistent if Staging and Lambda use the **same** interval.

## Athena / BI when the interval changes

If you move from 15 → 5 minutes (or vice versa):

- Old and new `minute=` folder labels **coexist** in S3 until you backfill or expire data.
- Athena tables may need **new partitions**, **partition projection** updates, or a **crawler** refresh.
- BI filters on `minute = '15'` must be updated to the new bucket set.

Plan interval changes as a **data migration**, not a config-only flip.
