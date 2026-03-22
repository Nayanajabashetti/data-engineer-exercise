# Why small Glue jobs still take ~60–90+ seconds

Glue runs **Apache Spark on managed DPUs**. Wall-clock time is dominated by **framework overhead**, not your Python/Spark transforms—especially for tiny inputs.

## What the logs usually show

| Phase | What’s happening |
|--------|-------------------|
| **Glue bootstrap** | JVM, Spark driver, Glue libraries, logging (e.g. Drools rules loading), **DPU allocation**. |
| **Executor wait** | Gap between “stage submitted” and the first task running while a **G.1X worker** registers and connects to the driver—often **tens of seconds** on small jobs. |
| **S3 / planning** | Spark builds a file **index** (`InMemoryFileIndex`–style listing) under the resolved prefix. Even with a **narrow path**, **recursive** listing walks subfolders; many objects in the bucket prefix increases listing time. |
| **Useful work** | Your parse → window → aggregate → write — often **seconds** for a small TSV. |
| **Teardown** | Commit, metrics, releasing executors. |

So **~90s total with ~8s of “real” compute** is normal for **small files on Glue**; you’re not doing anything wrong.

## What this repo already does

- **Narrow paths** — `--partition_dt` (and optional hour/minute) resolve to `.../dt=YYYY-MM-DD/...` so Spark does **not** scan the whole bucket.
- **Optional Spark tuning** — `docs/glue_spark_optimizations.md` (shuffle, AQE, repartition, etc.) helps **large** workloads; it does **not** remove Glue/executor startup.

## Optional: trim S3 listing (flat folders only)

If **all** TSVs for your run are **direct children** of the resolved prefix (e.g. `input/dt=2026-03-21/*.tsv` with **no** `hour=` / `minute=` subfolders), set:

- `--s3_recursive_list` = `false` (Terraform: `glue_s3_recursive_list = false`)

That sets Glue’s S3 `recurse` connection option to **false**, so listing does **not** recurse into subdirectories. **Do not** use this if you store files under nested `hour=` / `minute=` paths.

## Pushdown predicates & job bookmarks

- **`push_down_predicate`** on `create_dynamic_frame.from_options` is most useful when reading **registered Glue Data Catalog** tables with **partition columns** (e.g. `dt`, `hour`). It filters **catalog partitions** before reading files. **Raw S3 CSV** paths without a catalog table get most of their benefit from **path** narrowing (see above), not a SQL-style predicate string.
- **Job bookmarks** (`--job-bookmark-option`) help **incremental** reprocessing of tracked sources. This job often uses **overwrite** semantics per run; enabling bookmarks **without** understanding the semantics can skip reprocessing. Treat bookmarks as a **product** decision, not a default latency fix.

## If you need sub-second latency on small files

Use the **Lambda** path (or local CLI) for **small** landing files; reserve **Glue** for **scale** (large data, long runs).

**Rough comparison** (same sample TSV): Glue often **~60–90+ seconds** wall-clock (Spark startup + S3 listing + executors). Lambda is typically **~1–3 seconds** billed duration for a small file (plus **cold start** `Init Duration` on the first invoke after idle — often **hundreds of ms**). Measure locally with:

```bash
export AWS_REGION=...
export DATA_BUCKET=...
INPUT_PREFIX=input/   # must match S3 notification prefix
./scripts/lambda_timing_aws.sh
```

The script prints the CloudWatch **REPORT** line (`Duration`, `Billed Duration`, `Init Duration`).
