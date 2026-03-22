# Medallion / three-layer data architecture

This pipeline uses **three S3 prefixes** in one bucket (configurable in Terraform):

| Layer | Names | Default prefix | Contents |
|-------|--------|-----------------|----------|
| **Landing** | Raw, Bronze | `landing/` | Hit-level **Parquet** (default) with the same columns as the legacy Adobe TSV; immutable-by-convention. Glue accepts legacy TSV via `glue_landing_format = "tsv"`. |
| **Staging** | Silver, curated intermediate | `staging/search_hits/` | Optional Glue output: **typed Parquet**, Hive-partitioned by `dt` / `hour` / `minute` (15-minute buckets). Schema-validated shape for downstream jobs. |
| **Curated** | Gold, cleansed, final | `curated/search_keyword/` | **Aggregated** search-keyword revenue (Parquet), BI/Athena-ready. Lambda writes the same layer for the small-file path. Optional RDS mirrors this layer. |

## Flow

```
Source → [Landing TSV] → Glue / Lambda → [Curated aggregates]
                    └→ Glue (optional) → [Staging Parquet hits]
```

- **Glue** reads **Landing** (`--landing_format` `parquet` or `tsv`), optionally materializes **Staging** (`--partitioned_hits_path`), always writes **Curated** aggregates (`--output_path`) as Parquet.
- **Lambda** triggers on new objects under **Landing** (`input_prefix`), reads `.parquet` (or legacy TSV/`.tab`), writes **Curated** as Parquet (requires PyArrow in the deployment package).

## Legacy paths

Existing deployments may still use `input/` and `output/`. Set in `terraform.tfvars`:

```hcl
input_prefix  = "input/"
output_prefix = "output/"
staging_prefix = ""  # disable staging Parquet until ready
```

## Related

- `src/s3_data_layers.py` — named defaults and helpers.
- `docs/partition_interval.md` — configurable `minute=` buckets (single Terraform variable for Lambda + Glue).
- `docs/golden_path_compaction.md` — pruning, small files, compaction jobs.
