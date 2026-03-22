"""
Medallion / three-layer S3 layout for this pipeline.

Maps the common industry naming to concrete prefixes (single bucket, trailing slash):

+------------------+----------------------------+------------------------------------------+
| Layer            | Also called                | Purpose                                  |
+==================+============================+==========================================+
| **Landing**      | Raw, Bronze                | Hit-level **Parquet** (or legacy TSV via Glue `landing_format`) |
| **Staging**      | Curated (intermediate),    | Typed, partitioned Parquet (validated    |
|                  | Silver                     | schema, dt/hour/minute partitions)       |
| **Curated**      | Cleansed, Gold, Final      | Business aggregates: keyword revenue     |
|                  |                            | Parquet (and optional DB sinks)          |
+------------------+----------------------------+------------------------------------------+

Terraform / env defaults (override per deployment):
  landing/                  — Glue reads from here; Lambda trigger on PutObject
  staging/search_hits/      — optional Glue write of partitioned raw hits (Parquet)
  curated/search_keyword/   — Glue + Lambda write aggregated outputs

Legacy aliases: ``input/`` ≈ landing, ``output/`` ≈ curated (set prefixes in tfvars to match).

**Partition interval:** Hive ``minute=`` buckets are driven by ``PARTITION_INTERVAL_MINUTES`` / Terraform
``partition_interval_minutes`` — see ``src/partition_time.py`` (not hard-coded in multiple places).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

LayerName = Literal["landing", "staging", "curated"]


@dataclass(frozen=True)
class DataLayerPrefixes:
    """Configurable S3 key prefixes (relative to bucket root)."""

    landing: str
    staging: str
    curated: str

    def __post_init__(self) -> None:
        for name, val in (("landing", self.landing), ("staging", self.staging), ("curated", self.curated)):
            if not val:
                continue
            if not val.endswith("/"):
                raise ValueError(f"{name} prefix must end with '/': {val!r}")


def default_layer_prefixes() -> DataLayerPrefixes:
    """Defaults aligned with Terraform variables in this repo."""
    return DataLayerPrefixes(
        landing="landing/",
        staging="staging/search_hits/",
        curated="curated/search_keyword/",
    )


def s3_uri(bucket: str, prefix: str) -> str:
    """``s3://bucket/prefix/`` with normalized prefix."""
    p = prefix.strip("/")
    return f"s3://{bucket}/{p}/"
