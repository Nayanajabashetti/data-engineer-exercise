#!/usr/bin/env python3
"""
Convert Adobe-style hit-level TSV (tab-separated, header row) to Parquet for landing/.

Used by e2e scripts so Glue and Lambda exercise the Parquet path. Requires pyarrow.

Usage:
  python3 scripts/tsv_to_parquet_landing.py sample_hit_data.tsv out.parquet
"""

from __future__ import annotations

import csv
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: tsv_to_parquet_landing.py <input.tsv> <output.parquet>", file=sys.stderr)
        sys.exit(1)
    inp, out = sys.argv[1], sys.argv[2]
    rows: list[dict[str, str]] = []
    with open(inp, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        fieldnames = reader.fieldnames
        if not fieldnames:
            print("ERROR: TSV has no header row.", file=sys.stderr)
            sys.exit(1)
        for row in reader:
            rows.append({k: (row.get(k) or "").strip() if row.get(k) is not None else "" for k in fieldnames})
    if not rows:
        print("ERROR: no data rows in TSV.", file=sys.stderr)
        sys.exit(1)

    keys = list(rows[0].keys())
    cols: dict[str, pa.Array] = {}
    for k in keys:
        vals = [r[k] for r in rows]
        if k == "hit_time_gmt":
            cols[k] = pa.array(
                [int(v) if v.isdigit() else 0 for v in vals],
                type=pa.int64(),
            )
        else:
            cols[k] = pa.array(vals)
    table = pa.table(cols)
    pq.write_table(table, out)
    print(f"Wrote {table.num_rows} rows -> {out}")


if __name__ == "__main__":
    main()
