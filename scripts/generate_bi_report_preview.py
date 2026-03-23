#!/usr/bin/env python3
"""
Regenerate docs/bi_report_preview_730.html from sample_hit_data.tsv (repo ground truth).

Usage (from repo root):
  python3 scripts/generate_bi_report_preview.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.bi_reporting import BIReporter  # noqa: E402
from src.search_keyword_analyzer import SearchKeywordAnalyzer  # noqa: E402


def main() -> None:
    sample = ROOT / "sample_hit_data.tsv"
    out = ROOT / "docs" / "bi_report_preview_730.html"

    analyzer = SearchKeywordAnalyzer(mask_pii=False)
    records = analyzer.process_file(sample)
    revenue_data = [
        {"engine_domain": r.engine_domain, "keyword": r.keyword, "revenue": float(r.revenue)}
        for r in records
    ]

    rep = BIReporter()
    report = rep.generate_report(
        revenue_data=revenue_data,
        report_date="2009-09-27",
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rep.export_to_html_summary(report), encoding="utf-8")
    print(f"Wrote {out.relative_to(ROOT)}")
    print(f"  Total revenue: ${report.total_revenue:.2f}")
    for k in report.top_keywords:
        print(f"  {k.search_engine} / {k.keyword}: ${k.revenue:.2f}")


if __name__ == "__main__":
    main()
