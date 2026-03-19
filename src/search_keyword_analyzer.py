"""
SearchKeywordAnalyzer -- parses Adobe Analytics hit-level data to attribute
revenue from external search engines to the keywords that drove the traffic.

Attribution model:
    For each visitor (identified by IP + User-Agent), the most recent external
    search-engine referrer is remembered.  When a purchase event (event_list
    contains "1") occurs, the revenue from the product_list is credited to
    that search-engine / keyword pair.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import IO
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

SEARCH_ENGINE_QUERY_PARAMS: dict[str, list[str]] = {
    "google": ["q"],
    "bing": ["q"],
    "yahoo": ["p"],
    "msn": ["q"],
    "ask": ["q", "ask"],
    "aol": ["q", "query"],
    "duckduckgo": ["q"],
    "baidu": ["wd", "word"],
    "yandex": ["text"],
}


@dataclass
class SearchAttribution:
    engine_domain: str
    keyword: str


@dataclass
class RevenueRecord:
    engine_domain: str
    keyword: str
    revenue: float


class SearchKeywordAnalyzer:
    """
    Parses a tab-separated hit-level data file and produces a revenue-by-keyword
    report for traffic originating from external search engines.
    """

    def __init__(self) -> None:
        self._visitor_search: dict[str, SearchAttribution] = {}
        self._revenue: dict[tuple[str, str], float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_file(self, path: str | Path) -> list[RevenueRecord]:
        """Read *path*, process all rows in time order, and return aggregated revenue records."""
        with open(path, encoding="utf-8") as fh:
            return self.process_stream(fh)

    def process_stream(self, stream: IO[str]) -> list[RevenueRecord]:
        """Read from *stream*, sort hits by hit_time_gmt, and return aggregated revenue records.

        Sorting ensures last-touch attribution is always time-ordered regardless of
        whether the input file is pre-sorted, keeping behaviour aligned with the
        Glue/Spark window-function implementation.
        Sorting tolerates malformed or missing hit_time_gmt values by treating
        them as 0, ensuring bad rows do not crash processing.
        """

        def _safe_hit_timestamp(row: dict[str, str]) -> int:
            raw = (row.get("hit_time_gmt") or "").strip()
            if not raw:
                return 0
            if raw.isdigit():
                try:
                    return int(raw)
                except ValueError:
                    return 0
            return 0

        reader = csv.DictReader(stream, delimiter="\t")
        rows = list(reader)
        rows.sort(key=_safe_hit_timestamp)
        for row in rows:
            self._process_row(row)
        return self._build_sorted_results()

    def write_output(self, records: list[RevenueRecord], output_dir: str | Path = ".") -> Path:
        """Write the tab-delimited output file and return its path."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{date.today().isoformat()}_SearchKeywordPerformance.tab"
        output_path = output_dir / filename

        with open(output_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter="\t")
            writer.writerow(["Search Engine Domain", "Search Keyword", "Revenue"])
            for rec in records:
                writer.writerow([rec.engine_domain, rec.keyword, f"{rec.revenue:.2f}"])

        return output_path

    # ------------------------------------------------------------------
    # Internal processing
    # ------------------------------------------------------------------

    def _process_row(self, row: dict[str, str]) -> None:
        ip = (row.get("ip") or "").strip()
        if not ip:
            return

        user_agent = (row.get("user_agent") or "").strip()
        visitor_id = f"{ip}|{user_agent}"

        referrer = (row.get("referrer") or "").strip()
        event_list = (row.get("event_list") or "").strip()
        product_list = (row.get("product_list") or "").strip()

        attribution = self._parse_search_referrer(referrer)
        if attribution:
            self._visitor_search[visitor_id] = attribution

        is_purchase = "1" in {e.strip() for e in event_list.split(",")} if event_list else False

        if is_purchase and product_list and visitor_id in self._visitor_search:
            revenue = self._parse_product_revenue(product_list)
            if revenue > 0:
                attr = self._visitor_search[visitor_id]
                key = (attr.engine_domain, attr.keyword)
                self._revenue[key] = self._revenue.get(key, 0.0) + revenue

    def _build_sorted_results(self) -> list[RevenueRecord]:
        results = [
            RevenueRecord(engine_domain=domain, keyword=kw, revenue=rev)
            for (domain, kw), rev in self._revenue.items()
        ]
        results.sort(key=lambda r: r.revenue, reverse=True)
        return results

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_search_referrer(referrer: str) -> SearchAttribution | None:
        """Return (engine_domain, keyword) if the referrer is a known search engine."""
        if not referrer:
            return None

        try:
            parsed = urlparse(referrer)
        except ValueError:
            return None

        hostname = (parsed.hostname or "").lower()
        if not hostname:
            return None

        engine_name = None
        for engine in SEARCH_ENGINE_QUERY_PARAMS:
            if engine in hostname:
                engine_name = engine
                break

        if engine_name is None:
            return None

        # Extract the top-level search engine domain (e.g. "search.yahoo.com" -> "yahoo.com")
        parts = hostname.split(".")
        engine_domain = ".".join(parts[-2:]) if len(parts) >= 2 else hostname

        query_params = parse_qs(parsed.query)
        for param in SEARCH_ENGINE_QUERY_PARAMS[engine_name]:
            values = query_params.get(param)
            if values:
                keyword = values[0].strip().lower()  # parse_qs already percent-decodes
                if keyword:
                    return SearchAttribution(engine_domain=engine_domain, keyword=keyword)

        return None

    @staticmethod
    def _parse_product_revenue(product_list: str) -> float:
        """
        Parse revenue from the product_list field.

        Format per product: Category;Product;Qty;Revenue;CustomEvent
        Multiple products are comma-separated.  Revenue is the 4th
        semicolon-delimited field (index 3).
        """
        total = 0.0
        products = product_list.split(",")
        for product in products:
            fields = product.split(";")
            if len(fields) >= 4:
                revenue_str = fields[3].strip()
                if revenue_str:
                    try:
                        total += float(revenue_str)
                    except ValueError:
                        logger.warning("Non-numeric revenue value: %r", revenue_str)
        return total
