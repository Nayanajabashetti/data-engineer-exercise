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
import io
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import IO
from urllib.parse import parse_qs, urlparse

try:
    from src.partition_time import TimePartition, partition_from_unix_seconds
except ImportError:  # pragma: no cover - Lambda zip layout
    from partition_time import TimePartition, partition_from_unix_seconds

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
    event_type: str = "purchase"  # purchase, return, exchange, support
    post_purchase_impact: float = 0.0  # PPI score


class SearchKeywordAnalyzer:
    """
    Parses a tab-separated hit-level data file and produces a revenue-by-keyword
    report for traffic originating from external search engines.
    """

    # PII handling configuration
    PII_FIELDS = {"ip", "user_agent", "geo_city", "geo_region", "geo_country"}
    
    # PII redaction methods
    @staticmethod
    def _mask_ip(ip: str) -> str:
        """Mask IP address to preserve network segment but hide host."""
        parts = ip.split(".")
        if len(parts) == 4:
            return f"{parts[0]}.{parts[1]}.{parts[2]}.xxx"
        return "xxx.xxx.xxx.xxx"
    
    @staticmethod
    def _mask_user_agent(ua: str) -> str:
        """Hash user agent for privacy while preserving uniqueness."""
        import hashlib
        return f"ua_{hashlib.sha256(ua.encode()).hexdigest()[:8]}"
    
    @staticmethod
    def _mask_location(city: str, region: str, country: str) -> tuple[str, str, str]:
        """Mask location data to region level only."""
        # Keep country, mask city/region to generic
        return "masked_city", "masked_region", country or "unknown"
    
    def _mask_pii_row(self, row: dict[str, str]) -> dict[str, str]:
        """Apply PII masking to a row if masking is enabled."""
        if not self._mask_pii:
            return row
            
        masked_row = row.copy()
        
        # Mask IP
        if "ip" in masked_row:
            masked_row["ip"] = self._mask_ip(masked_row["ip"])
            
        # Mask user agent
        if "user_agent" in masked_row:
            masked_row["user_agent"] = self._mask_user_agent(masked_row["user_agent"])
            
        # Mask location data
        city = masked_row.get("geo_city", "")
        region = masked_row.get("geo_region", "")
        country = masked_row.get("geo_country", "")
        if city or region or country:
            masked_city, masked_region, masked_country = self._mask_location(city, region, country)
            masked_row["geo_city"] = masked_city
            masked_row["geo_region"] = masked_region
            masked_row["geo_country"] = masked_country
            
        return masked_row

    def __init__(self, mask_pii: bool = True) -> None:
        self._visitor_search: dict[str, SearchAttribution] = {}
        self._revenue: dict[tuple[str, str], float] = {}
        self._ppi_scores: dict[tuple[str, str], float] = {}
        self._min_hit_ts: int | None = None
        self._max_hit_ts: int | None = None
        self._mask_pii = mask_pii

    def process_file(self, path: str | Path) -> list[RevenueRecord]:
        """Read *path*, process all rows in time order, and return aggregated revenue records."""
        with open(path, encoding="utf-8") as fh:
            return self.process_stream(fh)

    @staticmethod
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

    def process_stream(self, stream: IO[str]) -> list[RevenueRecord]:
        """Read from *stream*, sort hits by hit_time_gmt, and return aggregated revenue records.

        The function sorts all rows by hit_time_gmt to ensure consistent last-touch
        attribution.  Malformed timestamps are treated as 0, ensuring bad rows do not
        crash processing.
        """

        self._visitor_search.clear()
        self._revenue.clear()
        self._ppi_scores.clear()
        self._min_hit_ts = None
        self._max_hit_ts = None

        reader = csv.DictReader(stream, delimiter="\t")
        rows = list(reader)
        
        # Apply PII masking to all rows if enabled
        if self._mask_pii:
            rows = [self._mask_pii_row(row) for row in rows]
            
        rows.sort(key=self._safe_hit_timestamp)
        for row in rows:
            ts = self._safe_hit_timestamp(row)
            if self._min_hit_ts is None or ts < self._min_hit_ts:
                self._min_hit_ts = ts
            if self._max_hit_ts is None or ts > self._max_hit_ts:
                self._max_hit_ts = ts
            self._process_row(row)
        return self._build_sorted_results()

    def process_parquet_bytes(self, data: bytes) -> list[RevenueRecord]:
        """
        Read a Parquet buffer (Apache Arrow / PyArrow), normalize columns to the same
        names as the TSV path, sort by ``hit_time_gmt``, and aggregate.

        Required columns (case-insensitive): ``hit_time_gmt``, ``ip``, ``user_agent``,
        ``referrer``, ``event_list``, ``product_list``. Other columns are ignored.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError as e:
            raise RuntimeError(
                "Parquet input requires pyarrow. Install pyarrow in the Lambda deployment package."
            ) from e

        table = pq.read_table(io.BytesIO(data))
        if table.num_rows == 0:
            return []

        col_map = {n.lower(): n for n in table.column_names}
        required = ("hit_time_gmt", "ip", "user_agent", "referrer", "event_list", "product_list")
        missing = [c for c in required if c not in col_map]
        if missing:
            raise ValueError(
                f"Parquet input missing columns {missing}; found {list(table.column_names)}"
            )

        self._visitor_search.clear()
        self._revenue.clear()
        self._min_hit_ts = None
        self._max_hit_ts = None

        rows: list[dict[str, str]] = []
        for i in range(table.num_rows):
            row: dict[str, str] = {}
            for orig in table.column_names:
                key = orig.lower()
                cell = table.column(orig)[i].as_py()
                if cell is None:
                    row[key] = ""
                elif isinstance(cell, bytes):
                    row[key] = cell.decode("utf-8", errors="replace")
                else:
                    row[key] = str(cell)
            
            # Apply PII masking to the row
            row = self._mask_pii_row(row)
            rows.append(row)

        rows.sort(key=self._safe_hit_timestamp)
        for row in rows:
            ts = self._safe_hit_timestamp(row)
            if self._min_hit_ts is None or ts < self._min_hit_ts:
                self._min_hit_ts = ts
            if self._max_hit_ts is None or ts > self._max_hit_ts:
                self._max_hit_ts = ts
            self._process_row(row)
        return self._build_sorted_results()

    def earliest_hit_partition_utc(self) -> TimePartition | None:
        """Hive-style UTC partition from the earliest hit_time_gmt in the last process_stream run."""
        if self._min_hit_ts is None:
            return None
        return partition_from_unix_seconds(self._min_hit_ts)

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
