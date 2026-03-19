"""Unit tests for SearchKeywordAnalyzer."""

import io
import tempfile
from pathlib import Path

import pytest

from src.search_keyword_analyzer import (
    RevenueRecord,
    SearchAttribution,
    SearchKeywordAnalyzer,
)


# ── Referrer parsing ──────────────────────────────────────────────

class TestParseSearchReferrer:

    @pytest.mark.parametrize(
        "referrer,expected_domain,expected_keyword",
        [
            (
                "http://www.google.com/search?hl=en&q=Ipod",
                "google.com",
                "ipod",
            ),
            (
                "http://www.bing.com/search?q=Zune&form=QBLH",
                "bing.com",
                "zune",
            ),
            (
                "http://search.yahoo.com/search?p=cd+player&ei=UTF-8",
                "yahoo.com",
                "cd player",
            ),
        ],
    )
    def test_known_engines(self, referrer, expected_domain, expected_keyword):
        result = SearchKeywordAnalyzer._parse_search_referrer(referrer)
        assert result is not None
        assert result.engine_domain == expected_domain
        assert result.keyword == expected_keyword

    def test_google_uk_like_domain_is_parsed(self):
        # Documents the known limitation around ccTLDs but ensures we still
        # recognise google as the engine and extract the keyword.
        referrer = "https://www.google.co.uk/search?q=Ipod"
        result = SearchKeywordAnalyzer._parse_search_referrer(referrer)
        assert result is not None
        assert result.keyword == "ipod"

    def test_internal_referrer_returns_none(self):
        result = SearchKeywordAnalyzer._parse_search_referrer(
            "http://www.esshopzilla.com/product/?pid=123"
        )
        assert result is None

    def test_empty_referrer_returns_none(self):
        assert SearchKeywordAnalyzer._parse_search_referrer("") is None

    def test_url_decoded_keywords(self):
        result = SearchKeywordAnalyzer._parse_search_referrer(
            "http://www.google.com/search?q=Laffy%20Taffy"
        )
        assert result is not None
        assert result.keyword == "laffy taffy"

    def test_no_query_param_returns_none(self):
        result = SearchKeywordAnalyzer._parse_search_referrer(
            "http://www.google.com/"
        )
        assert result is None


# ── Product revenue parsing ───────────────────────────────────────

class TestParseProductRevenue:

    def test_single_product_with_revenue(self):
        assert SearchKeywordAnalyzer._parse_product_revenue(
            "Electronics;Zune - 32GB;1;250;"
        ) == 250.0

    def test_multiple_products(self):
        assert SearchKeywordAnalyzer._parse_product_revenue(
            "Computers;HP Pavillion;1;1000;200|201,Office Supplies;Red Folders;4;4.00;205"
        ) == 1004.0

    def test_no_revenue_field(self):
        assert SearchKeywordAnalyzer._parse_product_revenue(
            "Electronics;Zune - 328GB;1;;"
        ) == 0.0

    def test_empty_string(self):
        assert SearchKeywordAnalyzer._parse_product_revenue("") == 0.0


# ── End-to-end with the sample data ──────────────────────────────

SAMPLE_DATA = """\
hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27 06:34:40\tMozilla/5.0\t67.98.123.1\t\tSalem\tOR\tUS\tHome\thttp://www.esshopzilla.com\t\thttp://www.google.com/search?hl=en&q=Ipod
1254033379\t2009-09-27 06:36:19\tMozilla/5.0\t23.8.61.21\t2\tRochester\tNY\tUS\tZune\thttp://www.esshopzilla.com/product/?pid=asfe13\tElectronics;Zune - 328GB;1;;\thttp://www.bing.com/search?q=Zune&form=QBLH
1254033478\t2009-09-27 06:37:58\tMozilla/5.0\t112.33.98.231\t\tSalt Lake City\tUT\tUS\tHome\thttp://www.esshopzilla.com\t\thttp://search.yahoo.com/search?p=cd+player&ei=UTF-8
1254033577\t2009-09-27 06:39:37\tMozilla/5.0\t44.12.96.2\t\tDuncan\tOK\tUS\tHot Buys\thttp://www.esshopzilla.com/hotbuys/\t\thttp://www.google.com/search?hl=en&q=ipod
1254034666\t2009-09-27 06:57:46\tMozilla/5.0\t23.8.61.21\t1\tRochester\tNY\tUS\tOrder Complete\thttps://www.esshopzilla.com/checkout/?a=complete\tElectronics;Zune - 32GB;1;250;\thttps://www.esshopzilla.com/checkout/?a=confirm
1254034963\t2009-09-27 07:02:43\tMozilla/5.0\t44.12.96.2\t1\tDuncan\tOK\tUS\tOrder Complete\thttps://www.esshopzilla.com/checkout/?a=complete\tElectronics;Ipod - Nano - 8GB;1;190;\thttps://www.esshopzilla.com/checkout/?a=confirm
1254035260\t2009-09-27 07:07:40\tMozilla/5.0\t67.98.123.1\t1\tSalem\tOR\tUS\tOrder Complete\thttps://www.esshopzilla.com/checkout/?a=complete\tElectronics;Ipod - Touch - 32GB;1;290;\thttps://www.esshopzilla.com/checkout/?a=confirm
"""


class TestEndToEnd:

    def test_sample_data_revenue_attribution(self):
        analyzer = SearchKeywordAnalyzer()
        results = analyzer.process_stream(io.StringIO(SAMPLE_DATA))

        assert len(results) == 2

        # "Ipod" and "ipod" now normalize to "ipod" -> $290 + $190 = $480
        assert results[0].engine_domain == "google.com"
        assert results[0].keyword == "ipod"
        assert results[0].revenue == 480.0

        assert results[1].engine_domain == "bing.com"
        assert results[1].keyword == "zune"
        assert results[1].revenue == 250.0

    def test_no_purchase_events_yields_empty(self):
        data = """\
hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27 06:34:40\tMozilla/5.0\t67.98.123.1\t\tSalem\tOR\tUS\tHome\thttp://www.esshopzilla.com\t\thttp://www.google.com/search?q=Ipod
"""
        analyzer = SearchKeywordAnalyzer()
        results = analyzer.process_stream(io.StringIO(data))
        assert results == []

    def test_purchase_without_search_referrer_not_counted(self):
        data = """\
hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27 06:34:40\tMozilla/5.0\t99.99.99.1\t\tSalem\tOR\tUS\tHome\thttp://www.esshopzilla.com\t\thttp://www.esshopzilla.com/
1254034666\t2009-09-27 06:57:46\tMozilla/5.0\t99.99.99.1\t1\tSalem\tOR\tUS\tComplete\thttp://www.esshopzilla.com/checkout\tElectronics;Widget;1;500;\thttp://www.esshopzilla.com/cart/
"""
        analyzer = SearchKeywordAnalyzer()
        results = analyzer.process_stream(io.StringIO(data))
        assert results == []

    def test_aggregates_multiple_purchases_same_keyword(self):
        data = """\
hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27 06:34:40\tMozilla/5.0\t10.0.0.1\t\tCity\tST\tUS\tHome\thttp://shop.com\t\thttp://www.google.com/search?q=Laptop
1254034666\t2009-09-27 06:57:46\tMozilla/5.0\t10.0.0.1\t1\tCity\tST\tUS\tDone\thttp://shop.com/done\tComputers;Laptop;1;500;\thttp://shop.com/cart
1254035000\t2009-09-27 07:10:00\tMozilla/5.0\t10.0.0.1\t1\tCity\tST\tUS\tDone\thttp://shop.com/done\tAccessories;Case;1;50;\thttp://shop.com/cart
"""
        analyzer = SearchKeywordAnalyzer()
        results = analyzer.process_stream(io.StringIO(data))
        assert len(results) == 1
        assert results[0].keyword == "laptop"
        assert results[0].revenue == 550.0

    def test_out_of_order_hits_are_sorted_by_time(self):
        # Same as SAMPLE_DATA but with rows deliberately shuffled to be out of order.
        data = """\
hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254034666\t2009-09-27 06:57:46\tMozilla/5.0\t23.8.61.21\t1\tRochester\tNY\tUS\tOrder Complete\thttps://www.esshopzilla.com/checkout/?a=complete\tElectronics;Zune - 32GB;1;250;\thttps://www.esshopzilla.com/checkout/?a=confirm
1254033280\t2009-09-27 06:34:40\tMozilla/5.0\t67.98.123.1\t\tSalem\tOR\tUS\tHome\thttp://www.esshopzilla.com\t\thttp://www.google.com/search?hl=en&q=Ipod
1254035260\t2009-09-27 07:07:40\tMozilla/5.0\t67.98.123.1\t1\tSalem\tOR\tUS\tOrder Complete\thttps://www.esshopzilla.com/checkout/?a=complete\tElectronics;Ipod - Touch - 32GB;1;290;\thttps://www.esshopzilla.com/checkout/?a=confirm
"""
        analyzer = SearchKeywordAnalyzer()
        results = analyzer.process_stream(io.StringIO(data))
        assert len(results) == 1
        assert results[0].engine_domain == "google.com"
        assert results[0].keyword == "ipod"
        assert results[0].revenue == 290.0

    def test_non_numeric_hit_time_gmt_does_not_crash(self):
        data = """\
hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
bad\t2009-09-27 06:34:40\tMozilla/5.0\t10.0.0.1\t\tCity\tST\tUS\tHome\thttp://shop.com\t\thttp://www.google.com/search?q=Laptop
1254034666\t2009-09-27 06:57:46\tMozilla/5.0\t10.0.0.1\t1\tCity\tST\tUS\tDone\thttp://shop.com/done\tComputers;Laptop;1;500;\thttp://shop.com/cart
"""
        analyzer = SearchKeywordAnalyzer()
        results = analyzer.process_stream(io.StringIO(data))
        assert len(results) == 1
        assert results[0].keyword == "laptop"
        assert results[0].revenue == 500.0

    def test_multi_keyword_visitor_attributes_to_most_recent(self):
        data = """\
hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27 06:34:40\tMozilla/5.0\t10.0.0.1\t\tCity\tST\tUS\tHome\thttp://shop.com\t\thttp://www.google.com/search?q=ipod
1254033380\t2009-09-27 06:36:20\tMozilla/5.0\t10.0.0.1\t\tCity\tST\tUS\tHome\thttp://shop.com\t\thttp://www.google.com/search?q=macbook
1254034666\t2009-09-27 06:57:46\tMozilla/5.0\t10.0.0.1\t1\tCity\tST\tUS\tDone\thttp://shop.com/done\tComputers;Laptop;1;500;\thttp://shop.com/cart
"""
        analyzer = SearchKeywordAnalyzer()
        results = analyzer.process_stream(io.StringIO(data))
        assert len(results) == 1
        assert results[0].keyword == "macbook"
        assert results[0].revenue == 500.0


# ── Output file ───────────────────────────────────────────────────

class TestWriteOutput:

    def test_output_file_format(self):
        analyzer = SearchKeywordAnalyzer()
        records = [
            RevenueRecord("google.com", "ipod", 480.0),
            RevenueRecord("bing.com", "zune", 250.0),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = analyzer.write_output(records, tmpdir)
            assert path.suffix == ".tab"
            assert "SearchKeywordPerformance" in path.name

            lines = path.read_text().splitlines()
            assert lines[0] == "Search Engine Domain\tSearch Keyword\tRevenue"
            assert lines[1] == "google.com\tipod\t480.00"
            assert lines[2] == "bing.com\tzune\t250.00"
