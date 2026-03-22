"""Tests for S3 medallion layer prefix helpers."""

import pytest

from src.s3_data_layers import DataLayerPrefixes, default_layer_prefixes, s3_uri


def test_default_prefixes_end_with_slash():
    d = default_layer_prefixes()
    assert d.landing.endswith("/")
    assert d.staging.endswith("/")
    assert d.curated.endswith("/")


def test_s3_uri():
    assert s3_uri("my-bucket", "landing/") == "s3://my-bucket/landing/"
    assert s3_uri("my-bucket", "curated/search_keyword/") == "s3://my-bucket/curated/search_keyword/"


def test_invalid_prefix_without_slash():
    with pytest.raises(ValueError, match="landing"):
        DataLayerPrefixes(landing="bad", staging="staging/", curated="curated/")


def test_empty_staging_allowed():
    d = DataLayerPrefixes(landing="landing/", staging="", curated="curated/")
    assert d.staging == ""
