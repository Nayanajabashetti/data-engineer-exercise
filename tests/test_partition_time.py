"""Tests for configurable UTC minute-bucket partitioning."""

from datetime import datetime, timezone

import pytest

from src.partition_time import (
    DEFAULT_PARTITION_INTERVAL_MINUTES,
    TimePartition,
    get_minute_bucket,
    get_partition_interval_minutes,
    partition_from_unix_seconds,
    partition_prefix,
    valid_minute_bucket_strings,
)


def test_default_interval_constant():
    assert DEFAULT_PARTITION_INTERVAL_MINUTES == 15


def test_epoch_zero_explicit_interval():
    tp = partition_from_unix_seconds(0, interval_minutes=15)
    assert tp == TimePartition(dt="1970-01-01", hour="00", minute_bucket="00")


@pytest.mark.parametrize(
    "minute,expected",
    [
        (0, "00"),
        (14, "00"),
        (15, "15"),
        (29, "15"),
        (30, "30"),
        (44, "30"),
        (45, "45"),
        (59, "45"),
    ],
)
def test_minute_bucket_labels_interval_15(minute, expected):
    assert get_minute_bucket(minute, interval_minutes=15) == expected
    ts = int(datetime(2024, 6, 15, 10, minute, 0, tzinfo=timezone.utc).timestamp())
    tp = partition_from_unix_seconds(ts, interval_minutes=15)
    assert tp.dt == "2024-06-15"
    assert tp.hour == "10"
    assert tp.minute_bucket == expected


def test_interval_5_minutes():
    assert get_minute_bucket(7, interval_minutes=5) == "05"
    assert get_minute_bucket(12, interval_minutes=5) == "10"
    ts = int(datetime(2024, 6, 15, 10, 12, 0, tzinfo=timezone.utc).timestamp())
    tp = partition_from_unix_seconds(ts, interval_minutes=5)
    assert tp.minute_bucket == "10"


def test_valid_minute_buckets_15():
    assert valid_minute_bucket_strings(15) == frozenset({"00", "15", "30", "45"})


def test_valid_minute_buckets_10():
    assert valid_minute_bucket_strings(10) == frozenset({"00", "10", "20", "30", "40", "50"})


def test_partition_prefix_format():
    tp = TimePartition(dt="2024-06-15", hour="09", minute_bucket="30")
    assert partition_prefix(tp) == "dt=2024-06-15/hour=09/minute=30"


def test_negative_ts_treated_as_epoch():
    tp = partition_from_unix_seconds(-1, interval_minutes=15)
    assert tp.dt == "1970-01-01"


def test_none_treated_as_epoch():
    tp = partition_from_unix_seconds(None, interval_minutes=15)
    assert tp.minute_bucket == "00"


def test_invalid_interval_raises():
    with pytest.raises(ValueError):
        get_minute_bucket(10, interval_minutes=0)
    with pytest.raises(ValueError):
        get_minute_bucket(10, interval_minutes=61)


def test_env_interval(monkeypatch):
    monkeypatch.setenv("PARTITION_INTERVAL_MINUTES", "30")
    assert get_partition_interval_minutes() == 30
    ts = int(datetime(2024, 6, 15, 10, 45, 0, tzinfo=timezone.utc).timestamp())
    tp = partition_from_unix_seconds(ts)
    assert tp.minute_bucket == "30"
