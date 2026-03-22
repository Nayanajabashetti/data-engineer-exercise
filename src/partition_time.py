"""
Time partitioning helpers for hit-level data (Adobe Analytics hit_time_gmt = Unix seconds UTC).

Human-readable Hive-style paths:
  .../dt=YYYY-MM-DD/hour=HH/minute=<bucket>/

The **partition interval** (e.g. 15 → 00,15,30,45) is **not** hard-coded: use environment
``PARTITION_INTERVAL_MINUTES`` (Lambda) or Glue ``--partition_interval_minutes`` so one
change propagates consistently. Defaults to :data:`DEFAULT_PARTITION_INTERVAL_MINUTES`.

If you change the interval in production, treat **Staging** as re-buildable from **Landing**
and expect Athena/catalog evolution (multiple partition schemes or backfills) — see docs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone

# Single source of truth for the default; Terraform/Lambda should match.
DEFAULT_PARTITION_INTERVAL_MINUTES = 15
_ENV_KEY = "PARTITION_INTERVAL_MINUTES"


def _validate_interval(interval_minutes: int) -> int:
    if interval_minutes < 1 or interval_minutes > 60:
        raise ValueError(
            f"partition interval must be between 1 and 60 minutes inclusive; got {interval_minutes!r}"
        )
    return interval_minutes


def get_partition_interval_minutes() -> int:
    """
    Active interval from environment (Lambda, local) or default.

    Set ``PARTITION_INTERVAL_MINUTES`` to the same value as Glue's
    ``--partition_interval_minutes``.
    """
    raw = os.environ.get(_ENV_KEY, str(DEFAULT_PARTITION_INTERVAL_MINUTES))
    try:
        v = int(str(raw).strip())
    except ValueError:
        v = DEFAULT_PARTITION_INTERVAL_MINUTES
    return _validate_interval(v)


def get_minute_bucket(minute: int, interval_minutes: int | None = None) -> str:
    """
    Bucket label for *minute* (0–59) using a configurable interval.

    ``bucket_val = (minute // interval_minutes) * interval_minutes``, then zero-padded width 2.

    Examples (interval 15): 0–14 → 00, 15–29 → 15, …
    """
    iv = interval_minutes if interval_minutes is not None else get_partition_interval_minutes()
    iv = _validate_interval(iv)
    bucket_val = (minute // iv) * iv
    return str(bucket_val).zfill(2)


def minute_bucket_from_minute(minute: int) -> str:
    """Backward-compatible alias for :func:`get_minute_bucket` using env/default interval."""
    return get_minute_bucket(minute)


def valid_minute_bucket_strings(interval_minutes: int) -> frozenset[str]:
    """All valid ``minute=`` folder labels for Glue path pruning (e.g. 00,15,30,45 when interval is 15)."""
    iv = _validate_interval(interval_minutes)
    return frozenset(str(m).zfill(2) for m in range(0, 60, iv))


@dataclass(frozen=True)
class TimePartition:
    """UTC calendar partition for one hit timestamp (seconds since epoch)."""

    dt: str  # YYYY-MM-DD
    hour: str  # 00–23
    minute_bucket: str  # depends on partition interval (e.g. 00, 15, 30, 45 for 15m)


def partition_from_unix_seconds(ts: int | None, interval_minutes: int | None = None) -> TimePartition:
    """
    Map Unix seconds (UTC) to day / hour / minute bucket.

    ``interval_minutes`` overrides env for tests and single-call control; if ``None``, uses
    :func:`get_partition_interval_minutes`.

    Non-numeric or negative ``ts`` are treated as 0 (epoch UTC).
    """
    iv = interval_minutes if interval_minutes is not None else get_partition_interval_minutes()
    if ts is None or ts < 0:
        ts = 0
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return TimePartition(
        dt=dt.strftime("%Y-%m-%d"),
        hour=f"{dt.hour:02d}",
        minute_bucket=get_minute_bucket(dt.minute, iv),
    )


def partition_prefix(tp: TimePartition) -> str:
    """Hive-style path segment: dt=.../hour=.../minute=..."""
    return f"dt={tp.dt}/hour={tp.hour}/minute={tp.minute_bucket}"
