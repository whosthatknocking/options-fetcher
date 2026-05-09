"""Shared timestamp helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

UTC_COMPACT_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
UTC_Z_SECONDS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
UTC_Z_MICROSECONDS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(tz=timezone.utc)


def utc_now_timestamp() -> pd.Timestamp:
    """Return the current UTC timestamp as a pandas scalar."""
    return pd.Timestamp.now(tz="UTC")


def datetime_to_iso(value: datetime | None) -> str | None:
    """Serialize a datetime value to ISO text, preserving None."""
    return value.isoformat() if value is not None else None


def _as_utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def format_utc_compact(value: Any) -> str:
    """Format a timestamp as compact UTC text for filenames/run identifiers."""
    return _as_utc_timestamp(value).strftime(UTC_COMPACT_TIMESTAMP_FORMAT)


def format_utc_z_seconds(value: Any) -> str:
    """Format a timestamp as second-precision UTC ISO text with a Z suffix."""
    return _as_utc_timestamp(value).strftime(UTC_Z_SECONDS_FORMAT)


def format_utc_z_microseconds(value: Any) -> str:
    """Format a timestamp as microsecond-precision UTC ISO text with a Z suffix."""
    return _as_utc_timestamp(value).strftime(UTC_Z_MICROSECONDS_FORMAT)


def iso_to_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime text, preserving None."""
    return parse_iso_datetime(value) if value is not None else None


def parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetimes, accepting a trailing UTC ``Z`` suffix."""
    text = value.strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    return datetime.fromisoformat(normalized)
