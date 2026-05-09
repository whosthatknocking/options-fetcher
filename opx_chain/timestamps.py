"""Shared timestamp helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(tz=timezone.utc)


def utc_now_timestamp() -> pd.Timestamp:
    """Return the current UTC timestamp as a pandas scalar."""
    return pd.Timestamp.now(tz="UTC")


def datetime_to_iso(value: datetime | None) -> str | None:
    """Serialize a datetime value to ISO text, preserving None."""
    return value.isoformat() if value is not None else None


def iso_to_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime text, preserving None."""
    return parse_iso_datetime(value) if value is not None else None


def parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetimes, accepting a trailing UTC ``Z`` suffix."""
    text = value.strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    return datetime.fromisoformat(normalized)
