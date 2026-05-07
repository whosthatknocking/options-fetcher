"""Shared timestamp helpers."""

from __future__ import annotations

from datetime import datetime


def parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetimes, accepting a trailing UTC ``Z`` suffix."""
    text = value.strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    return datetime.fromisoformat(normalized)
