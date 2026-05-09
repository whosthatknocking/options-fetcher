"""Tests for shared timestamp helpers."""

from datetime import datetime, timezone

import pandas as pd
import pytest

from opx_chain.timestamps import (
    datetime_to_iso,
    iso_to_datetime,
    parse_iso_datetime,
    utc_now,
    utc_now_timestamp,
)


def test_parse_iso_datetime_accepts_trailing_z_suffix():
    """Zulu timestamps should parse through the shared trailing-Z policy."""
    parsed = parse_iso_datetime("2026-04-22T12:00:00Z")

    assert parsed.utcoffset() == timezone.utc.utcoffset(parsed)
    assert parsed.isoformat() == "2026-04-22T12:00:00+00:00"


def test_parse_iso_datetime_does_not_replace_embedded_z():
    """Only a trailing Z should be normalized, not every Z in the string."""
    with pytest.raises(ValueError):
        parse_iso_datetime("2026-04-22TZ12:00:00Z")


def test_datetime_to_iso_and_iso_to_datetime_preserve_none_and_timezone():
    """Optional datetime serialization should share one storage policy."""
    value = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)

    assert datetime_to_iso(value) == "2026-04-22T12:00:00+00:00"
    assert iso_to_datetime("2026-04-22T12:00:00Z") == value
    assert datetime_to_iso(None) is None
    assert iso_to_datetime(None) is None


def test_utc_now_helpers_return_utc_values():
    """Datetime and pandas callers should share UTC clock helpers."""
    assert utc_now().utcoffset() == timezone.utc.utcoffset(None)

    timestamp = utc_now_timestamp()
    assert isinstance(timestamp, pd.Timestamp)
    assert str(timestamp.tz) == "UTC"
