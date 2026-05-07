"""Tests for shared timestamp helpers."""

from datetime import timezone

import pytest

from opx_chain.timestamps import parse_iso_datetime


def test_parse_iso_datetime_accepts_trailing_z_suffix():
    """Zulu timestamps should parse through the shared trailing-Z policy."""
    parsed = parse_iso_datetime("2026-04-22T12:00:00Z")

    assert parsed.utcoffset() == timezone.utc.utcoffset(parsed)
    assert parsed.isoformat() == "2026-04-22T12:00:00+00:00"


def test_parse_iso_datetime_does_not_replace_embedded_z():
    """Only a trailing Z should be normalized, not every Z in the string."""
    with pytest.raises(ValueError):
        parse_iso_datetime("2026-04-22TZ12:00:00Z")
