"""Canonical option-type values used by option-chain processing."""

from __future__ import annotations

from typing import Any


OPTION_TYPE_CALL = "call"
OPTION_TYPE_PUT = "put"
OPTION_TYPES = frozenset([OPTION_TYPE_CALL, OPTION_TYPE_PUT])

OPTION_TYPE_CALL_LABEL = OPTION_TYPE_CALL.upper()
OPTION_TYPE_PUT_LABEL = OPTION_TYPE_PUT.upper()


def normalize_option_type(value: Any) -> str:
    """Return canonical lowercase option type text, or an empty string."""
    return str(value or "").strip().lower()


def option_type_label(value: Any) -> str:
    """Return uppercase display/storage label for a known option type."""
    return normalize_option_type(value).upper()
