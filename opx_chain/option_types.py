"""Canonical option-type values used by option-chain processing."""

from __future__ import annotations

from typing import Any

import pandas as pd


OPTION_TYPE_CALL = "call"
OPTION_TYPE_PUT = "put"
OPTION_TYPES = frozenset([OPTION_TYPE_CALL, OPTION_TYPE_PUT])

OPTION_TYPE_CALL_LABEL = OPTION_TYPE_CALL.upper()
OPTION_TYPE_PUT_LABEL = OPTION_TYPE_PUT.upper()


def normalize_option_type(value: Any) -> str:
    """Return canonical lowercase option type text, or an empty string."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip().lower()


def option_type_label(value: Any) -> str:
    """Return uppercase display/storage label for a known option type."""
    return normalize_option_type(value).upper()
