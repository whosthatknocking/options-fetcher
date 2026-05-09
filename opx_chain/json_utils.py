"""Strict JSON helpers shared by storage and provider metadata paths."""

from __future__ import annotations

from datetime import date, datetime
import json
import math
from typing import Any

import pandas as pd


def to_python_scalar(value: Any) -> Any:
    """Return a Python scalar for numpy/pandas scalar-like values when safe."""
    if isinstance(value, str | bytes | bytearray):
        return value
    item = getattr(value, "item", None)
    if not callable(item):
        return value
    try:
        return item()
    except (OverflowError, TypeError, ValueError):
        return value


def _reject_non_finite_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON value is not allowed: {value}")


def loads_strict_json(text: str) -> Any:
    """Load JSON while rejecting non-standard NaN and Infinity constants."""
    return json.loads(text, parse_constant=_reject_non_finite_json_constant)


def dumps_strict_json(value: Any, **kwargs: Any) -> str:
    """Dump JSON while rejecting non-standard NaN and Infinity values."""
    kwargs["allow_nan"] = False
    return json.dumps(value, **kwargs)


def sanitize_json_payload(value: Any) -> Any:  # pylint: disable=too-many-return-statements
    """Convert nested payload values into strict JSON-safe primitives."""
    if isinstance(value, dict):
        return {key: sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_json_payload(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, str | bytes | bytearray) or value is None:
        return value
    scalar = to_python_scalar(value)
    if scalar is not value:
        return sanitize_json_payload(scalar)
    return value


def dumps_sanitized_json(value: Any, **kwargs: Any) -> str:
    """Dump JSON after converting non-finite scalar values to null."""
    kwargs["allow_nan"] = False
    return json.dumps(sanitize_json_payload(value), **kwargs)
