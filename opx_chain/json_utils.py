"""Strict JSON helpers shared by storage and provider metadata paths."""

from __future__ import annotations

import json
from typing import Any


def _reject_non_finite_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON value is not allowed: {value}")


def loads_strict_json(text: str) -> Any:
    """Load JSON while rejecting non-standard NaN and Infinity constants."""
    return json.loads(text, parse_constant=_reject_non_finite_json_constant)


def dumps_strict_json(value: Any, **kwargs: Any) -> str:
    """Dump JSON while rejecting non-standard NaN and Infinity values."""
    kwargs["allow_nan"] = False
    return json.dumps(value, **kwargs)
