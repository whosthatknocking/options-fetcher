"""Scalar coercion helpers shared across opx-chain and downstream consumers."""

from __future__ import annotations

from typing import Any

import numpy as np


TRUTHY_STRINGS = frozenset({"1", "true", "yes", "y", "on"})
FALSY_STRINGS = frozenset({"0", "false", "no", "n", "off"})


def coerce_bool_or_default(value: Any, *, default: bool | None) -> bool | None:
    """Coerce common boolean-like values, returning default when unrecognized."""
    if value is None:
        return default
    result = default
    if isinstance(value, (bool, np.bool_)):
        result = bool(value)
    elif isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in TRUTHY_STRINGS:
            result = True
        elif normalized in FALSY_STRINGS:
            result = False
    elif isinstance(value, (int, float, np.integer, np.floating)):
        numeric_value = float(value)
        if numeric_value == 1.0:
            result = True
        elif numeric_value == 0.0:
            result = False
    return result
