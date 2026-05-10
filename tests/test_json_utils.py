"""Tests for shared strict JSON and scalar-normalization helpers."""
# pylint: disable=too-few-public-methods

import ast
import json
import math
from pathlib import Path

import pandas as pd

from opx_chain.json_utils import (
    dumps_sanitized_json,
    sanitize_json_payload,
    to_python_scalar,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "opx_chain"


class _BadScalar:
    """Scalar-like object whose item conversion fails."""

    def item(self) -> object:
        """Raise a conversion failure."""
        raise ValueError("cannot convert")


class _OverflowScalar:
    """Scalar-like object whose item conversion overflows."""

    def item(self) -> object:
        """Raise an overflow conversion failure."""
        raise OverflowError("too large")


class _GoodScalar:
    """Scalar-like object whose item conversion succeeds."""

    def item(self) -> int:
        """Return the Python scalar value."""
        return 42


class _NonCallableItem:
    """Object with a non-callable item attribute."""

    item = 1


def test_to_python_scalar_defensively_calls_item() -> None:
    """Scalar conversion should use safe item calls and preserve failures."""
    good = _GoodScalar()
    bad = _BadScalar()
    overflow = _OverflowScalar()
    non_callable = _NonCallableItem()

    assert to_python_scalar(good) == 42
    assert to_python_scalar(bad) is bad
    assert to_python_scalar(overflow) is overflow
    assert to_python_scalar(non_callable) is non_callable
    assert to_python_scalar("abc") == "abc"


def test_dumps_sanitized_json_converts_nested_non_finite_values() -> None:
    """Graceful JSON responses should turn non-finite scalars into null."""
    payload = {
        "nan": math.nan,
        "pos_inf": math.inf,
        "items": [1, -math.inf],
    }

    encoded = dumps_sanitized_json(payload, sort_keys=True)

    assert "NaN" not in encoded
    assert "Infinity" not in encoded
    assert json.loads(encoded) == {"items": [1, None], "nan": None, "pos_inf": None}


def test_sanitize_json_payload_handles_scalar_like_non_finite_values() -> None:
    """Numpy/pandas scalar-like values should pass through the same sanitizer."""
    assert sanitize_json_payload(math.inf) is None
    assert sanitize_json_payload(pd.NA) is None
    assert sanitize_json_payload(pd.NaT) is None
    assert json.loads(dumps_sanitized_json({"option_quote_time": pd.NaT})) == {
        "option_quote_time": None
    }


def test_scalar_item_conversion_stays_in_json_utils() -> None:
    """Package code should delegate scalar item conversion to json_utils."""
    assert PACKAGE_ROOT.exists()
    offenders: list[str] = []
    scanned_files = 0
    for path in PACKAGE_ROOT.rglob("*.py"):
        if path == PACKAGE_ROOT / "json_utils.py":
            continue
        scanned_files += 1
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Attribute) and node.func.attr == "item":
                offenders.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}")
            if (
                isinstance(node.func, ast.Name)
                and node.func.id == "hasattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == "item"
            ):
                offenders.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}")

    assert scanned_files > 0
    assert not offenders
