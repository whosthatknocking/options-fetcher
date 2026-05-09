"""Tests for canonical option-type values."""

from __future__ import annotations

import ast
from pathlib import Path

from opx_chain.option_types import (
    OPTION_TYPE_CALL,
    OPTION_TYPE_CALL_LABEL,
    OPTION_TYPE_PUT,
    OPTION_TYPE_PUT_LABEL,
    OPTION_TYPES,
    normalize_option_type,
    option_type_label,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "opx_chain"


def test_option_type_helpers_normalize_values() -> None:
    """Option-type helpers should own casing and whitespace normalization."""
    assert OPTION_TYPE_CALL == "call"
    assert OPTION_TYPE_PUT == "put"
    assert OPTION_TYPES == frozenset({OPTION_TYPE_CALL, OPTION_TYPE_PUT})
    assert OPTION_TYPE_CALL_LABEL == "CALL"
    assert OPTION_TYPE_PUT_LABEL == "PUT"
    assert normalize_option_type(" CALL ") == OPTION_TYPE_CALL
    assert option_type_label(" put ") == OPTION_TYPE_PUT_LABEL
    assert normalize_option_type(None) == ""


def test_option_type_literals_are_centralized_in_production_code() -> None:
    """Production code should use option_types.py for exact call/put values."""
    violations: list[str] = []

    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if path.relative_to(PACKAGE_ROOT) == Path("option_types.py"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and node.value in {OPTION_TYPE_CALL, OPTION_TYPE_PUT}
            ):
                violations.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}")

    assert not violations
