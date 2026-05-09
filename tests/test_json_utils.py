"""Tests for shared strict JSON and scalar-normalization helpers."""
# pylint: disable=too-few-public-methods

from pathlib import Path

from opx_chain.json_utils import to_python_scalar

PACKAGE_ROOT = Path("opx_chain")


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


def test_scalar_item_conversion_stays_in_json_utils() -> None:
    """Package code should delegate scalar item conversion to json_utils."""
    offenders: list[str] = []
    for path in PACKAGE_ROOT.rglob("*.py"):
        if path == PACKAGE_ROOT / "json_utils.py":
            continue
        source = path.read_text(encoding="utf-8")
        if ".item()" in source or 'hasattr(value, "item")' in source:
            offenders.append(str(path))

    assert not offenders
