"""Price-context calculation tests."""

from datetime import date

import pandas as pd
import pytest

from opx_chain.price_context import (
    PRICE_CONTEXT_FIELDS,
    blank_price_context,
    compute_price_context,
)


def _history(start: str = "2025-07-01", periods: int = 220) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=periods)
    closes = [100.0 + index * 0.2 for index in range(periods)]
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": [close - 0.5 for close in closes],
            "High": [close + 1.0 for close in closes],
            "Low": [close - 1.5 for close in closes],
            "Close": closes,
            "Volume": [1000 + index for index in range(periods)],
        }
    )


def test_compute_price_context_derives_daily_ohlcv_boundaries():
    """Daily OHLCV history should produce deterministic flat context fields."""
    history = _history()

    context = compute_price_context(
        history,
        source="unit",
        today=date(2026, 5, 5),
        max_age_days=7,
    )

    assert set(PRICE_CONTEXT_FIELDS).issubset(context)
    assert context["price_context_staleness_status"] == "FRESH"
    assert context["price_context_source"] == "unit"
    assert context["price_context_lookback_trading_days"] == 220
    assert context["price_context_as_of"] == "2026-05-04"
    assert context["20d_high"] == pytest.approx(144.8)
    assert context["20d_low"] == pytest.approx(138.5)
    assert context["50dma"] == pytest.approx(138.9)
    assert context["200dma"] == pytest.approx(123.9)
    assert context["support_1"] == pytest.approx(143.633333)
    assert context["support_2"] == pytest.approx(138.9)
    assert context["resistance_1"] == pytest.approx(144.8)
    assert context["vwap"] > 0
    assert context["volume_profile_high_volume_node"] > 0
    assert context["pre_earnings_move_pct"] is None


def test_compute_price_context_blanks_stale_numeric_fields():
    """Stale price history should inform status without exporting stale levels."""
    context = compute_price_context(
        _history(periods=20),
        source="unit",
        today=date(2026, 6, 1),
        max_age_days=7,
    )

    assert context["price_context_staleness_status"] == "STALE"
    assert context["price_context_as_of"] == "2025-07-28"
    assert context["price_context_age_days"] > 7
    assert all(context[field] is None for field in PRICE_CONTEXT_FIELDS)


def test_compute_price_context_returns_blank_payload_for_missing_history():
    """Missing or malformed history should not raise."""
    context = compute_price_context(
        pd.DataFrame({"Close": [100.0]}),
        source="unit",
        today=date(2026, 5, 5),
        max_age_days=7,
    )

    assert context == blank_price_context(source="unit")
