"""Tests for the durable daily price-history store."""

from datetime import date, datetime, timedelta, timezone
import sqlite3

import pandas as pd
import pytest

from conftest import make_runtime_config
from opx_chain.price_history import PriceHistoryStore, reconcile_price_history


def _history(end: str = "2026-03-20", periods: int = 20) -> pd.DataFrame:
    dates = pd.bdate_range(end=end, periods=periods)
    closes = [100.0 + index * 0.1 for index in range(periods)]
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": [close - 0.2 for close in closes],
            "High": [close + 0.5 for close in closes],
            "Low": [close - 0.5 for close in closes],
            "Close": closes,
            "Volume": [1000 + index for index in range(periods)],
        }
    )


class HistoryProvider:  # pylint: disable=too-few-public-methods
    """Provider stub that records requested lookback windows."""

    name = "stub"

    def __init__(self, *, end: str = "2026-03-20"):
        self.end = end
        self.lookback_calls: list[int] = []

    def load_price_history(self, ticker, *, lookback_days):  # pylint: disable=unused-argument
        """Return deterministic daily bars and record the requested window."""
        self.lookback_calls.append(lookback_days)
        return _history(end=self.end, periods=lookback_days)


class FailingConnection:
    """Connection stub that verifies failed write transactions roll back."""

    def __init__(self) -> None:
        self.rolled_back = False

    def execute(self, *_args, **_kwargs):
        """Fail every write statement so the transaction context handles rollback."""
        raise sqlite3.OperationalError("forced failure")

    def rollback(self) -> None:
        """Record that rollback was requested."""
        self.rolled_back = True


def test_price_history_store_enables_sqlite_foreign_keys(tmp_path):
    """Price-history connections should use the same FK guard as sibling stores."""
    store = PriceHistoryStore(tmp_path / "price-history.db")

    enabled = store._connection_for_use().execute("PRAGMA foreign_keys").fetchone()[0]  # pylint: disable=protected-access

    assert enabled == 1


def test_price_history_store_detaches_finalizer_on_close(tmp_path):
    """Price-history pooled connections should mirror sibling SQLite cleanup."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    finalizer = store._connection_finalizer  # pylint: disable=protected-access

    assert finalizer is not None
    assert finalizer.alive

    store.close()

    assert store._connection is None  # pylint: disable=protected-access
    assert store._connection_finalizer is None  # pylint: disable=protected-access
    assert not finalizer.alive


def test_price_history_store_rolls_back_failed_write(tmp_path, monkeypatch):
    """Failed price-history writes must not leave dirty pooled transactions."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    connection = FailingConnection()
    monkeypatch.setattr(store, "_connection_for_use", lambda: connection)

    with pytest.raises(sqlite3.OperationalError):
        store.record_sync(
            provider="stub",
            ticker="AAA",
            lookback_days=30,
            status="error",
            requested_lookback_days=30,
            latest_trading_date=None,
            fetched_rows=0,
            stored_rows=0,
        )

    assert connection.rolled_back


def test_reconcile_price_history_backfills_new_ticker(tmp_path):
    """New tickers should fetch the configured lookback and persist local bars."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    provider = HistoryProvider()
    config = make_runtime_config(
        today=date(2026, 3, 20),
        price_context_lookback_days=30,
        provider_price_context_ttl=86400,
    )

    result = reconcile_price_history(
        ticker="AAA",
        provider=provider,
        config=config,
        store=store,
    )

    assert result.fetched is True
    assert result.requested_lookback_days == 30
    assert len(result.history) == 30
    assert provider.lookback_calls == [30]
    assert store.stats(provider="stub", ticker="AAA").row_count == 30


def test_reconcile_price_history_uses_store_when_coverage_is_current(tmp_path):
    """Existing current coverage should avoid provider calls."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="stub", ticker="AAA", history=_history(periods=30))
    provider = HistoryProvider()
    config = make_runtime_config(
        today=date(2026, 3, 20),
        price_context_lookback_days=30,
        provider_price_context_ttl=86400,
    )

    result = reconcile_price_history(
        ticker="AAA",
        provider=provider,
        config=config,
        store=store,
    )

    assert result.fetched is False
    assert len(result.history) == 30
    assert not provider.lookback_calls


def test_reconcile_price_history_fetches_tail_delta(tmp_path):
    """Stale local tails should fetch only the recent delta window."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="stub", ticker="AAA", history=_history(end="2026-03-18", periods=30))
    provider = HistoryProvider(end="2026-03-20")
    config = make_runtime_config(
        today=date(2026, 3, 20),
        price_context_lookback_days=30,
        provider_price_context_ttl=86400,
    )

    result = reconcile_price_history(
        ticker="AAA",
        provider=provider,
        config=config,
        store=store,
    )

    assert result.fetched is True
    assert result.requested_lookback_days == 7
    assert provider.lookback_calls == [7]
    assert store.stats(provider="stub", ticker="AAA").latest_date == date(2026, 3, 20)


def test_reconcile_price_history_respects_recent_sync_ttl(tmp_path):
    """A recent sync should prevent repeated provider calls for the same missing tail."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="stub", ticker="AAA", history=_history(end="2026-03-18", periods=30))
    store.record_sync(
        provider="stub",
        ticker="AAA",
        lookback_days=30,
        status="ok",
        requested_lookback_days=7,
        latest_trading_date=date(2026, 3, 18),
        fetched_rows=7,
        stored_rows=7,
        checked_at=datetime.now(tz=timezone.utc) - timedelta(seconds=10),
    )
    provider = HistoryProvider(end="2026-03-20")
    config = make_runtime_config(
        today=date(2026, 3, 20),
        price_context_lookback_days=30,
        provider_price_context_ttl=86400,
    )

    result = reconcile_price_history(
        ticker="AAA",
        provider=provider,
        config=config,
        store=store,
    )

    assert result.fetched is False
    assert not provider.lookback_calls
