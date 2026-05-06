"""Standalone price-context CLI tests."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from conftest import make_runtime_config
from opx_chain.positions import EMPTY_POSITION_SET
from opx_chain.storage.memory import MemoryBackend


class StubProvider:  # pylint: disable=too-few-public-methods
    """Provider stub that captures per-ticker preparation calls."""

    name = "stub"

    def __init__(self):
        self.prepared_tickers = []

    def prepare_ticker_fetch(self, ticker):
        """Record ticker preparation before a standalone context fetch."""
        self.prepared_tickers.append(ticker)


class PriceContextFetchStub:  # pylint: disable=too-few-public-methods
    """Callable price-context fetch stub with captured config arguments."""

    def __init__(self):
        self.seen_configs = []

    def __call__(self, ticker, *, provider=None, logger=None, config=None):
        """Return deterministic price context for the requested ticker."""
        del provider, logger
        self.seen_configs.append(config)
        return {
            "support_1": 90.0,
            "support_2": 85.0,
            "resistance_1": 110.0,
            "resistance_2": 115.0,
            "20d_high": 112.0,
            "20d_low": 88.0,
            "50dma": 101.0,
            "200dma": 99.0,
            "vwap": 100.0,
            "volume_profile_high_volume_node": 98.0,
            "gap_fill_level": None,
            "pre_earnings_move_pct": None,
            "price_context_as_of": "2026-03-20",
            "price_context_age_days": 0,
            "price_context_source": "stub",
            "price_context_lookback_trading_days": 260,
            "price_context_calculation_method": "daily_ohlcv_v1",
            "price_context_staleness_status": "FRESH",
            "ticker_seen": ticker,
        }


def test_price_context_only_writes_json_without_option_export(tmp_path: Path, capsys):
    """--price-context-only must fetch daily context without option-chain side effects."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    storage_dir = tmp_path / "data"
    config = make_runtime_config(
        storage_enabled=True,
        storage_dir=storage_dir,
        tickers=("AAA", "BBB"),
        price_context_enable=False,
    )
    provider = StubProvider()
    price_fetch = PriceContextFetchStub()

    with (
        patch.object(fetcher, "FETCHER_LOCK_PATH", tmp_path / "fetcher.lock"),
        patch.object(fetcher, "acquire_fetcher_lock", return_value=MagicMock()),
        patch.object(fetcher, "release_fetcher_lock"),
        patch.object(fetcher, "get_runtime_config", return_value=config),
        patch.object(fetcher, "set_runtime_config_override"),
        patch.object(
            fetcher,
            "create_run_logger",
            return_value=(MagicMock(), tmp_path / "run.log"),
        ),
        patch.object(fetcher, "get_storage_backend", return_value=backend),
        patch.object(fetcher, "load_positions", return_value=EMPTY_POSITION_SET),
        patch.object(fetcher, "get_data_provider", return_value=provider),
        patch.object(
            fetcher,
            "fetch_ticker_price_context",
            side_effect=price_fetch,
        ) as mock_price_context,
        patch.object(fetcher, "fetch_ticker_option_chain") as mock_option_chain,
        patch.object(fetcher, "write_options_csv") as mock_write_csv,
    ):
        result = fetcher.main(["--price-context-only"])

    captured = capsys.readouterr()
    assert result == 0
    assert "CLI override: price_context_enable=true, price_context_only=true" in captured.out
    assert "Saved price context:" in captured.out
    assert "AAA: price_context  status=FRESH  as_of=2026-03-20" in captured.out
    assert "BBB: price_context  status=FRESH  as_of=2026-03-20" in captured.out
    assert provider.prepared_tickers == ["AAA", "BBB"]
    assert [call.args[0] for call in mock_price_context.call_args_list] == ["AAA", "BBB"]
    assert all(config.price_context_enable is True for config in price_fetch.seen_configs)
    mock_option_chain.assert_not_called()
    mock_write_csv.assert_not_called()
    assert not backend.list_datasets()

    latest_path = storage_dir / "runs" / "price_context_latest.json"
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "price_context"
    assert payload["provider"] == "yfinance"
    assert payload["tickers"] == ["AAA", "BBB"]
    assert [record["ticker"] for record in payload["records"]] == ["AAA", "BBB"]
    assert all(
        record["price_context_staleness_status"] == "FRESH"
        for record in payload["records"]
    )


def test_price_context_only_conflicts_with_disable_flag():
    """Standalone price-context mode cannot also disable price-context fetching."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    with pytest.raises(SystemExit):
        fetcher.parse_args(["--price-context-only", "--disable-price-context"])
