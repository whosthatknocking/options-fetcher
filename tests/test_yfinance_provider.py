"""YFinance provider tests covering snapshot normalization, events, and debug payload dumping."""

from datetime import date
from pathlib import Path
import json

import pandas as pd
import pytest

from conftest import make_runtime_config
from opx_chain.providers.yfinance import YFinanceProvider


class FakeChain:  # pylint: disable=too-few-public-methods
    """Minimal yfinance option-chain stand-in."""

    def __init__(self):
        self.calls = pd.DataFrame([{"contractSymbol": "TSLACALL", "bid": 1.0, "ask": 1.1}])
        self.puts = pd.DataFrame([{"contractSymbol": "TSLAPUT", "bid": 0.9, "ask": 1.0}])


class FakeTicker:  # pylint: disable=too-few-public-methods
    """Minimal yfinance ticker stand-in."""

    def __init__(self, ticker):
        self.ticker = ticker
        self.fast_info = {"lastPrice": 101.5, "previousClose": 100.0}
        self.info = {
            "regularMarketTime": "2026-03-23T13:30:00Z",
            "regularMarketPrice": 101.5,
            "previousClose": 100.0,
        }
        self.options = ("2026-04-17",)

    def option_chain(self, expiration_date):
        """Return a minimal option-chain payload."""
        assert expiration_date == "2026-04-17"
        return FakeChain()

    def history(self, **_kwargs):
        """Return enough daily bars for HV calculation."""
        return pd.DataFrame({"Close": [100.0, 101.0, 99.5, 102.0] * 30})


def test_yfinance_provider_can_dump_raw_payloads(monkeypatch, tmp_path: Path, capsys):
    """Shared provider debug mode should dump raw yfinance payloads to JSON."""
    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(
            debug_dump_provider_payload=True,
            debug_dump_dir=tmp_path,
        ),
    )
    monkeypatch.setattr("opx_chain.providers.base.get_runtime_config", lambda: make_runtime_config(
        debug_dump_provider_payload=True,
        debug_dump_dir=tmp_path,
    ))
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", FakeTicker)

    provider = YFinanceProvider()
    provider.load_underlying_snapshot("TSLA")
    expirations = provider.list_option_expirations("TSLA")
    chain = provider.load_option_chain("TSLA", expirations[0])

    assert not chain.calls.empty
    assert not chain.puts.empty
    dumped_files = sorted(tmp_path.glob("yfinance_TSLA_*.json"))
    assert len(dumped_files) == 3
    payloads = [json.loads(path.read_text(encoding="utf-8")) for path in dumped_files]
    labels = {payload["label"] for payload in payloads}
    assert labels == {"underlying_snapshot", "expirations", "option_chain_2026-04-17"}
    assert "yfinance debug: dumped underlying_snapshot payload to" in capsys.readouterr().out


def test_yfinance_snapshot_preserves_zero_last_price_instead_of_falling_back(monkeypatch):
    """A real zero price should remain zero instead of being replaced by previous-close fallback."""
    class ZeroPriceTicker:  # pylint: disable=too-few-public-methods
        """Minimal ticker stub that reports a zero last price."""

        def __init__(self, _ticker):
            self.fast_info = {"lastPrice": 0.0, "previousClose": 10.0}
            self.info = {
                "regularMarketTime": "2026-03-23T13:30:00Z",
                "regularMarketPrice": 11.0,
                "previousClose": 10.0,
            }

        def history(self, **_kwargs):
            """Return enough daily bars for HV calculation."""
            return pd.DataFrame({"Close": [100.0, 101.0, 99.5, 102.0] * 30})

    def fake_runtime_config():
        """Return a standard runtime config for the provider test."""
        return make_runtime_config()

    monkeypatch.setattr("opx_chain.providers.yfinance.get_runtime_config", fake_runtime_config)
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", ZeroPriceTicker)

    snapshot = YFinanceProvider().load_underlying_snapshot("TSLA")

    assert snapshot["underlying_price"] == 0.0


def test_yfinance_provider_load_ticker_events_parses_earnings_and_dividends(monkeypatch):
    """Yahoo event metadata should populate the canonical earnings and dividend fields."""
    class EventTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub with future earnings and dividend metadata."""

        def __init__(self, ticker):
            super().__init__(ticker)
            self.info.update(
                {
                    "earningsTimestampStart": 1777507200,  # 2026-04-29 UTC
                    "earningsTimestampEnd": 1777939200,    # 2026-05-04 UTC
                    "isEarningsDateEstimate": True,
                    "exDividendDate": 1776556800,          # 2026-04-18 UTC
                }
            )
            self.calendar = {
                "Earnings Date": [
                    pd.Timestamp("2026-04-29"),
                    pd.Timestamp("2026-05-04"),
                ],
                "Ex-Dividend Date": pd.Timestamp("2026-04-18"),
            }
            self.dividends = pd.Series(
                [0.88, 0.75],
                index=pd.to_datetime(["2026-04-18", "2026-07-18"]),
                dtype="float64",
            )

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 17)),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", EventTicker)

    events = YFinanceProvider().load_ticker_events("TSLA")

    assert events["next_earnings_date"] == "2026-04-29"
    assert events["next_earnings_date_is_estimated"] is True
    assert events["next_ex_div_date"] == "2026-04-18"
    assert events["dividend_amount"] == pytest.approx(0.88)


def test_yfinance_provider_load_ticker_events_returns_blanks_on_missing_data(monkeypatch):
    """Yahoo event loading should degrade to blank canonical fields when data is absent."""
    class BlankEventTicker(FakeTicker):  # pylint: disable=too-few-public-methods
        """Ticker stub with no future event metadata."""

        def __init__(self, ticker):
            super().__init__(ticker)
            self.info = {}
            self.calendar = None
            self.dividends = pd.Series(dtype="float64")

    monkeypatch.setattr(
        "opx_chain.providers.yfinance.get_runtime_config",
        lambda: make_runtime_config(today=date(2026, 4, 17)),
    )
    monkeypatch.setattr("opx_chain.providers.yfinance.yf.Ticker", BlankEventTicker)

    events = YFinanceProvider().load_ticker_events("TSLA")

    assert events["next_earnings_date"] is None
    assert events["next_earnings_date_is_estimated"] is None
    assert events["next_ex_div_date"] is None
    assert pd.isna(events["dividend_amount"])
