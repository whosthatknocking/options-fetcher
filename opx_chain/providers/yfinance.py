"""Yahoo Finance provider implementation."""

# pylint: disable=duplicate-code

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

from opx_chain.config import US_MARKET_TIMEZONE, get_runtime_config
from opx_chain.providers.base import DataProvider, OptionChainFrames, normalize_provider_frame
from opx_chain.utils import coerce_float, normalize_timestamp


def _first_non_missing(*values):
    """Return the first value that is not None/NaN, preserving legitimate zeroes."""
    for value in values:
        if value is not None and not pd.isna(value):
            return value
    return None


def _parse_event_date(raw_date) -> date | None:
    """Convert Yahoo event date values into U.S. market-calendar dates."""
    if raw_date is None:
        return None
    parsed_date = None
    try:
        if pd.isna(raw_date):
            return None
        if isinstance(raw_date, (int, float, np.integer, np.floating)):
            parsed_date = datetime.fromtimestamp(
                float(raw_date),
                tz=timezone.utc,
            ).astimezone(US_MARKET_TIMEZONE).date()
        elif isinstance(raw_date, str):
            parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
        elif isinstance(raw_date, datetime):
            if raw_date.tzinfo is None:
                parsed_date = raw_date.date()
            else:
                parsed_date = raw_date.astimezone(US_MARKET_TIMEZONE).date()
        elif isinstance(raw_date, date):
            parsed_date = raw_date
    except (ValueError, TypeError, OSError):
        pass
    return parsed_date


def _flatten_calendar_values(value: Any) -> list[Any]:  # pylint: disable=too-many-return-statements
    """Flatten yfinance calendar payload shapes into a scalar list."""
    if value is None:
        return []
    if isinstance(value, pd.DataFrame):
        return _flatten_calendar_values(value.to_numpy().ravel().tolist())
    if isinstance(value, pd.Series):
        return _flatten_calendar_values(value.tolist())
    if isinstance(value, pd.Index):
        return _flatten_calendar_values(value.tolist())
    if isinstance(value, np.ndarray):
        return _flatten_calendar_values(value.tolist())
    if isinstance(value, (list, tuple, set)):
        flattened: list[Any] = []
        for item in value:
            flattened.extend(_flatten_calendar_values(item))
        return flattened
    return [value]


def _extract_calendar_field(  # pylint: disable=too-many-return-statements
    calendar_payload: Any,
    field_name: str,
) -> list[Any]:
    """Extract one named field from a yfinance calendar payload."""
    if calendar_payload is None:
        return []
    if isinstance(calendar_payload, dict):
        if field_name in calendar_payload:
            return _flatten_calendar_values(calendar_payload[field_name])
        return []
    if isinstance(calendar_payload, pd.DataFrame):
        if field_name in calendar_payload.columns:
            return _flatten_calendar_values(calendar_payload[field_name])
        if field_name in calendar_payload.index:
            return _flatten_calendar_values(calendar_payload.loc[field_name])
        return []
    if isinstance(calendar_payload, pd.Series):
        if field_name in calendar_payload.index:
            return _flatten_calendar_values(calendar_payload.loc[field_name])
        return []
    return []


def _pick_next_future_date(raw_values: list[Any], today: date) -> date | None:
    """Return the earliest date on or after today from a list of raw Yahoo values."""
    upcoming = sorted(
        parsed
        for raw_value in raw_values
        if (parsed := _parse_event_date(raw_value)) is not None and parsed >= today
    )
    return upcoming[0] if upcoming else None


def compute_historical_volatility(stock):  # pylint: disable=broad-exception-caught
    """Compute trailing annualized realized volatility from daily closes."""
    config = get_runtime_config()
    lookback_period = f"{max(config.hv_lookback_days * 3, 90)}d"
    try:
        history = stock.history(period=lookback_period, interval="1d", auto_adjust=False)
    except Exception:  # pylint: disable=broad-exception-caught
        return np.nan
    if history.empty:
        return np.nan

    close_column = "Adj Close" if "Adj Close" in history.columns else "Close"
    closes = pd.to_numeric(history[close_column], errors="coerce").dropna()
    log_returns = np.log(closes / closes.shift(1)).dropna()
    if len(log_returns) < config.hv_lookback_days:
        return np.nan

    recent_returns = log_returns.tail(config.hv_lookback_days)
    return recent_returns.std(ddof=1) * np.sqrt(config.trading_days_per_year)


class YFinanceProvider(DataProvider):
    """Market-data provider backed by yfinance/Yahoo Finance."""

    name = "yfinance"

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose yfinance's logger so runlog can capture vendor errors."""
        return ("yfinance",)

    @staticmethod
    def _safe_info(stock) -> dict[str, Any]:
        """Return the yfinance info payload or an empty dict on failure."""
        try:
            info = stock.info
        except Exception:  # pylint: disable=broad-exception-caught
            return {}
        return info if isinstance(info, dict) else {}

    @staticmethod
    def _safe_calendar(stock):
        """Return the yfinance calendar payload or None on failure."""
        try:
            return stock.calendar
        except Exception:  # pylint: disable=broad-exception-caught
            return None

    @staticmethod
    def _safe_dividends(stock) -> pd.Series:
        """Return the yfinance dividends series or an empty series on failure."""
        try:
            dividends = stock.dividends
        except Exception:  # pylint: disable=broad-exception-caught
            return pd.Series(dtype="float64")
        return dividends if isinstance(dividends, pd.Series) else pd.Series(dtype="float64")

    @staticmethod
    def _next_earnings_event(info: dict[str, Any], calendar_payload, today: date):
        """Return the next Yahoo earnings date and estimate flag when available."""
        info_dates = [
            _parse_event_date(info.get("earningsTimestampStart")),
            _parse_event_date(info.get("earningsTimestamp")),
            _parse_event_date(info.get("earningsTimestampEnd")),
        ]
        info_upcoming = sorted(d for d in info_dates if d is not None and d >= today)
        calendar_upcoming = _pick_next_future_date(
            _extract_calendar_field(calendar_payload, "Earnings Date"),
            today,
        )
        candidates = info_upcoming.copy()
        if calendar_upcoming is not None:
            candidates.append(calendar_upcoming)
        if not candidates:
            return None, None
        next_date = min(candidates)
        estimate_flag = None
        estimate_value = info.get("isEarningsDateEstimate")
        if next_date in info_upcoming and isinstance(estimate_value, bool):
            estimate_flag = estimate_value
        return next_date.isoformat(), estimate_flag

    @staticmethod
    def _next_dividend_event(
        info: dict[str, Any],
        calendar_payload,
        dividends: pd.Series,
        today: date,
    ):
        """Return the next Yahoo ex-dividend date and associated amount when available."""
        future_dividends: dict[date, float] = {}
        if not dividends.empty:
            for raw_date, raw_amount in dividends.items():
                ex_div_date = _parse_event_date(raw_date)
                if ex_div_date is None or ex_div_date < today:
                    continue
                amount = coerce_float(raw_amount)
                future_dividends[ex_div_date] = np.nan if pd.isna(amount) else float(amount)

        candidates: list[date] = list(future_dividends)
        info_ex_div_date = _parse_event_date(info.get("exDividendDate"))
        if info_ex_div_date is not None and info_ex_div_date >= today:
            candidates.append(info_ex_div_date)
        calendar_ex_div_date = _pick_next_future_date(
            _extract_calendar_field(calendar_payload, "Ex-Dividend Date"),
            today,
        )
        if calendar_ex_div_date is not None:
            candidates.append(calendar_ex_div_date)
        if not candidates:
            return None, np.nan
        next_date = min(candidates)
        return next_date.isoformat(), future_dividends.get(next_date, np.nan)

    def load_underlying_snapshot(self, ticker: str) -> dict:  # pylint: disable=broad-exception-caught
        """Load the underlying snapshot once per ticker and reuse it for each expiration."""
        stock = yf.Ticker(ticker)
        fast_info = getattr(stock, "fast_info", {}) or {}
        info = self._safe_info(stock)
        self.debug_dump_payload(
            ticker,
            "underlying_snapshot",
            {"fast_info": fast_info, "info": info},
        )

        last_price = coerce_float(
            _first_non_missing(
                fast_info.get("lastPrice"),
                info.get("regularMarketPrice"),
                info.get("previousClose"),
            )
        )
        previous_close = coerce_float(
            _first_non_missing(
                fast_info.get("previousClose"),
                info.get("previousClose"),
            )
        )

        if pd.notna(last_price) and pd.notna(previous_close) and previous_close > 0:
            underlying_day_change_pct = (last_price - previous_close) / previous_close
        else:
            underlying_day_change_pct = np.nan

        return {
            "underlying_price": last_price,
            "underlying_price_time": normalize_timestamp(info.get("regularMarketTime")),
            "underlying_day_change_pct": underlying_day_change_pct,
            "historical_volatility": compute_historical_volatility(stock),
        }

    def load_ticker_events(self, ticker: str) -> dict:
        """Load best-effort earnings and dividend events from Yahoo metadata."""
        stock = yf.Ticker(ticker)
        info = self._safe_info(stock)
        calendar_payload = self._safe_calendar(stock)
        dividends = self._safe_dividends(stock)
        self.debug_dump_payload(
            ticker,
            "ticker_events",
            {
                "info": info,
                "calendar": calendar_payload,
                "dividends": dividends,
            },
        )
        today = get_runtime_config().today
        next_earnings_date, is_estimated = self._next_earnings_event(info, calendar_payload, today)
        next_ex_div_date, dividend_amount = self._next_dividend_event(
            info,
            calendar_payload,
            dividends,
            today,
        )
        return {
            "next_earnings_date": next_earnings_date,
            "next_earnings_date_is_estimated": is_estimated,
            "next_ex_div_date": next_ex_div_date,
            "dividend_amount": dividend_amount,
        }

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return option expiration strings available from yfinance."""
        stock = yf.Ticker(ticker)
        expirations = list(stock.options)
        self.debug_dump_payload(ticker, "expirations", expirations)
        return expirations

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Load one yfinance option chain and return its raw call/put frames."""
        stock = yf.Ticker(ticker)
        chain = stock.option_chain(expiration_date)
        self.debug_dump_payload(
            ticker,
            f"option_chain_{expiration_date}",
            {"calls": chain.calls, "puts": chain.puts},
        )
        return OptionChainFrames(calls=chain.calls, puts=chain.puts)

    def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Normalize a yfinance frame into the canonical options schema."""
        return normalize_provider_frame(
            df=df,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )
