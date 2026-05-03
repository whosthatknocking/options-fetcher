"""Provider abstractions for loading market data from different vendors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from opx_chain.config import get_runtime_config
from opx_chain.normalize import normalize_vendor_option_frame
from opx_chain.storage.atomic import atomic_write_text


class ProviderAuthenticationError(RuntimeError):
    """Raised when provider authentication fails and the run should stop clearly."""


class ProviderQuotaError(RuntimeError):
    """Raised when the provider rejects the request due to a quota or rate limit."""


def is_provider_quota_error(exc: Exception) -> bool:
    """Return True when a provider exception represents quota/rate-limit exhaustion."""
    status_code = (
        getattr(exc, "status_code", None)
        or getattr(exc, "status", None)
        or getattr(getattr(exc, "response", None), "status_code", None)
        or getattr(getattr(exc, "response", None), "status", None)
    )
    if str(status_code) == "429":
        return True
    message = str(exc).lower()
    quota_patterns = (
        r"\bapi\s+quota\b",
        r"\bprovider\s+quota\b",
        r"\brequest\s+quota\b",
        r"\bquota\s*/\s*rate\s*limit\b",
    )
    if any(re.search(pattern, message) for pattern in quota_patterns):
        return True
    return any(
        token in message
        for token in (
            "429",
            "rate limit",
            "rate-limit",
            "rate limited",
            "request limit",
            "too many requests",
        )
    )


@dataclass(frozen=True)
class OptionChainFrames:
    """Vendor option-chain payload split into calls and puts."""

    calls: pd.DataFrame
    puts: pd.DataFrame


def _to_json_ready(value):  # pylint: disable=too-many-return-statements
    """Convert provider payloads into JSON-serializable structures."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _to_json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json_ready(item) for item in value]
    if isinstance(value, pd.DataFrame):
        records = value.where(pd.notna(value), None).to_dict(orient="records")
        return [_to_json_ready(record) for record in records]
    if isinstance(value, pd.Series):
        return _to_json_ready(value.where(pd.notna(value), None).to_dict())
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass
    if hasattr(value, "__dict__"):
        return {
            key: _to_json_ready(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return str(value)


class DataProvider(ABC):
    """Abstract market-data provider used by the fetch pipeline."""

    name: str

    def prepare_ticker_fetch(self, ticker: str) -> None:  # pylint: disable=unused-argument
        """Reset provider-local per-ticker state before a new fetch pipeline call."""

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Logger names used by vendor libraries that should be routed to the run log."""
        return ()

    def debug_dump_payload(self, ticker: str, label: str, payload) -> Path | None:
        """Write a raw provider payload dump when shared debug mode is enabled."""
        config = get_runtime_config()
        if not config.debug_dump_provider_payload:
            return None
        dump_dir = Path(config.debug_dump_dir)
        dump_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_label = label.replace(" ", "_")
        dump_path = dump_dir / f"{self.name}_{ticker.upper()}_{safe_label}_{timestamp}.json"
        debug_payload = {
            "provider": self.name,
            "ticker": ticker.upper(),
            "label": label,
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "payload": _to_json_ready(payload),
        }
        atomic_write_text(
            dump_path,
            json.dumps(debug_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print(f"{self.name} debug: dumped {label} payload to {dump_path}")
        return dump_path

    def load_ticker_events(self, ticker: str) -> dict:  # pylint: disable=unused-argument
        """Return corporate event data for a ticker. Override for providers that support it."""
        return {
            "next_earnings_date": None,
            "next_earnings_date_is_estimated": None,
            "next_ex_div_date": None,
            "dividend_amount": np.nan,
        }

    @abstractmethod
    def load_underlying_snapshot(self, ticker: str) -> dict:
        """Load the current underlying snapshot for one ticker."""

    @abstractmethod
    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return available option expiration strings for a ticker."""

    @abstractmethod
    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Load the raw option chain for one ticker and expiration."""

    @abstractmethod
    # The provider contract needs these canonical normalization inputs.
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def normalize_option_frame(
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Map one vendor-specific option frame into the canonical schema."""


# pylint: disable=too-many-arguments
def normalize_provider_frame(
    *,
    df: pd.DataFrame,
    underlying_price: float,
    expiration_date: str,
    option_type: str,
    ticker: str,
    data_source: str,
) -> pd.DataFrame:
    """Apply the shared canonical vendor normalization for one provider frame."""
    return normalize_vendor_option_frame(
        df=df,
        underlying_price=underlying_price,
        expiration_date=expiration_date,
        option_type=option_type,
        ticker=ticker,
        data_source=data_source,
    )
