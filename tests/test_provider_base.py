"""Tests for shared provider base behavior."""

import json
from pathlib import Path

import pandas as pd
import pytest

from conftest import BoundaryTickDateTime, make_runtime_config
from opx_chain.providers.base import (
    DataProvider,
    OptionChainFrames,
    RequestThrottle,
    compute_backoff_delay,
    is_provider_quota_error,
)
from opx_chain.storage.atomic import atomic_write_text


class MinimalProvider(DataProvider):
    """Concrete provider used to exercise base-class helpers."""

    name = "minimal"

    def load_underlying_snapshot(self, ticker: str) -> dict:  # pylint: disable=unused-argument
        return {"last": 100.0}

    def list_option_expirations(self, ticker: str) -> list[str]:  # pylint: disable=unused-argument
        return ["2026-05-15"]

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        del ticker, expiration_date
        return OptionChainFrames(calls=pd.DataFrame(), puts=pd.DataFrame())

    # The base provider interface requires these canonical normalization inputs.
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def normalize_option_frame(
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        del underlying_price, expiration_date, option_type, ticker
        return df


def test_debug_dump_payload_uses_atomic_text_writer(monkeypatch, tmp_path: Path) -> None:
    """Provider diagnostic dumps should follow the shared atomic-write discipline."""
    monkeypatch.setattr(
        "opx_chain.providers.base.get_runtime_config",
        lambda: make_runtime_config(
            debug_dump_provider_payload=True,
            debug_dump_dir=tmp_path,
        ),
    )
    calls: list[Path] = []

    def spy_atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
        calls.append(Path(path))
        atomic_write_text(path, content, encoding=encoding)

    monkeypatch.setattr("opx_chain.providers.base.atomic_write_text", spy_atomic_write_text)

    dump_path = MinimalProvider().debug_dump_payload("tsla", "snapshot", {"price": 123.45})

    assert dump_path is not None
    assert calls == [dump_path]
    assert dump_path.exists()
    assert not list(tmp_path.glob(".*.tmp"))
    assert '"ticker": "TSLA"' in dump_path.read_text(encoding="utf-8")


def test_debug_dump_payload_reuses_timestamp_for_filename_and_payload(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """The debug dump filename timestamp and fetched_at field must stay paired."""
    monkeypatch.setattr(
        "opx_chain.providers.base.get_runtime_config",
        lambda: make_runtime_config(
            debug_dump_provider_payload=True,
            debug_dump_dir=tmp_path,
        ),
    )

    BoundaryTickDateTime.reset()
    monkeypatch.setattr("opx_chain.providers.base.datetime", BoundaryTickDateTime)

    dump_path = MinimalProvider().debug_dump_payload("tsla", "snapshot", {"price": 123.45})

    assert dump_path is not None
    payload = json.loads(dump_path.read_text(encoding="utf-8"))
    assert BoundaryTickDateTime.calls == 1
    assert dump_path.name == "minimal_TSLA_snapshot_20260509_055959.json"
    assert payload["fetched_at"] == "2026-05-09T05:59:59Z"


def test_debug_dump_payload_sanitizes_non_finite_values(monkeypatch, tmp_path: Path) -> None:
    """Provider debug dumps should stay strict-JSON compatible."""
    monkeypatch.setattr(
        "opx_chain.providers.base.get_runtime_config",
        lambda: make_runtime_config(
            debug_dump_provider_payload=True,
            debug_dump_dir=tmp_path,
        ),
    )

    dump_path = MinimalProvider().debug_dump_payload(
        "tsla",
        "snapshot",
        {"price": float("inf"), "nested": [float("-inf")]},
    )

    assert dump_path is not None
    text = dump_path.read_text(encoding="utf-8")
    assert "Infinity" not in text
    payload = json.loads(text)
    assert payload["payload"] == {"price": None, "nested": [None]}


def test_provider_quota_classifier_matches_provider_rate_limits() -> None:
    """Provider quota/rate-limit wording should still classify as terminal quota."""
    assert is_provider_quota_error(RuntimeError("HTTP 429 Too Many Requests"))
    assert is_provider_quota_error(RuntimeError("daily request limit reached"))
    assert is_provider_quota_error(RuntimeError("api quota exhausted"))
    assert is_provider_quota_error(RuntimeError("quota/rate limit from provider"))


def test_provider_quota_classifier_ignores_local_quota_errors() -> None:
    """Local resource-quota failures should not masquerade as provider quota errors."""
    assert not is_provider_quota_error(OSError(122, "Disk quota exceeded"))
    assert not is_provider_quota_error(RuntimeError("memory quota exceeded"))
    assert not is_provider_quota_error(RuntimeError("time quota expired"))


def test_compute_backoff_delay_applies_jitter_and_final_cap(monkeypatch) -> None:
    """Shared retry backoff should jitter delays without exceeding the max cap."""
    monkeypatch.setattr("opx_chain.providers.base.random.uniform", lambda _low, _high: 1.3)

    assert compute_backoff_delay(1, 2.0) == pytest.approx(5.2)
    assert compute_backoff_delay(10, 2.0, max_seconds=60.0) == pytest.approx(60.0)


def test_compute_backoff_delay_ignores_invalid_inputs() -> None:
    """Non-finite or invalid retry config should never create invalid sleep values."""
    assert compute_backoff_delay(1, float("nan")) == 0.0
    assert compute_backoff_delay(1, float("inf")) == 0.0
    assert compute_backoff_delay(1, "bad") == 0.0
    assert compute_backoff_delay(1, -1.0) == 0.0


def test_request_throttle_serializes_start_times(monkeypatch) -> None:
    """Request pacing should use one shared lock-protected start-time boundary."""
    monotonic_values = iter([10.0, 10.25, 11.0])
    sleeps: list[float] = []
    monkeypatch.setattr(
        "opx_chain.providers.base.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("opx_chain.providers.base.time.sleep", sleeps.append)

    throttle = RequestThrottle()
    throttle.wait(1.0)
    throttle.wait(1.0)

    assert sleeps == [pytest.approx(0.75)]
