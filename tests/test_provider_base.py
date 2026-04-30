"""Tests for shared provider base behavior."""

from pathlib import Path

import pandas as pd

from conftest import make_runtime_config
from opx_chain.providers.base import DataProvider, OptionChainFrames
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
