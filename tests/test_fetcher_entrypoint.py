"""Tests for the repo-root fetcher compatibility entrypoint."""

from pathlib import Path
import runpy

import pytest

import opx_chain.fetcher as packaged_fetcher


FETCHER_ENTRYPOINT = Path(__file__).resolve().parent.parent / "fetcher.py"


def test_root_fetcher_entrypoint_delegates_to_packaged_main(monkeypatch):
    """Running fetcher.py as __main__ should delegate to opx_chain.fetcher.main."""
    captured = {}

    def stub_main():
        captured["called"] = True
        return 17

    monkeypatch.setattr(packaged_fetcher, "main", stub_main)

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(str(FETCHER_ENTRYPOINT), run_name="__main__")

    assert captured == {"called": True}
    assert exc_info.value.code == 17
