"""Run-log tests covering shared logging between the app and yfinance."""

import logging
from pathlib import Path

import pytest

from conftest import make_runtime_config
from opx_chain.runlog import create_run_logger, log_run_started


@pytest.fixture(autouse=True)
def restore_run_logger():
    """Reset the shared run logger so tests do not leak handlers or propagation."""
    logger = logging.getLogger("opx_chain.run")
    original_level = logger.level
    original_propagate = logger.propagate
    original_handlers = list(logger.handlers)
    yield
    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()
    logger.handlers.extend(original_handlers)
    logger.setLevel(original_level)
    logger.propagate = original_propagate


def _stub_runlog_dependencies(monkeypatch, tmp_path):
    """Route run-log dependencies to a temp runtime root."""
    monkeypatch.setattr("opx_chain.runlog.get_state_dir", lambda: tmp_path / "state")
    monkeypatch.setattr("opx_chain.runlog.get_data_dir", lambda: tmp_path / "data")
    monkeypatch.setattr(
        "opx_chain.runlog.get_runtime_config",
        lambda: make_runtime_config(
            data_provider="yfinance",
            config_path=Path("/tmp/opx-test.toml"),
        ),
    )

    def stub_provider():
        """Return a provider stub exposing yfinance logger routing."""
        return type("StubProvider", (), {"external_logger_names": ("yfinance",)})()

    monkeypatch.setattr(
        "opx_chain.runlog.get_data_provider",
        stub_provider,
    )


def test_create_run_logger_routes_yfinance_errors_to_run_log(monkeypatch, tmp_path):
    """yfinance errors should be written into the shared run log file."""
    _stub_runlog_dependencies(monkeypatch, tmp_path)

    logger, log_path = create_run_logger()
    log_run_started(logger, run_id="storage-run-123")
    logging.getLogger("yfinance").error("remote request failed for TSLA")

    for handler in logger.handlers:
        handler.flush()

    assert logger.name == "opx_chain.run"
    contents = log_path.read_text(encoding="utf-8")
    assert "run_started run_id=storage-run-123" in contents
    assert "remote request failed for TSLA" in contents
    assert log_path.name == "opx_runs.log"
    assert log_path == tmp_path / "state" / "logs" / "opx_runs.log"


def test_create_run_logger_migrates_legacy_data_log(monkeypatch, tmp_path):
    """Existing data-dir logs should move to the XDG state log path once."""
    _stub_runlog_dependencies(monkeypatch, tmp_path)
    legacy_log = tmp_path / "data" / "logs" / "opx_runs.log"
    legacy_log.parent.mkdir(parents=True)
    legacy_log.write_text("legacy entry\n", encoding="utf-8")

    logger, log_path = create_run_logger()
    log_run_started(logger, run_id="storage-run-456")
    for handler in logger.handlers:
        handler.flush()

    assert log_path == tmp_path / "state" / "logs" / "opx_runs.log"
    assert not legacy_log.exists()
    contents = log_path.read_text(encoding="utf-8")
    assert "legacy entry" in contents
    assert "run_started run_id=storage-run-456" in contents
