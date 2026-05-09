"""Run-log tests covering shared logging between the app and yfinance."""

import ast
import logging
from pathlib import Path

import pytest

from conftest import make_runtime_config
from opx_chain.runlog import LOG_NAME, create_run_logger, get_logger, log_run_started

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "opx_chain"


class TrackingHandler(logging.Handler):
    """Test handler that records whether close was called."""

    def __init__(self):
        super().__init__()
        self.was_closed = False

    def emit(self, record):  # noqa: D401 - required logging.Handler hook
        """Discard records."""

    def close(self):
        self.was_closed = True
        super().close()


@pytest.fixture(autouse=True)
def restore_run_logger():
    """Reset the shared run logger so tests do not leak handlers or propagation."""
    loggers = [logging.getLogger("opx_chain.run"), logging.getLogger("yfinance")]
    original_state = {
        logger.name: (logger.level, logger.propagate, list(logger.handlers))
        for logger in loggers
    }
    yield
    for logger in loggers:
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()
        original_level, original_propagate, original_handlers = original_state[logger.name]
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


def test_get_logger_uses_canonical_opx_chain_namespace():
    """opx-chain package loggers should all live under one root namespace."""
    assert get_logger().name == LOG_NAME
    assert get_logger("fetch").name == "opx_chain.fetch"
    assert get_logger(".providers.marketdata.sdk.").name == "opx_chain.providers.marketdata.sdk"


def _logging_getlogger_call_lines(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    matches: list[int] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "logging"
            and node.func.attr == "getLogger"
        ):
            matches.append(node.lineno)
    return matches


def test_production_code_uses_runlog_get_logger_for_opx_loggers():
    """Only runlog.py should call logging.getLogger in production code."""
    assert PACKAGE_ROOT.exists()
    offenders: list[str] = []
    scanned_files = 0
    for path in PACKAGE_ROOT.rglob("*.py"):
        if path.name == "runlog.py":
            continue
        scanned_files += 1
        offenders.extend(
            f"{path.relative_to(PROJECT_ROOT)}:{line}"
            for line in _logging_getlogger_call_lines(path)
        )

    assert scanned_files > 0
    assert not offenders


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


def test_create_run_logger_closes_replaced_run_handlers(monkeypatch, tmp_path):
    """Replacing the shared run logger should close prior file descriptors."""
    _stub_runlog_dependencies(monkeypatch, tmp_path)
    logger = logging.getLogger("opx_chain.run")
    stale_handler = TrackingHandler()
    logger.addHandler(stale_handler)

    create_run_logger()

    assert stale_handler.was_closed
    assert stale_handler not in logger.handlers


def test_create_run_logger_preserves_external_handlers(monkeypatch, tmp_path):
    """Provider logger routing should only replace opx-chain-managed handlers."""
    _stub_runlog_dependencies(monkeypatch, tmp_path)
    external_logger = logging.getLogger("yfinance")
    external_handler = TrackingHandler()
    external_logger.addHandler(external_handler)

    first_logger, _ = create_run_logger()
    managed_handler = first_logger.handlers[0]
    assert external_handler in external_logger.handlers
    assert not external_handler.was_closed
    assert managed_handler in external_logger.handlers

    second_logger, _ = create_run_logger()

    assert external_handler in external_logger.handlers
    assert not external_handler.was_closed
    assert managed_handler not in external_logger.handlers
    assert second_logger.handlers[0] in external_logger.handlers
