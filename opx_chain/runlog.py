"""Run-log configuration for fetcher execution and vendor error capture."""

import logging
import time
from datetime import datetime, timezone

from opx_chain.config import SCRIPT_VERSION, get_runtime_config
from opx_chain.paths import get_state_dir
from opx_chain.providers import get_data_provider
from opx_chain.storage.factory import get_data_dir


def configure_external_loggers(file_handler):
    """Route configured provider-library errors into the same append-only run log."""
    provider = get_data_provider()
    for logger_name in provider.external_logger_names:
        provider_logger = logging.getLogger(logger_name)
        provider_logger.setLevel(logging.ERROR)
        provider_logger.handlers.clear()
        provider_logger.propagate = False
        provider_logger.addHandler(file_handler)


def _migrate_legacy_shared_log(src, dst):
    """Relocate the pre-XDG-state shared log without blocking a fetch run."""
    if dst.exists() or not src.exists():
        return
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dst)
        src.parent.rmdir()
    except OSError:
        pass


def create_run_logger():
    """Create the append-only run logger and return it with its file path."""
    logs_dir = get_state_dir() / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "opx_runs.log"
    _migrate_legacy_shared_log(get_data_dir() / "logs" / "opx_runs.log", log_path)

    logger = logging.getLogger("opx_chain.run")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)sZ | %(levelname)s | %(message)s")
    formatter.converter = time.gmtime
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    configure_external_loggers(file_handler)

    logger.info("=" * 80)
    return logger, log_path


def log_run_started(logger, run_id: str | None = None, config=None) -> str:
    """Write the canonical run-start line and return the emitted run identifier."""
    if config is None:
        config = get_runtime_config()
    resolved_run_id = run_id or datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(
        "run_started run_id=%s script_version=%s provider=%s config_path=%s",
        resolved_run_id,
        SCRIPT_VERSION,
        config.data_provider,
        config.config_path,
    )
    return resolved_run_id
