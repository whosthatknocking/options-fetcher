"""Cross-backend missing-run contract tests."""

from collections.abc import Callable
from pathlib import Path

import pytest

from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.memory import MemoryBackend
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend

BackendFactory = Callable[[Path], object]


def _filesystem_backend(tmp_path: Path) -> FilesystemBackend:
    return FilesystemBackend(
        runs_dir=tmp_path / "filesystem-runs",
        debug_dir=tmp_path / "filesystem-debug",
    )


def _memory_backend(_tmp_path: Path) -> MemoryBackend:
    return MemoryBackend()


def _sqlite_backend(tmp_path: Path) -> SqliteIndexedBackend:
    return SqliteIndexedBackend(
        db_path=tmp_path / "opx-chain.db",
        runs_dir=tmp_path / "sqlite-runs",
        debug_dir=tmp_path / "sqlite-debug",
    )


BACKENDS: tuple[BackendFactory, ...] = (
    _filesystem_backend,
    _memory_backend,
    _sqlite_backend,
)


def _close_backend(backend: object) -> None:
    close = getattr(backend, "close", None)
    if close is not None:
        close()


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_get_run_raises_key_error_for_unknown_run(
    backend_factory: BackendFactory,
    tmp_path: Path,
):
    """get_run must raise the same error type for unknown run IDs."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(KeyError, match="run not found"):
            backend.get_run("missing-run")
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_get_ticker_results_raises_key_error_for_unknown_run(
    backend_factory: BackendFactory,
    tmp_path: Path,
):
    """get_ticker_results must match get_run semantics for unknown run IDs."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(KeyError, match="run not found"):
            backend.get_ticker_results("missing-run")
    finally:
        _close_backend(backend)
