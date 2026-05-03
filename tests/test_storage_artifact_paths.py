"""Storage artifact path containment tests."""
# pylint: disable=duplicate-code

from collections.abc import Callable
from pathlib import Path

import pytest

from opx_chain.storage._disk import write_artifact_bytes
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import ArtifactWrite, RunContext
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend


def _make_context(**kwargs) -> RunContext:
    defaults = {
        "provider": "yfinance",
        "tickers": ("TSLA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **kwargs})


def _filesystem_backend(tmp_path: Path):
    return FilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
    )


def _sqlite_backend(tmp_path: Path):
    return SqliteIndexedBackend(
        db_path=tmp_path / "opx-chain.db",
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
    )


@pytest.mark.parametrize(
    "filename",
    ["", ".", "..", "../escape.txt", "nested/file.txt", "/tmp/escape.txt", "nested\\file.txt"],
)
def test_write_artifact_bytes_rejects_unsafe_filename(tmp_path: Path, filename: str):
    """Debug artifact filenames must be single path components."""
    with pytest.raises(ValueError, match="invalid filename"):
        write_artifact_bytes(b"x", tmp_path / "debug", filename)


@pytest.mark.parametrize("backend_factory", [_filesystem_backend, _sqlite_backend])
@pytest.mark.parametrize("filename", ["../escape.txt", "nested/file.txt", "/tmp/escape.txt"])
def test_sidecar_artifacts_reject_unsafe_filename(
    tmp_path: Path,
    backend_factory: Callable[[Path], object],
    filename: str,
):
    """Sidecar artifact filenames must not escape their run directory."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())

    with pytest.raises(ValueError, match="invalid path component"):
        backend.write_artifact(
            run_id,
            ArtifactWrite(artifact_type="sidecar", content=b"x", filename=filename),
        )


@pytest.mark.parametrize("backend_factory", [_filesystem_backend, _sqlite_backend])
def test_sidecar_artifacts_reject_unsafe_run_id(
    tmp_path: Path,
    backend_factory: Callable[[Path], object],
):
    """Sidecar writes must validate the run-id path component before writing."""
    backend = backend_factory(tmp_path)

    with pytest.raises(ValueError, match="invalid path component"):
        backend.write_artifact(
            "../escape",
            ArtifactWrite(artifact_type="sidecar", content=b"x", filename="positions.csv"),
        )

    assert not (tmp_path / "escape" / "positions.csv").exists()
