"""Tests for storage backend factory wiring."""

import threading
import time
from pathlib import Path

import pytest

from conftest import make_runtime_config
import opx_chain.storage.factory as factory_mod
from opx_chain.storage.filesystem import FilesystemBackend


def test_factory_constructs_one_filesystem_backend_under_concurrency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Concurrent callers must share the first cached backend instance."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )
    constructed = 0
    count_lock = threading.Lock()

    class SlowFilesystemBackend(FilesystemBackend):
        """Filesystem backend with a widened construction race window."""

        def __init__(self, *args, **kwargs):
            nonlocal constructed
            with count_lock:
                constructed += 1
            time.sleep(0.02)
            super().__init__(*args, **kwargs)

    factory_mod.clear_storage_backend_cache()
    monkeypatch.setattr(factory_mod, "FilesystemBackend", SlowFilesystemBackend)
    results = []

    def call_factory() -> None:
        results.append(factory_mod.get_storage_backend(config))

    threads = [threading.Thread(target=call_factory) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == len(threads)
    assert len({id(result) for result in results}) == 1
    assert constructed == 1
