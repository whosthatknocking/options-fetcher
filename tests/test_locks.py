"""Tests for cross-platform advisory lock helpers."""

import pytest

from opx_chain import locks


def test_nonblocking_file_lock_raises_without_platform_backend(tmp_path, monkeypatch):
    """Missing lock support must surface as a platform error, not lock contention."""
    monkeypatch.setattr(locks, "fcntl", None)
    monkeypatch.setattr(locks, "msvcrt", None)

    lock_path = tmp_path / "fetcher.lock"
    with pytest.raises(OSError, match="no file-lock implementation available"):
        locks.acquire_nonblocking_file_lock(lock_path)

    assert not lock_path.exists()
