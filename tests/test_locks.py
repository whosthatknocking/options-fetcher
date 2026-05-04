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


def test_blocking_file_lock_raises_without_platform_backend(tmp_path, monkeypatch):
    """Blocking locks use the same platform support check as non-blocking locks."""
    monkeypatch.setattr(locks, "fcntl", None)
    monkeypatch.setattr(locks, "msvcrt", None)

    lock_path = tmp_path / "fetcher.lock"
    with pytest.raises(OSError, match="no file-lock implementation available"):
        locks.acquire_blocking_file_lock(lock_path)

    assert not lock_path.exists()


def test_blocking_file_lock_acquires_and_releases(tmp_path):
    """Blocking locks should create the lock file and release cleanly."""
    lock_path = tmp_path / "fetcher.lock"
    handle = locks.acquire_blocking_file_lock(lock_path)
    assert lock_path.exists()

    locks.release_file_lock(handle)

    second = locks.acquire_nonblocking_file_lock(lock_path)
    assert second is not None
    locks.release_file_lock(second)
