"""Tests for cross-platform advisory lock helpers."""

import errno
from types import SimpleNamespace

import pytest

from opx_chain import locks


def _fake_fcntl_raising(exc: OSError) -> SimpleNamespace:
    """Return a minimal fcntl-like object whose flock raises exc."""

    def flock(_fileno, _flags):
        """Raise the configured lock error."""
        raise exc

    return SimpleNamespace(LOCK_EX=1, LOCK_NB=2, LOCK_UN=3, flock=flock)


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


def test_nonblocking_file_lock_returns_none_for_busy_errno(tmp_path, monkeypatch):
    """Non-blocking lock contention returns None."""
    monkeypatch.setattr(locks, "fcntl", _fake_fcntl_raising(OSError(errno.EAGAIN, "busy")))
    monkeypatch.setattr(locks, "msvcrt", None)

    lock_path = tmp_path / "fetcher.lock"

    assert locks.acquire_nonblocking_file_lock(lock_path) is None
    assert lock_path.exists()


def test_nonblocking_file_lock_reraises_unexpected_oserror(tmp_path, monkeypatch):
    """Unexpected lock failures must not be misreported as contention."""
    monkeypatch.setattr(
        locks,
        "fcntl",
        _fake_fcntl_raising(OSError(errno.EINVAL, "invalid lock")),
    )
    monkeypatch.setattr(locks, "msvcrt", None)

    lock_path = tmp_path / "fetcher.lock"

    with pytest.raises(OSError, match="invalid lock"):
        locks.acquire_nonblocking_file_lock(lock_path)
