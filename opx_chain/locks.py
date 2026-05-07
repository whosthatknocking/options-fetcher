"""Cross-platform advisory file-lock helpers."""

from __future__ import annotations

import errno
from pathlib import Path
from typing import TextIO

try:  # pragma: no cover - exercised on POSIX hosts.
    import fcntl
except ImportError:  # pragma: no cover - exercised on Windows hosts.
    fcntl = None

try:  # pragma: no cover - exercised on Windows hosts.
    import msvcrt
except ImportError:  # pragma: no cover - exercised on POSIX hosts.
    msvcrt = None


def _ensure_lock_byte(handle: TextIO) -> None:
    """Ensure byte-range locking has at least one byte to lock."""
    handle.seek(0, 2)
    if handle.tell() == 0:
        handle.write("\0")
        handle.flush()
    handle.seek(0)


def _ensure_lock_implementation() -> None:
    if fcntl is None and msvcrt is None:
        raise OSError("no file-lock implementation available")


_LOCK_BUSY_ERRNOS = {errno.EACCES, errno.EAGAIN}
if hasattr(errno, "EWOULDBLOCK"):
    _LOCK_BUSY_ERRNOS.add(errno.EWOULDBLOCK)


def _acquire_file_lock(path: Path, *, blocking: bool) -> TextIO | None:
    _ensure_lock_implementation()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        if fcntl is not None:
            flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
            fcntl.flock(handle.fileno(), flags)
        else:
            _ensure_lock_byte(handle)
            lock_mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
            msvcrt.locking(handle.fileno(), lock_mode, 1)
    except OSError as exc:
        handle.close()
        if not blocking and exc.errno in _LOCK_BUSY_ERRNOS:
            return None
        raise
    return handle


def acquire_nonblocking_file_lock(path: Path) -> TextIO | None:
    """Acquire an exclusive non-blocking file lock, or return None if busy."""
    return _acquire_file_lock(path, blocking=False)


def acquire_blocking_file_lock(path: Path) -> TextIO:
    """Acquire an exclusive blocking file lock."""
    handle = _acquire_file_lock(path, blocking=True)
    assert handle is not None
    return handle


def release_file_lock(handle: TextIO) -> None:
    """Release a file lock acquired by this module."""
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        elif msvcrt is not None:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    finally:
        handle.close()
