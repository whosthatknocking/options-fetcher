"""Cross-platform advisory file-lock helpers."""

from __future__ import annotations

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


def acquire_nonblocking_file_lock(path: Path) -> TextIO | None:
    """Acquire an exclusive non-blocking file lock, or return None if busy."""
    if fcntl is None and msvcrt is None:
        raise OSError("no file-lock implementation available")

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif msvcrt is not None:
            _ensure_lock_byte(handle)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        handle.close()
        return None
    return handle


def release_file_lock(handle: TextIO) -> None:
    """Release a file lock acquired by acquire_nonblocking_file_lock."""
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        elif msvcrt is not None:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    finally:
        handle.close()
