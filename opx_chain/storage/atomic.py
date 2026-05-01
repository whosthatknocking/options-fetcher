"""Atomic filesystem write helpers for storage artifacts."""

from __future__ import annotations

import errno
import os
import uuid
from pathlib import Path
from typing import Callable


def temporary_write_path(dest: Path) -> Path:
    """Return a same-directory temporary path for atomic replacement."""
    return dest.with_name(f".{dest.name}.{uuid.uuid4().hex}.tmp")


def _fsync_file(path: Path) -> None:
    """Flush file content and metadata for a fully-written temporary file."""
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_parent_dir(path: Path) -> None:
    """Best-effort fsync of the parent directory after atomic replacement."""
    if os.name == "nt":  # pragma: no cover - directory fsync is POSIX-specific
        return
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        fd = os.open(str(Path(path).parent), flags)
    except OSError:
        return
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if exc.errno not in {errno.EINVAL, errno.ENOTSUP, errno.ENOTDIR}:
                raise
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, content: bytes) -> None:
    """Write bytes through a same-directory temp file and atomically replace."""
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = temporary_write_path(dest)
    try:
        with tmp_path.open("wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, dest)
        _fsync_parent_dir(dest)
    finally:
        tmp_path.unlink(missing_ok=True)


def atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    """Write text through a same-directory temp file and atomically replace."""
    atomic_write_bytes(Path(path), content.encode(encoding))


def atomic_file_write(path: Path, writer: Callable[[Path], None]) -> int:
    """Run a writer against a temp path, replace dest, and return final bytes."""
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = temporary_write_path(dest)
    try:
        writer(tmp_path)
        _fsync_file(tmp_path)
        os.replace(tmp_path, dest)
        _fsync_parent_dir(dest)
    finally:
        tmp_path.unlink(missing_ok=True)
    return dest.stat().st_size
