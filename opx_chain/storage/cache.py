"""ProviderCache implementations: NullCache (no-op) and FilesystemCache (disk-backed).

Use get_provider_cache(config) to obtain the cache configured by [storage] settings.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import ClassVar, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback.
    fcntl = None  # type: ignore[assignment]

from opx_chain.json_utils import dumps_strict_json, loads_strict_json
from opx_chain.storage.atomic import atomic_write_bytes, atomic_write_text
from opx_chain.timestamps import parse_iso_datetime


class NullCache:  # pylint: disable=too-few-public-methods
    """No-op cache that never stores anything. Default when cache is disabled."""

    def get(self, key: str) -> bytes | None:  # pylint: disable=unused-argument
        """Always return None."""
        return None

    def put(self, key: str, value: bytes, ttl_seconds: int) -> None:  # pylint: disable=unused-argument
        """Discard the value."""

    def invalidate(self, key: str) -> None:  # pylint: disable=unused-argument
        """No-op."""


class FilesystemCache:
    """Disk-backed cache with per-entry TTL."""

    _pruned_dirs: ClassVar[set[Path]] = set()
    _prune_lock: ClassVar[Lock] = Lock()
    _io_lock: ClassVar[Lock] = Lock()

    def __init__(self, cache_dir: Path) -> None:
        self._dir = Path(cache_dir)
        self._prune_expired_once()

    def _prune_expired_once(self) -> None:
        """Run startup pruning once per cache directory in this process."""
        cache_dir = self._dir.expanduser().resolve()
        with self._prune_lock:
            if cache_dir in self._pruned_dirs:
                return
            self.prune_expired()
            self._pruned_dirs.add(cache_dir)

    def _key_paths(self, key: str) -> tuple[Path, Path]:
        digest = hashlib.sha256(key.encode()).hexdigest()
        return self._dir / f"{digest}.bin", self._dir / f"{digest}.meta.json"

    @staticmethod
    def _unlink_entry(bin_path: Path, meta_path: Path) -> None:
        bin_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)

    @contextmanager
    def _locked_cache(self, *, create: bool = False) -> Iterator[None]:
        if create:
            self._dir.mkdir(parents=True, exist_ok=True)
        with self._io_lock:
            if create:
                self._dir.mkdir(parents=True, exist_ok=True)
            lock_file = None
            if fcntl is not None:
                lock_file = (self._dir / ".cache.lock").open("a+b")
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if lock_file is not None:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()

    def get(self, key: str) -> bytes | None:
        """Return cached bytes if present and unexpired, else None."""
        if not self._dir.exists():
            return None
        bin_path, meta_path = self._key_paths(key)
        with self._locked_cache():
            if not bin_path.exists() or not meta_path.exists():
                if bin_path.exists() != meta_path.exists():
                    bin_path.unlink(missing_ok=True)
                    meta_path.unlink(missing_ok=True)
                return None
            try:
                meta = loads_strict_json(meta_path.read_text(encoding="utf-8"))
                expires_at = parse_iso_datetime(meta["expires_at"])
                if datetime.now(tz=timezone.utc) > expires_at:
                    self._unlink_entry(bin_path, meta_path)
                    return None
                return bin_path.read_bytes()
            except (OSError, KeyError, TypeError, ValueError):
                self._unlink_entry(bin_path, meta_path)
                return None

    def put(self, key: str, value: bytes, ttl_seconds: int) -> None:
        """Write bytes to disk with an expiry timestamp."""
        with self._locked_cache(create=True):
            bin_path, meta_path = self._key_paths(key)
            expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl_seconds)
            atomic_write_bytes(bin_path, value)
            atomic_write_text(
                meta_path,
                dumps_strict_json({"key": key, "expires_at": expires_at.isoformat()}),
            )

    def invalidate(self, key: str) -> None:
        """Delete the cache entry for a key if it exists."""
        if not self._dir.exists():
            return
        bin_path, meta_path = self._key_paths(key)
        with self._locked_cache():
            self._unlink_entry(bin_path, meta_path)

    def prune_expired(self) -> None:
        """Remove expired or unreadable cache entries from the cache directory."""
        if not self._dir.exists():
            return
        with self._locked_cache():
            now = datetime.now(tz=timezone.utc)
            meta_bins = {
                meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".bin")
                for meta_path in self._dir.glob("*.meta.json")
            }
            for meta_path in self._dir.glob("*.meta.json"):
                bin_path = meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".bin")
                if not bin_path.exists():
                    meta_path.unlink(missing_ok=True)
                    continue
                try:
                    meta = loads_strict_json(meta_path.read_text(encoding="utf-8"))
                    expires_at = parse_iso_datetime(meta["expires_at"])
                except (OSError, KeyError, TypeError, ValueError):
                    meta_path.unlink(missing_ok=True)
                    bin_path.unlink(missing_ok=True)
                    continue
                if now > expires_at:
                    meta_path.unlink(missing_ok=True)
                    bin_path.unlink(missing_ok=True)
            for bin_path in self._dir.glob("*.bin"):
                if bin_path not in meta_bins:
                    bin_path.unlink(missing_ok=True)


def get_provider_cache(config=None):
    """Return a ProviderCache instance based on config, or NullCache when disabled."""
    if config is None:
        from opx_chain.config import get_runtime_config  # pylint: disable=import-outside-toplevel
        config = get_runtime_config()
    if config.provider_cache_backend == "filesystem":
        return FilesystemCache(config.provider_cache_dir)
    return NullCache()
