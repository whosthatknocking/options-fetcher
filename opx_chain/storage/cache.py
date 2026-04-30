"""ProviderCache implementations: NullCache (no-op) and FilesystemCache (disk-backed).

Use get_provider_cache(config) to obtain the cache configured by [storage] settings.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from opx_chain.storage.atomic import atomic_write_bytes, atomic_write_text


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

    def __init__(self, cache_dir: Path) -> None:
        self._dir = Path(cache_dir)
        self.prune_expired()

    def _key_paths(self, key: str) -> tuple[Path, Path]:
        digest = hashlib.sha256(key.encode()).hexdigest()
        return self._dir / f"{digest}.bin", self._dir / f"{digest}.meta.json"

    def get(self, key: str) -> bytes | None:
        """Return cached bytes if present and unexpired, else None."""
        bin_path, meta_path = self._key_paths(key)
        if not bin_path.exists() or not meta_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            expires_at = datetime.fromisoformat(meta["expires_at"])
            if datetime.now(tz=timezone.utc) > expires_at:
                self.invalidate(key)
                return None
            return bin_path.read_bytes()
        except (OSError, KeyError, ValueError):
            self.invalidate(key)
            return None

    def put(self, key: str, value: bytes, ttl_seconds: int) -> None:
        """Write bytes to disk with an expiry timestamp."""
        self._dir.mkdir(parents=True, exist_ok=True)
        bin_path, meta_path = self._key_paths(key)
        expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl_seconds)
        atomic_write_bytes(bin_path, value)
        atomic_write_text(
            meta_path,
            json.dumps({"key": key, "expires_at": expires_at.isoformat()}),
        )

    def invalidate(self, key: str) -> None:
        """Delete the cache entry for a key if it exists."""
        bin_path, meta_path = self._key_paths(key)
        bin_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)

    def prune_expired(self) -> None:
        """Remove expired or unreadable cache entries from the cache directory."""
        if not self._dir.exists():
            return
        now = datetime.now(tz=timezone.utc)
        meta_bins = {
            meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".bin")
            for meta_path in self._dir.glob("*.meta.json")
        }
        for meta_path in self._dir.glob("*.meta.json"):
            bin_path = meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".bin")
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                expires_at = datetime.fromisoformat(meta["expires_at"])
            except (OSError, KeyError, ValueError):
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
