"""Tests for NullCache, FilesystemCache, and get_provider_cache factory."""

import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from conftest import make_runtime_config
from opx_chain.storage.cache import FilesystemCache, NullCache, get_provider_cache


def _cache_paths(cache_dir: Path, key: str) -> tuple[Path, Path]:
    digest = hashlib.sha256(key.encode()).hexdigest()
    return cache_dir / f"{digest}.bin", cache_dir / f"{digest}.meta.json"


# ---------------------------------------------------------------------------
# NullCache
# ---------------------------------------------------------------------------

def test_null_cache_get_always_returns_none():
    """NullCache.get must always return None regardless of prior puts."""
    cache = NullCache()
    cache.put("k", b"v", ttl_seconds=60)
    assert cache.get("k") is None


def test_null_cache_invalidate_is_no_op():
    """NullCache.invalidate must not raise."""
    NullCache().invalidate("k")


# ---------------------------------------------------------------------------
# FilesystemCache
# ---------------------------------------------------------------------------

def test_filesystem_cache_roundtrip(tmp_path: Path):
    """put then get must return the same bytes when TTL has not expired."""
    cache = FilesystemCache(tmp_path / "cache")
    cache.put("mykey", b"hello", ttl_seconds=60)
    assert cache.get("mykey") == b"hello"


def test_filesystem_cache_miss_returns_none(tmp_path: Path):
    """get must return None for keys that were never put."""
    cache = FilesystemCache(tmp_path / "cache")
    assert cache.get("no-such-key") is None


def test_filesystem_cache_invalidate_removes_entry(tmp_path: Path):
    """invalidate must cause subsequent get calls to return None."""
    cache = FilesystemCache(tmp_path / "cache")
    cache.put("k", b"data", ttl_seconds=60)
    cache.invalidate("k")
    assert cache.get("k") is None


def test_filesystem_cache_expired_returns_none(tmp_path: Path):
    """get must return None when the entry's TTL has elapsed."""
    cache_dir = tmp_path / "cache"
    cache = FilesystemCache(cache_dir)
    cache.put("k", b"x", ttl_seconds=1)
    time.sleep(1.1)

    assert cache.get("k") is None
    bin_path, meta_path = _cache_paths(cache_dir, "k")
    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_prunes_expired_entries_on_startup(tmp_path: Path):
    """Constructor should remove stale cache files left by prior runs."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    bin_path, meta_path = _cache_paths(cache_dir, "old-key")
    bin_path.write_bytes(b"stale")
    meta_path.write_text(
        json.dumps({
            "key": "old-key",
            "expires_at": (datetime.now(tz=timezone.utc) - timedelta(seconds=1)).isoformat(),
        }),
        encoding="utf-8",
    )

    FilesystemCache(cache_dir)

    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_prunes_unreadable_metadata_on_startup(tmp_path: Path):
    """Corrupt metadata should not keep orphaned cache payloads forever."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    bin_path, meta_path = _cache_paths(cache_dir, "bad-key")
    bin_path.write_bytes(b"bad")
    meta_path.write_text("{not json", encoding="utf-8")

    FilesystemCache(cache_dir)

    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_creates_directory(tmp_path: Path):
    """FilesystemCache must create the cache directory on first put."""
    cache_dir = tmp_path / "nested" / "cache"
    cache = FilesystemCache(cache_dir)
    cache.put("k", b"v", ttl_seconds=10)
    assert cache_dir.exists()


def test_filesystem_cache_invalidate_nonexistent_is_safe(tmp_path: Path):
    """invalidate must not raise when the key does not exist."""
    FilesystemCache(tmp_path / "cache").invalidate("ghost")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_returns_null_cache_when_disabled():
    """get_provider_cache must return NullCache when provider_cache_backend = 'none'."""
    config = make_runtime_config(provider_cache_backend="none")
    assert isinstance(get_provider_cache(config), NullCache)


def test_factory_returns_filesystem_cache_when_enabled(tmp_path: Path):
    """get_provider_cache must return FilesystemCache when backend = 'filesystem'."""
    config = make_runtime_config(
        provider_cache_backend="filesystem",
        provider_cache_dir=tmp_path / "cache",
    )
    assert isinstance(get_provider_cache(config), FilesystemCache)
