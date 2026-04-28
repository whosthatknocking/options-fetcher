"""Storage contract documentation tests."""

import re
from pathlib import Path

from opx_chain.storage.base import StorageBackend


ROOT = Path(__file__).resolve().parents[1]


def test_storage_spec_protocol_methods_match_runtime_protocol():
    """The canonical spec snippet should list every StorageBackend method."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")
    snippet = spec.split("class StorageBackend(Protocol):", maxsplit=1)[1]
    snippet = snippet.split("class ProviderCache(Protocol):", maxsplit=1)[0]
    documented = set(
        re.findall(
            r"^    def ([a-zA-Z_][a-zA-Z0-9_]*)\(",
            snippet,
            flags=re.MULTILINE,
        )
    )
    runtime = {
        name
        for name, value in vars(StorageBackend).items()
        if not name.startswith("_") and callable(value)
    }

    assert documented == runtime


def test_storage_spec_uses_current_package_paths():
    """Storage docs should not regress to the pre-rename `opx/` package paths."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")

    assert "`opx/" not in spec
    assert "`opx[" not in spec
    assert "`opx." not in spec


def test_storage_spec_documents_current_opx_check_lookup():
    """The opx-check storage contract should match the implemented lookup."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")

    assert "`opx-check` uses `list_datasets(limit=100)`" in spec
    assert "newest readable CSV record" in spec
    assert "`opx-check` uses `list_datasets(limit=1)`" not in spec
