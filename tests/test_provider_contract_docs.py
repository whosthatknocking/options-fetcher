"""Provider contract documentation tests."""

import re
from pathlib import Path

from opx_chain.providers.base import DataProvider


ROOT = Path(__file__).resolve().parents[1]
METHOD_PATTERN = re.compile(r"^    def ([A-Za-z_]\w*)\(", flags=re.MULTILINE)


def _runtime_methods() -> set[str]:
    return {
        name
        for name, value in vars(DataProvider).items()
        if not name.startswith("_") and (callable(value) or isinstance(value, property))
    }


def test_project_spec_provider_methods_match_runtime_interface():
    """The canonical project spec should list every DataProvider method."""
    spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")
    snippet = spec.split("class DataProvider(ABC):", maxsplit=1)[1]
    snippet = snippet.split("Shared contract rules:", maxsplit=1)[0]
    documented = set(METHOD_PATTERN.findall(snippet))

    assert documented == _runtime_methods()


def test_development_provider_contract_names_runtime_methods():
    """Development docs should expose the provider interface checklist."""
    guide = (ROOT / "docs" / "DEVELOPMENT.md").read_text(encoding="utf-8")

    for method_name in _runtime_methods():
        assert f"`DataProvider.{method_name}`" in guide
