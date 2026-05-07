"""Static viewer-template checks for critical DOM hooks."""

from html.parser import HTMLParser
from pathlib import Path


STATIC_INDEX_PATH = (
    Path(__file__).resolve().parent.parent / "opx_chain" / "viewer_static" / "index.html"
)
STATIC_STYLES_PATH = (
    Path(__file__).resolve().parent.parent / "opx_chain" / "viewer_static" / "styles.css"
)


class IdCollector(HTMLParser):
    """Collect element IDs from the static viewer HTML."""

    def __init__(self):
        super().__init__()
        self.ids: set[str] = set()

    def handle_starttag(self, _tag, attrs):
        for key, value in attrs:
            if key == "id" and value:
                self.ids.add(value)


def test_viewer_index_contains_critical_dom_hooks():
    """Critical app.js DOM hooks should remain present in the static template."""
    parser = IdCollector()
    parser.feed(STATIC_INDEX_PATH.read_text(encoding="utf-8"))

    assert {
        "dataTable",
        "tableStatus",
        "filterPopover",
        "filterPopoverTitle",
        "filterValueSearch",
        "filterMinValue",
        "filterMaxValue",
        "clearFilterButton",
        "rowModal",
        "summaryTab",
        "tableTab",
        "chainTab",
        "readmeTab",
        "themeToggle",
    }.issubset(parser.ids)


def test_viewer_index_does_not_expose_positions_tab():
    """opx-chain viewer should not become a rich portfolio browser."""
    html = STATIC_INDEX_PATH.read_text(encoding="utf-8")

    assert 'data-tab="positions"' not in html
    assert "positionsDataTable" not in html


def test_viewer_header_theme_tokens_are_mode_specific():
    """Light mode should not inherit the dark header/tab treatment."""
    styles = STATIC_STYLES_PATH.read_text(encoding="utf-8")
    _before_overrides, overrides = styles.split(
        "/* Architectural Ledger overrides */",
        maxsplit=1,
    )
    light_block, dark_and_rest = overrides.split(
        ':root[data-theme="dark"],',
        maxsplit=1,
    )
    dark_block, _body_and_rest = dark_and_rest.split("body {", maxsplit=1)

    assert "--header-chip-shell: #ffffff;" in light_block
    assert "--header-chip-active: #e2e8f0;" in light_block
    assert "--header-chip-text-active: #0f172a;" in light_block
    assert "--header-chip-shell: #111a2e;" in dark_block
    assert "--header-chip-active: #1c2942;" in dark_block
    assert "--header-chip-text-active: #f8fafc;" in dark_block
