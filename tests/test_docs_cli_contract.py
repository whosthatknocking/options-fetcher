"""Documentation coverage tests for CLI contracts."""

from pathlib import Path

from opx_chain.config import (
    DEFAULT_ENABLE_FILTERS,
    DEFAULT_MAX_SPREAD_PCT_OF_MID,
    DEFAULT_MAX_STRIKE_DISTANCE_PCT,
    DEFAULT_MIN_BID,
    DEFAULT_MIN_OPEN_INTEREST,
    DEFAULT_MIN_VOLUME,
)


ROOT = Path(__file__).resolve().parents[1]


def test_dry_run_cli_flag_is_documented():
    """The zero-call fetch preflight flag should stay visible in user docs."""
    docs = {
        "README.md": ROOT / "README.md",
        "USER_GUIDE.md": ROOT / "docs" / "USER_GUIDE.md",
        "EXTERNAL_INTERFACE_SPEC.md": ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md",
    }

    for name, path in docs.items():
        assert "--dry-run" in path.read_text(encoding="utf-8"), name


def test_project_spec_lists_builtin_filter_defaults():
    """The canonical defaults list should cover every shared filter knob."""
    spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")
    min_bid_value = "disabled" if DEFAULT_MIN_BID is None else str(DEFAULT_MIN_BID)
    enable_filters_value = str(DEFAULT_ENABLE_FILTERS).lower()

    expected_lines = (
        f"`filters_max_spread_pct_of_mid = {DEFAULT_MAX_SPREAD_PCT_OF_MID}`",
        f"`filters_max_strike_distance_pct = {DEFAULT_MAX_STRIKE_DISTANCE_PCT}`",
        f"`filters_min_bid = {min_bid_value}`",
        f"`filters_min_open_interest = {DEFAULT_MIN_OPEN_INTEREST}`",
        f"`filters_min_volume = {DEFAULT_MIN_VOLUME}`",
        f"`filters_enable = {enable_filters_value}`",
    )

    for line in expected_lines:
        assert line in spec
