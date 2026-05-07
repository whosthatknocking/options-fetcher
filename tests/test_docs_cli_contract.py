"""Documentation coverage tests for CLI contracts."""

import inspect
from pathlib import Path

from opx_chain.config import (
    DEFAULT_ENABLE_FILTERS,
    DEFAULT_MAX_SPREAD_PCT_OF_MID,
    DEFAULT_MAX_STRIKE_DISTANCE_PCT,
    DEFAULT_MIN_BID,
    DEFAULT_MIN_OPEN_INTEREST,
    DEFAULT_MIN_VOLUME,
)
from opx_chain.check_positions import find_latest_output
from opx_chain.fetcher import run_fetch


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


def test_opx_check_latest_docstring_does_not_claim_symlink():
    """The latest CSV lookup docstring should match copy-based runtime behavior."""
    assert "latest copy" in inspect.getdoc(find_latest_output)
    assert "symlink" not in inspect.getdoc(find_latest_output).lower()


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


def test_min_bid_docs_describe_screen_not_export_filter():
    """filters_min_bid should be documented as a screen, not a row removal filter."""
    guide = (ROOT / "docs" / "USER_GUIDE.md").read_text(encoding="utf-8")
    project_spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")

    assert "it does not remove those rows from the exported dataset" in guide
    assert "rows are not removed solely by this threshold" in project_spec
    assert "exclude contracts below that premium threshold" not in guide


def test_recommended_dataset_reader_is_stable_public_surface():
    """The recommended artifact reader must not live outside the public API list."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    public_surface = spec.split("### 3.1 Public surface", maxsplit=1)[1]
    public_surface = public_surface.split("### 3.2", maxsplit=1)[0]
    reader_section = spec.split("### 3.7 Reading the chain artifact", maxsplit=1)[1]

    assert "from opx_chain.utils import read_dataset_file" in public_surface
    assert "from opx_chain.utils import read_dataset_file" in reader_section
    assert "only stable public import from `opx_chain.utils`" in public_surface


def test_positions_parser_is_stable_public_surface():
    """Downstream positions parsing should be covered by the public API contract."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    public_surface = spec.split("### 3.1 Public surface", maxsplit=1)[1]
    public_surface = public_surface.split("### 3.2", maxsplit=1)[0]
    positions_section = spec.split("### 3.8 Parsing positions consistently", maxsplit=1)[1]
    for name in (
        "OptionPositionKey",
        "PositionSet",
        "load_positions",
        "positions_fingerprint",
    ):
        assert name in public_surface
        assert name in positions_section
    assert "positions.option_keys" in positions_section
    assert "positions_fingerprint(Path" in positions_section


def test_run_fetch_public_params_are_documented():
    """The in-process fetch contract should document every public parameter."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    section = spec.split("### 3.2 Triggering a fresh fetch programmatically", maxsplit=1)[1]
    section = section.split("### 3.3", maxsplit=1)[0]

    for param in inspect.signature(run_fetch).parameters:
        assert f"**`{param}`" in section


def test_agents_architecture_map_lists_load_bearing_modules():
    """Agent guidance should keep the architecture map aligned with core modules."""
    agents_doc = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    required_entries = (
        "`opx-check`",
        "`opx_chain/check_positions.py`",
        "`opx_chain/paths.py`",
        "`opx_chain/positions.py`",
        "`opx_chain/runlog.py`",
        "`opx_chain/schema.py`",
        "`opx_chain/storage/`",
        "`opx_chain/utils.py`",
        "`opx_chain/version.py`",
    )

    for entry in required_entries:
        assert entry in agents_doc


def test_canonical_doc_indexes_list_source_of_truth_docs():
    """Canonical doc indexes should surface source-of-truth docs."""
    agents_doc = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    source_of_truth = agents_doc.split("## Source of Truth", maxsplit=1)[1]
    source_of_truth = source_of_truth.split("## Architecture Map", maxsplit=1)[0]

    project_spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")
    doc_layout = project_spec.split("### 8.1 Documentation Layout", maxsplit=1)[1]
    doc_layout = doc_layout.split("### 8.2", maxsplit=1)[0]

    required_docs = (
        "AGENTS.md",
        "docs/STORAGE_SPEC.md",
        "docs/METADATA_SPEC.md",
    )
    for doc_path in required_docs:
        assert f"`{doc_path}`" in source_of_truth
        assert f"`{doc_path}`" in doc_layout


def test_provider_cache_default_path_is_documented_precisely():
    """User-facing docs should point to the actual default filesystem cache path."""
    expected_path = "$XDG_CACHE_HOME/opx-chain/cache/"
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    development = (ROOT / "docs" / "DEVELOPMENT.md").read_text(encoding="utf-8")
    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")

    assert expected_path in readme
    assert expected_path in development
    assert expected_path in agents


def test_user_guide_shared_settings_use_lowercase_toml_keys():
    """Shared settings examples should be copyable into TOML config files."""
    guide = (ROOT / "docs" / "USER_GUIDE.md").read_text(encoding="utf-8")
    shared_settings = guide.split("### Shared Settings", maxsplit=1)[1]
    shared_settings = shared_settings.split("### Provider Settings", maxsplit=1)[0]
    uppercase_keys = (
        "TICKERS",
        "FILTERS_MIN_BID",
        "FILTERS_MIN_OPEN_INTEREST",
        "FILTERS_MIN_VOLUME",
        "FILTERS_MAX_SPREAD_PCT_OF_MID",
        "FILTERS_MAX_STRIKE_DISTANCE_PCT",
        "RISK_FREE_RATE",
        "HV_LOOKBACK_DAYS",
        "TRADING_DAYS_PER_YEAR",
        "STALE_QUOTE_SECONDS",
        "MAX_EXPIRATION_WEEKS",
        "VIEWER_HOST",
        "VIEWER_PORT",
        "OPTION_SCORE_INCOME_WEIGHT",
        "OPTION_SCORE_LIQUIDITY_WEIGHT",
        "OPTION_SCORE_RISK_WEIGHT",
        "OPTION_SCORE_EFFICIENCY_WEIGHT",
        "FILTERS_ENABLE",
        "ENABLE_VALIDATION",
        "DEBUG_DUMP_PROVIDER_PAYLOAD",
        "DEBUG_DUMP_DIR",
    )

    for key in uppercase_keys:
        assert f"`{key}" not in shared_settings

    expected_tickers = (
        '`tickers = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]`'
    )
    assert expected_tickers in shared_settings
    assert "`filters_min_open_interest = 100`" in shared_settings
    assert "`enable_validation = true`" in shared_settings


def test_user_guide_documents_valid_max_expiration_disable_value():
    """The max-expiration disable guidance should be valid TOML."""
    guide = (ROOT / "docs" / "USER_GUIDE.md").read_text(encoding="utf-8")

    assert "set it to `0` to disable the expiration cap entirely" in guide
    assert "set it to `0` to disable the max-expiration cutoff entirely" in guide
    assert "set to `null`" not in guide
    assert "TOML has no null literal" in guide
