"""Config-loader tests for optional price context settings."""

from pathlib import Path

from opx_chain.config import load_runtime_config


def test_load_runtime_config_defaults_invalid_price_context_settings(tmp_path: Path):
    """Invalid price-context settings should warn and fall back to safe defaults."""
    config_path = tmp_path / "bad-price-context.toml"
    config_path.write_text(
        """
[price_context]
enable = "maybe"
lookback_days = 5
max_age_days = -1
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.price_context_enable is False
    assert config.price_context_lookback_days == 260
    assert config.price_context_max_age_days == 7
    assert any("price_context.enable" in warning for warning in config.config_warnings)
    assert any("price_context.lookback_days" in warning for warning in config.config_warnings)
    assert any("price_context.max_age_days" in warning for warning in config.config_warnings)
