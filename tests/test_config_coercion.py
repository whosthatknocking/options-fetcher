"""Tests for shared runtime config coercion helpers."""

import inspect

import pytest

from opx_chain import config_coercion
from opx_chain.config_coercion import (
    ConfigError,
    coerce_bool,
    coerce_float,
    coerce_int,
    coerce_list,
    coerce_path,
    coerce_str,
)


def test_config_bool_coercion_uses_shared_bool_policy():
    """Config boolean fields should share the dataset bool coercion policy."""
    assert coerce_bool(None, field_name="settings.enable_validation") is None
    assert coerce_bool("on", field_name="settings.enable_validation") is True
    assert coerce_bool("off", field_name="settings.enable_validation") is False
    assert coerce_bool(1, field_name="settings.enable_validation") is True
    assert coerce_bool(0, field_name="settings.enable_validation") is False

    with pytest.raises(ConfigError, match="settings.enable_validation"):
        coerce_bool("garbage", field_name="settings.enable_validation")


def test_config_coercers_share_optional_value_handling():
    """Public config coercers should not reimplement the same None/type template."""
    for name in (
        "coerce_list",
        "coerce_str",
        "coerce_int",
        "coerce_bool",
        "coerce_float",
        "coerce_path",
    ):
        source = inspect.getsource(getattr(config_coercion, name))
        assert "if value is None" not in source
        assert "_coerce_config_value(" in source


def test_config_coercers_preserve_existing_normalization(tmp_path, monkeypatch):
    """The shared template should not change type-specific normalization."""
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))

    assert coerce_list([" spy ", "qqq"], field_name="settings.tickers") == ("SPY", "QQQ")
    assert coerce_str(" yfinance ", field_name="settings.data_provider") == "yfinance"
    assert coerce_int(3, field_name="providers.marketdata.max_retries") == 3
    assert coerce_float(1, field_name="providers.marketdata.backoff_seconds") == 1.0
    assert coerce_path("~/cache", field_name="storage.dir") == home / "cache"

    with pytest.raises(ConfigError, match="must not be empty"):
        coerce_list([" ", ""], field_name="settings.tickers")
    with pytest.raises(ConfigError, match="must not be blank"):
        coerce_str(" ", field_name="settings.data_provider")
    with pytest.raises(ConfigError, match="must be an integer"):
        coerce_int(True, field_name="providers.marketdata.max_retries")
    with pytest.raises(ConfigError, match="must be finite"):
        coerce_float(float("inf"), field_name="providers.marketdata.backoff_seconds")
    with pytest.raises(ConfigError, match="must not be blank"):
        coerce_path(" ", field_name="storage.dir")
