"""Shared coercion helpers for runtime config values."""

from __future__ import annotations

import math
from pathlib import Path

from opx_chain.coerce import coerce_bool_or_default


class ConfigError(ValueError):
    """Raised when user config is invalid for the requested runtime."""


def _raise_config_requirement(field_name: str, requirement: str) -> None:
    raise ConfigError(f"Config field '{field_name}' {requirement}.")


def _coerce_config_value(value, *, field_name, predicate, requirement, normalizer=None):
    if value is None:
        return None
    if not predicate(value):
        _raise_config_requirement(field_name, requirement)
    if normalizer is None:
        return value
    return normalizer(value, field_name=field_name)


def _normalize_non_empty_list(value: list[str], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(item.strip().upper() for item in value if item.strip())
    if not normalized:
        _raise_config_requirement(field_name, "must not be empty")
    return normalized


def _normalize_non_blank_string(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        _raise_config_requirement(field_name, "must not be blank")
    return normalized


def _normalize_bool(value, *, field_name: str) -> bool:
    resolved = coerce_bool_or_default(value, default=None)
    if resolved is None:
        _raise_config_requirement(field_name, "must be true or false")
    return resolved


def _normalize_finite_float(value: int | float, *, field_name: str) -> float:
    resolved = float(value)
    if not math.isfinite(resolved):
        _raise_config_requirement(field_name, "must be finite")
    return resolved


def _normalize_path(value: str, *, field_name: str) -> Path:
    return Path(_normalize_non_blank_string(value, field_name=field_name)).expanduser()


def coerce_list(value, *, field_name):
    """Coerce an optional list of strings to normalized ticker values."""
    return _coerce_config_value(
        value,
        field_name=field_name,
        predicate=lambda item: isinstance(item, list)
        and all(isinstance(entry, str) for entry in item),
        requirement="must be a list of strings",
        normalizer=_normalize_non_empty_list,
    )


def coerce_str(value, *, field_name):
    """Coerce an optional string config value to stripped text."""
    return _coerce_config_value(
        value,
        field_name=field_name,
        predicate=lambda item: isinstance(item, str),
        requirement="must be a string",
        normalizer=_normalize_non_blank_string,
    )


def coerce_int(value, *, field_name):
    """Coerce an optional integer config value, rejecting booleans."""
    return _coerce_config_value(
        value,
        field_name=field_name,
        predicate=lambda item: not isinstance(item, bool) and isinstance(item, int),
        requirement="must be an integer",
    )


def coerce_bool(value, *, field_name):
    """Coerce an optional boolean-like config value."""
    return _coerce_config_value(
        value,
        field_name=field_name,
        predicate=lambda item: coerce_bool_or_default(item, default=None) is not None,
        requirement="must be true or false",
        normalizer=_normalize_bool,
    )


def coerce_float(value, *, field_name):
    """Coerce an optional numeric config value to a finite float."""
    return _coerce_config_value(
        value,
        field_name=field_name,
        predicate=lambda item: not isinstance(item, bool) and isinstance(item, (int, float)),
        requirement="must be numeric",
        normalizer=_normalize_finite_float,
    )


def coerce_path(value, *, field_name):
    """Coerce an optional string path config value."""
    return _coerce_config_value(
        value,
        field_name=field_name,
        predicate=lambda item: isinstance(item, str),
        requirement="must be a string path",
        normalizer=_normalize_path,
    )
