"""Small scalar conversion helpers shared across fetch and normalization code."""

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from opx_chain.coerce import coerce_bool_or_default
from opx_chain.schema import BOOLEAN_FIELDS


INTEGER_DATASET_COLUMNS = ("days_to_expiration",)
TIMESTAMP_DATASET_COLUMNS = ("option_quote_time", "underlying_price_time")


def _coerce_boolean_value(value):
    """Coerce common persisted boolean representations to nullable booleans."""
    result = coerce_bool_or_default(value, default=None)
    return pd.NA if result is None else result


def _normalize_boolean_series(series: pd.Series) -> pd.Series:
    """Return a pandas nullable boolean series for CSV or parquet input."""
    if pd.api.types.is_bool_dtype(series):
        return series.astype("boolean")
    values = [_coerce_boolean_value(value) for value in series]
    return pd.Series(values, index=series.index, dtype="boolean")


def _normalize_dataset_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Apply canonical artifact dtypes after reading CSV or parquet."""
    normalized = df.copy()
    for column in INTEGER_DATASET_COLUMNS:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(
                normalized[column], errors="coerce"
            ).astype("Int64")
    for column in BOOLEAN_FIELDS:
        if column in normalized.columns:
            normalized[column] = _normalize_boolean_series(normalized[column])
    for column in TIMESTAMP_DATASET_COLUMNS:
        if column in normalized.columns:
            timestamp_values = pd.to_datetime(
                normalized[column], utc=True, errors="coerce", format="mixed"
            )
            normalized[column] = timestamp_values.dt.as_unit("ns")
    return normalized


def _requested_columns(columns: Iterable[str] | None) -> list[str] | None:
    if columns is None:
        return None
    return list(dict.fromkeys(columns))


def _parquet_columns(path: Path, columns: list[str] | None) -> list[str] | None:
    if columns is None:
        return None
    try:
        import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel
    except ImportError:
        return columns
    available_columns = set(pq.ParquetFile(path).schema.names)
    return [column for column in columns if column in available_columns]


def read_dataset_file(
    path: Path,
    *,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Read a dataset artifact from disk and normalize format-sensitive dtypes."""
    requested_columns = _requested_columns(columns)
    if path.suffix.lower() == ".parquet":
        parquet_columns = _parquet_columns(path, requested_columns)
        if requested_columns is not None and not parquet_columns:
            return _normalize_dataset_dtypes(pd.DataFrame())
        return _normalize_dataset_dtypes(pd.read_parquet(path, columns=parquet_columns))
    if requested_columns is None:
        return _normalize_dataset_dtypes(pd.read_csv(path, low_memory=False))
    requested_set = set(requested_columns)
    return _normalize_dataset_dtypes(
        pd.read_csv(
            path,
            low_memory=False,
            usecols=lambda column: column in requested_set,
        )
    )


def coerce_float(value):
    """Convert scalar inputs to float while keeping missing values as NaN."""
    return pd.to_numeric(value, errors="coerce")


def finite_float(value) -> float:
    """Convert a scalar to a finite float, returning NaN for invalid values."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return parsed if np.isfinite(parsed) else float("nan")


def finite_float_or_none(value) -> float | None:
    """Convert a scalar to a finite float, returning None for invalid values."""
    parsed = finite_float(value)
    return parsed if np.isfinite(parsed) else None


def is_finite_positive_number(value) -> bool:
    """Return true only for scalar values that coerce to finite positive floats."""
    parsed = finite_float(value)
    return bool(np.isfinite(parsed) and parsed > 0)


def normalize_timestamp(value):
    """Convert vendor timestamps to timezone-aware UTC pandas timestamps."""
    if value is None or pd.isna(value):
        return pd.NaT

    if isinstance(value, (int, float, np.integer, np.floating)):
        numeric_value = float(value)
        absolute_value = abs(numeric_value)
        if absolute_value >= 1e17:
            unit = "ns"
        elif absolute_value >= 1e14:
            unit = "us"
        elif absolute_value >= 1e11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(value, unit=unit, utc=True, errors="coerce")

    return pd.to_datetime(value, utc=True, errors="coerce")
