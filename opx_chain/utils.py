"""Small scalar conversion helpers shared across fetch and normalization code."""

from pathlib import Path

import pandas as pd
import numpy as np

from opx_chain.schema import BOOLEAN_FIELDS


INTEGER_DATASET_COLUMNS = ("days_to_expiration",)
TIMESTAMP_DATASET_COLUMNS = ("option_quote_time", "underlying_price_time")


def _coerce_boolean_value(value):
    """Coerce common persisted boolean representations to nullable booleans."""
    result = pd.NA
    if value is None or pd.isna(value):
        pass
    elif isinstance(value, (bool, np.bool_)):
        result = bool(value)
    elif isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            result = True
        elif normalized in {"false", "0", "no"}:
            result = False
    elif isinstance(value, (int, float, np.integer, np.floating)):
        numeric_value = float(value)
        if numeric_value == 1.0:
            result = True
        elif numeric_value == 0.0:
            result = False
    return result


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


def read_dataset_file(path: Path) -> pd.DataFrame:
    """Read a dataset artifact from disk and normalize format-sensitive dtypes."""
    if path.suffix.lower() == ".parquet":
        return _normalize_dataset_dtypes(pd.read_parquet(path))
    return _normalize_dataset_dtypes(pd.read_csv(path, low_memory=False))


def coerce_float(value):
    """Convert scalar inputs to float while keeping missing values as NaN."""
    return pd.to_numeric(value, errors="coerce")


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
