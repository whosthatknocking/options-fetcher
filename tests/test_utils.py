"""Tests for shared scalar and timestamp utilities."""

import pandas as pd

from opx_chain.storage.serializers import get_serializer
from opx_chain.utils import normalize_timestamp, read_dataset_file


def test_normalize_timestamp_infers_numeric_epoch_units():
    """Numeric vendor timestamps should infer seconds, milliseconds, and nanoseconds."""
    assert str(normalize_timestamp(1710942000)) == "2024-03-20 13:40:00+00:00"
    assert str(normalize_timestamp(1710942000000)) == "2024-03-20 13:40:00+00:00"
    assert str(normalize_timestamp(1710942000000000000)) == "2024-03-20 13:40:00+00:00"


def test_read_dataset_file_normalizes_csv_and_parquet_dtypes(tmp_path):
    """Dataset reads should expose the same core dtypes for CSV and parquet artifacts."""
    frame = pd.DataFrame({
        "underlying_symbol": ["TSLA", "NVDA"],
        "days_to_expiration": pd.Series([21, pd.NA], dtype="Int64"),
        "is_stale_quote": pd.Series([False, pd.NA], dtype="boolean"),
        "earnings_within_5d": pd.Series([True, False], dtype="boolean"),
        "option_quote_time": pd.to_datetime(
            ["2026-05-01T13:00:00.123456Z", None],
            utc=True,
        ),
        "underlying_price_time": pd.to_datetime(
            ["2026-05-01T13:00:01.654321Z", "2026-05-01T13:00:02Z"],
            utc=True,
            format="mixed",
        ),
    })
    csv_path = tmp_path / "dataset.csv"
    parquet_path = tmp_path / "dataset.parquet"
    get_serializer("csv").serialize(frame, str(csv_path))
    get_serializer("parquet").serialize(frame, str(parquet_path))

    csv_result = read_dataset_file(csv_path)
    parquet_result = read_dataset_file(parquet_path)

    expected_dtypes = {
        "days_to_expiration": "Int64",
        "is_stale_quote": "boolean",
        "earnings_within_5d": "boolean",
        "option_quote_time": "datetime64[ns, UTC]",
        "underlying_price_time": "datetime64[ns, UTC]",
    }
    assert {
        column: str(csv_result[column].dtype)
        for column in expected_dtypes
    } == expected_dtypes
    assert {
        column: str(parquet_result[column].dtype)
        for column in expected_dtypes
    } == expected_dtypes
    assert not bool(csv_result.loc[0, "is_stale_quote"])
    assert not bool(parquet_result.loc[0, "is_stale_quote"])
    assert pd.isna(csv_result.loc[1, "is_stale_quote"])
    assert pd.isna(parquet_result.loc[1, "is_stale_quote"])
