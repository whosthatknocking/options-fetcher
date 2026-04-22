"""Dataset serializer protocol and implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd


class DatasetSerializer(Protocol):  # pylint: disable=too-few-public-methods
    """Serialize a DataFrame to a file path. Returns bytes written."""

    format: str  # "csv" | "parquet"

    def serialize(self, df: pd.DataFrame, path: str) -> int: ...  # pylint: disable=missing-function-docstring


class CsvSerializer:  # pylint: disable=too-few-public-methods
    """CSV implementation of DatasetSerializer."""

    format = "csv"

    def serialize(self, df: pd.DataFrame, path: str) -> int:
        """Write df to path as CSV. Returns bytes written."""
        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(dest, index=False)
        return dest.stat().st_size
