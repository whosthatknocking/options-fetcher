"""Dataset serializer protocol and implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd

from opx_chain.storage.atomic import atomic_file_write


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
        return atomic_file_write(dest, lambda tmp_path: df.to_csv(tmp_path, index=False))


class ParquetSerializer:  # pylint: disable=too-few-public-methods
    """Parquet implementation of DatasetSerializer. Requires pyarrow."""

    format = "parquet"

    def __init__(self) -> None:
        ensure_parquet_available()

    def serialize(self, df: pd.DataFrame, path: str) -> int:
        """Write df to path as Parquet. Returns bytes written."""
        dest = Path(path)
        return atomic_file_write(
            dest,
            lambda tmp_path: df.to_parquet(tmp_path, index=False, engine="pyarrow"),
        )


_SERIALIZERS: dict[str, DatasetSerializer] = {
    CsvSerializer.format: CsvSerializer(),
}


def get_serializer(fmt: str) -> DatasetSerializer:
    """Return the serializer for the given format name."""
    if fmt == ParquetSerializer.format:
        return ParquetSerializer()
    if fmt in _SERIALIZERS:
        return _SERIALIZERS[fmt]
    raise ValueError(f"Unsupported dataset format: {fmt!r}")


def ensure_parquet_available() -> None:
    """Raise a consistent error when the optional parquet dependency is missing."""
    try:
        import pyarrow as _pyarrow  # pylint: disable=import-outside-toplevel
        del _pyarrow
    except ImportError as exc:
        raise RuntimeError(
            "Parquet serialization requires pyarrow. "
            "Install it with: pip install 'opx-chain[parquet]'"
        ) from exc
