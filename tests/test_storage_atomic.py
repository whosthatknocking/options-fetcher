"""Atomic storage-write helper tests."""

from pathlib import Path

import pandas as pd
import pytest

from opx_chain.storage.atomic import atomic_file_write, atomic_write_bytes
from opx_chain.storage.serializers import CsvSerializer


def _temp_files_for(path: Path) -> list[Path]:
    return list(path.parent.glob(f".{path.name}.*.tmp"))


def test_atomic_write_bytes_replaces_existing_file_and_cleans_temp(tmp_path: Path):
    """Successful writes should replace existing content without temp leftovers."""
    dest = tmp_path / "artifact.bin"
    dest.write_bytes(b"old")

    atomic_write_bytes(dest, b"new")

    assert dest.read_bytes() == b"new"
    assert not _temp_files_for(dest)


def test_atomic_file_write_preserves_existing_file_when_writer_fails(tmp_path: Path):
    """Failed temp writes must leave the existing destination intact."""
    dest = tmp_path / "run.json"
    dest.write_text("old", encoding="utf-8")

    def failing_writer(tmp_path: Path) -> None:
        tmp_path.write_text("partial", encoding="utf-8")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        atomic_file_write(dest, failing_writer)

    assert dest.read_text(encoding="utf-8") == "old"
    assert not _temp_files_for(dest)


def test_csv_serializer_writes_without_temp_leftovers(tmp_path: Path):
    """Dataset serializers should write through the shared atomic file helper."""
    dest = tmp_path / "dataset.csv"
    serializer = CsvSerializer()

    bytes_written = serializer.serialize(pd.DataFrame({"ticker": ["TSLA"]}), str(dest))

    assert bytes_written == dest.stat().st_size
    assert "TSLA" in dest.read_text(encoding="utf-8")
    assert not _temp_files_for(dest)
