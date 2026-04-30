"""Shared disk-write utilities for filesystem-backed storage implementations."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import pandas as pd

from opx_chain.storage.atomic import atomic_write_bytes
from opx_chain.storage.serializers import DatasetSerializer


def write_dataset_artifact(
    data: pd.DataFrame,
    output_dir: Path,
    dataset_format: str,
    serializer: DatasetSerializer,
) -> tuple[str, Path, str]:
    """Write a DataFrame artifact to disk. Returns (dataset_id, artifact_path, content_hash)."""
    dataset_id = str(uuid.uuid4())
    artifact_path = (output_dir / f"{dataset_id}.{dataset_format}").resolve()
    content = serializer.serialize_bytes(data)
    atomic_write_bytes(artifact_path, content)
    content_hash = hashlib.sha256(content).hexdigest()
    return dataset_id, artifact_path, content_hash


def write_artifact_bytes(
    content: bytes,
    debug_dir: Path,
    filename: str,
) -> tuple[str, Path, str]:
    """Write raw artifact bytes to disk. Returns (artifact_id, dest_path, content_hash)."""
    artifact_id = str(uuid.uuid4())
    dest = (debug_dir / artifact_id / filename).resolve()
    atomic_write_bytes(dest, content)
    content_hash = hashlib.sha256(content).hexdigest()
    return artifact_id, dest, content_hash
