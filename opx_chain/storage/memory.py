"""In-memory storage backend for testing."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone

from opx_chain.storage.models import (
    ArtifactRecord,
    ArtifactWrite,
    DatasetHandle,
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunRecord,
    RunSummary,
    TickerFetchResult,
    TickerRunRecord,
    ValidationRecord,
    record_to_handle,
)
from opx_chain.storage.serializers import get_serializer


class MemoryBackend:
    """StorageBackend backed entirely by in-memory dicts.

    Writes no files. Used in tests that exercise the storage-enabled
    branches of fetcher.py and opx-check.
    """

    def __init__(self) -> None:
        """Initialise empty in-memory stores."""
        self._runs: dict[str, RunRecord] = {}
        self._datasets: list[DatasetRecord] = []
        self._ticker_results: dict[str, list[TickerRunRecord]] = {}
        self._validations: dict[str, list[ValidationRecord]] = {}
        self._artifacts: dict[str, list[ArtifactRecord]] = {}
        self._artifact_bytes: dict[str, bytes] = {}

    def create_run(self, context: RunContext) -> str:
        """Open a new run record and return its run_id."""
        run_id = str(uuid.uuid4())
        self._runs[run_id] = RunRecord(
            run_id=run_id,
            started_at=datetime.now(tz=timezone.utc),
            finished_at=None,
            status="running",
            provider=context.provider,
            script_version=context.script_version,
            tickers=context.tickers,
            config_fingerprint=context.config_fingerprint,
            positions_fingerprint=context.positions_fingerprint,
            dataset_id=None,
            error_summary=None,
        )
        return run_id

    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None:
        """Append a per-ticker fetch result to the run."""
        record = TickerRunRecord(
            run_id=run_id,
            ticker=result.ticker,
            raw_row_count=result.raw_row_count,
            normalized_row_count=result.normalized_row_count,
            kept_row_count=result.kept_row_count,
            filtered_row_count=result.filtered_row_count,
            expiration_count=result.expiration_count,
            status=result.status,
            error_summary=result.error_summary,
        )
        self._ticker_results.setdefault(run_id, []).append(record)

    def record_validation(self, record: ValidationRecord) -> None:
        """Append a validation summary record under its run_id."""
        self._validations.setdefault(record.run_id, []).append(record)

    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord:
        """Serialize the DataFrame in memory and record the dataset."""
        dataset_id = str(uuid.uuid4())
        serializer = get_serializer(dataset.format)
        content = serializer.serialize_bytes(dataset.data)
        content_hash = hashlib.sha256(content).hexdigest()
        record = DatasetRecord(
            dataset_id=dataset_id,
            run_id=run_id,
            created_at=datetime.now(tz=timezone.utc),
            provider=dataset.provider,
            schema_version=dataset.schema_version,
            row_count=len(dataset.data),
            format=dataset.format,
            location=f"memory://datasets/{dataset_id}.{dataset.format}",
            content_hash=content_hash,
            script_version=dataset.script_version,
        )
        self._datasets.append(record)
        self._artifact_bytes[dataset_id] = content
        if run_id in self._runs:
            self._runs[run_id].dataset_id = dataset_id
        return record

    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord:
        """Store artifact bytes in memory and return an ArtifactRecord."""
        artifact_id = str(uuid.uuid4())
        content_hash = hashlib.sha256(artifact.content).hexdigest()
        record = ArtifactRecord(
            artifact_id=artifact_id,
            run_id=run_id,
            artifact_type=artifact.artifact_type,
            location=f"memory://artifacts/{artifact_id}/{artifact.filename}",
            content_hash=content_hash,
        )
        self._artifacts.setdefault(run_id, []).append(record)
        return record

    def delete_run_artifacts(self, run_id: str) -> None:
        """Delete artifacts associated with a run."""
        for artifact in self._artifacts.pop(run_id, []):
            self._artifact_bytes.pop(artifact.artifact_id, None)

    def list_datasets(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
    ) -> list[DatasetRecord]:
        """Return datasets in reverse chronological order, newest first."""
        results = list(reversed(self._datasets))
        if provider is not None:
            results = [r for r in results if r.provider == provider]
        if since is not None:
            results = [r for r in results if r.created_at >= since]
        if until is not None:
            results = [r for r in results if r.created_at <= until]
        if ticker is not None:
            expected = ticker.upper()
            results = [
                record for record in results
                if expected in {symbol.upper() for symbol in self._runs[record.run_id].tickers}
                or any(
                    row.ticker.upper() == expected
                    for row in self._ticker_results.get(record.run_id, [])
                )
            ]
        return results[:limit]

    def get_dataset(self, dataset_id: str) -> DatasetHandle:
        """Return a DatasetHandle for the given dataset_id."""
        for record in self._datasets:
            if record.dataset_id == dataset_id:
                return record_to_handle(record)
        raise KeyError(f"dataset not found: {dataset_id}")

    def get_run(self, run_id: str) -> RunRecord:
        """Return the RunRecord for the given run_id."""
        if run_id not in self._runs:
            raise KeyError(f"run not found: {run_id}")
        return self._runs[run_id]

    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]:
        """Return per-ticker results stored for a run."""
        if run_id not in self._runs:
            raise KeyError(f"run not found: {run_id}")
        return list(self._ticker_results.get(run_id, []))

    def finalize_run(self, run_id: str, summary: RunSummary) -> None:
        """Mark run as complete or interrupted with the given summary."""
        if run_id in self._runs:
            run = self._runs[run_id]
            run.status = summary.status
            run.finished_at = datetime.now(tz=timezone.utc)
            run.error_summary = summary.error_summary

    def fail_run(self, run_id: str, error: str) -> None:
        """Mark run as failed with the given error message."""
        if run_id in self._runs:
            run = self._runs[run_id]
            run.status = "failed"
            run.finished_at = datetime.now(tz=timezone.utc)
            run.error_summary = error

    def interrupt_stale_runs(self, cutoff: datetime, error_summary: str) -> int:
        """Mark running runs older than cutoff as interrupted."""
        interrupted = 0
        for run in self._runs.values():
            if run.status != "running" or run.started_at >= cutoff:
                continue
            run.status = "interrupted"
            run.finished_at = datetime.now(tz=timezone.utc)
            run.error_summary = error_summary
            interrupted += 1
        return interrupted

    def count_runs_today(self, provider: str) -> int:
        """Return the number of complete runs started today (US/Eastern) for the provider."""
        from opx_chain.config import US_MARKET_TIMEZONE  # pylint: disable=import-outside-toplevel
        now_et = datetime.now(tz=US_MARKET_TIMEZONE)
        midnight_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        since_utc = midnight_et.astimezone(timezone.utc)
        return sum(
            1
            for run in self._runs.values()
            if (
                run.provider == provider
                and run.status == "complete"
                and run.started_at >= since_utc
            )
        )
