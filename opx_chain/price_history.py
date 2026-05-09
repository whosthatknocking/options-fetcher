"""Durable daily OHLCV history store for price-context calculations."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import sqlite3
import threading
import weakref

import pandas as pd

from opx_chain.paths import get_data_dir
from opx_chain.price_context import normalize_price_history_frame
from opx_chain.timestamps import parse_iso_datetime
from opx_chain.utils import finite_float_or_none


PRICE_HISTORY_SCHEMA_VERSION = 1
PRICE_HISTORY_TAIL_REFRESH_DAYS = 7

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS _schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_price_bars (
    provider     TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    open         REAL,
    high         REAL NOT NULL,
    low          REAL NOT NULL,
    close        REAL NOT NULL,
    volume       REAL,
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (provider, ticker, trading_date)
);

CREATE TABLE IF NOT EXISTS price_history_syncs (
    provider                TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    lookback_days           INTEGER NOT NULL,
    checked_at              TEXT NOT NULL,
    status                  TEXT NOT NULL,
    requested_lookback_days INTEGER,
    latest_trading_date     TEXT,
    fetched_rows            INTEGER NOT NULL DEFAULT 0,
    stored_rows             INTEGER NOT NULL DEFAULT 0,
    error_summary           TEXT,
    PRIMARY KEY (provider, ticker, lookback_days)
);

CREATE INDEX IF NOT EXISTS idx_daily_price_bars_ticker_date
    ON daily_price_bars(provider, ticker, trading_date DESC);
"""


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _date_to_str(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _history_db_path(config=None) -> Path:
    base = Path(config.storage_dir) if config is not None and config.storage_dir else get_data_dir()
    return base / "price-history.db"


@dataclass(frozen=True)
class PriceHistoryStats:
    """Stored daily-bar coverage metadata for one provider/ticker."""

    row_count: int
    earliest_date: date | None
    latest_date: date | None


@dataclass(frozen=True)
class PriceHistorySync:
    """Last reconciliation attempt metadata for one provider/ticker/lookback."""

    checked_at: datetime
    status: str
    requested_lookback_days: int | None
    latest_trading_date: date | None
    fetched_rows: int
    stored_rows: int
    error_summary: str | None


@dataclass(frozen=True)
class PriceHistoryReconcileResult:
    """Result of reconciling local daily bars before price-context calculation."""

    history: pd.DataFrame
    fetched: bool
    requested_lookback_days: int | None = None
    fetched_rows: int = 0
    stored_rows: int = 0
    error_summary: str | None = None


class PriceHistoryStore:
    """SQLite-backed local store of immutable-ish daily OHLCV bars."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._connection: sqlite3.Connection | None = None
        self._connection_finalizer: weakref.finalize | None = None
        self._lock = threading.RLock()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def close(self) -> None:
        """Close the pooled SQLite connection."""
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                if self._connection_finalizer is not None:
                    self._connection_finalizer.detach()
                    self._connection_finalizer = None
                self._connection = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # pragma: no cover  # pylint: disable=broad-exception-caught
            pass

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _connection_for_use(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = self._connect()
            self._connection_finalizer = weakref.finalize(self, self._connection.close)
        return self._connection

    @contextmanager
    def _open_connection(self):
        """Yield the pooled SQLite connection and rollback failed writes."""
        with self._lock:
            conn = self._connection_for_use()
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise

    def _init_schema(self) -> None:
        with self._open_connection() as conn:
            conn.executescript(_SCHEMA_SQL)
            row = conn.execute(
                "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO _schema_meta VALUES ('schema_version', ?)",
                    (str(PRICE_HISTORY_SCHEMA_VERSION),),
                )
            elif int(row["value"]) > PRICE_HISTORY_SCHEMA_VERSION:
                raise RuntimeError(
                    "Price history schema version "
                    f"{row['value']} is newer than supported version "
                    f"{PRICE_HISTORY_SCHEMA_VERSION}"
                )
            conn.commit()

    @staticmethod
    def _normalize_key(value: str) -> str:
        return value.upper().strip()

    def load_bars(
        self,
        *,
        provider: str,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Load stored daily bars for an inclusive date window."""
        provider_key = provider.strip()
        ticker_key = self._normalize_key(ticker)
        with self._lock:
            conn = self._connection_for_use()
            rows = conn.execute(
                """
                SELECT trading_date, open, high, low, close, volume
                FROM daily_price_bars
                WHERE provider = ?
                  AND ticker = ?
                  AND trading_date >= ?
                  AND trading_date <= ?
                ORDER BY trading_date
                """,
                (
                    provider_key,
                    ticker_key,
                    start_date.isoformat(),
                    end_date.isoformat(),
                ),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        return pd.DataFrame(
            {
                "date": pd.to_datetime([row["trading_date"] for row in rows], utc=True),
                "open": [row["open"] for row in rows],
                "high": [row["high"] for row in rows],
                "low": [row["low"] for row in rows],
                "close": [row["close"] for row in rows],
                "volume": [row["volume"] for row in rows],
            }
        )

    def load_recent_bars(
        self,
        *,
        provider: str,
        ticker: str,
        lookback_days: int,
        end_date: date,
    ) -> pd.DataFrame:
        """Load the latest stored daily bars up to an inclusive end date."""
        provider_key = provider.strip()
        ticker_key = self._normalize_key(ticker)
        with self._lock:
            conn = self._connection_for_use()
            rows = conn.execute(
                """
                SELECT trading_date, open, high, low, close, volume
                FROM daily_price_bars
                WHERE provider = ?
                  AND ticker = ?
                  AND trading_date <= ?
                ORDER BY trading_date DESC
                LIMIT ?
                """,
                (provider_key, ticker_key, end_date.isoformat(), max(lookback_days, 0)),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        ordered = list(reversed(rows))
        return pd.DataFrame(
            {
                "date": pd.to_datetime([row["trading_date"] for row in ordered], utc=True),
                "open": [row["open"] for row in ordered],
                "high": [row["high"] for row in ordered],
                "low": [row["low"] for row in ordered],
                "close": [row["close"] for row in ordered],
                "volume": [row["volume"] for row in ordered],
            }
        )

    def stats(self, *, provider: str, ticker: str) -> PriceHistoryStats:
        """Return total coverage stats for one provider/ticker."""
        provider_key = provider.strip()
        ticker_key = self._normalize_key(ticker)
        with self._lock:
            conn = self._connection_for_use()
            row = conn.execute(
                """
                SELECT COUNT(*) AS row_count,
                       MIN(trading_date) AS earliest_date,
                       MAX(trading_date) AS latest_date
                FROM daily_price_bars
                WHERE provider = ? AND ticker = ?
                """,
                (provider_key, ticker_key),
            ).fetchone()
        return PriceHistoryStats(
            row_count=int(row["row_count"] or 0),
            earliest_date=_parse_date(row["earliest_date"]),
            latest_date=_parse_date(row["latest_date"]),
        )

    def upsert_bars(
        self,
        *,
        provider: str,
        ticker: str,
        history: pd.DataFrame,
        fetched_at: datetime | None = None,
    ) -> int:
        """Normalize and upsert daily bars. Returns normalized row count."""
        normalized = normalize_price_history_frame(history)
        if normalized.empty:
            return 0
        provider_key = provider.strip()
        ticker_key = self._normalize_key(ticker)
        fetched_at = fetched_at or _utc_now()
        rows = []
        for _, row in normalized.iterrows():
            high = finite_float_or_none(row["high"])
            low = finite_float_or_none(row["low"])
            close = finite_float_or_none(row["close"])
            if high is None or low is None or close is None:
                continue
            rows.append(
                (
                    provider_key,
                    ticker_key,
                    pd.Timestamp(row["date"]).date().isoformat(),
                    finite_float_or_none(row.get("open")),
                    high,
                    low,
                    close,
                    finite_float_or_none(row.get("volume")),
                    fetched_at.isoformat(),
                )
            )
        if not rows:
            return 0
        with self._open_connection() as conn:
            conn.executemany(
                """
                INSERT INTO daily_price_bars
                    (provider, ticker, trading_date, open, high, low, close, volume, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, ticker, trading_date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def get_sync(
        self,
        *,
        provider: str,
        ticker: str,
        lookback_days: int,
    ) -> PriceHistorySync | None:
        """Return last sync metadata for one provider/ticker/lookback."""
        provider_key = provider.strip()
        ticker_key = self._normalize_key(ticker)
        with self._lock:
            conn = self._connection_for_use()
            row = conn.execute(
                """
                SELECT checked_at, status, requested_lookback_days,
                       latest_trading_date, fetched_rows, stored_rows, error_summary
                FROM price_history_syncs
                WHERE provider = ? AND ticker = ? AND lookback_days = ?
                """,
                (provider_key, ticker_key, lookback_days),
            ).fetchone()
        if row is None:
            return None
        return PriceHistorySync(
            checked_at=parse_iso_datetime(row["checked_at"]),
            status=str(row["status"]),
            requested_lookback_days=row["requested_lookback_days"],
            latest_trading_date=_parse_date(row["latest_trading_date"]),
            fetched_rows=int(row["fetched_rows"] or 0),
            stored_rows=int(row["stored_rows"] or 0),
            error_summary=row["error_summary"],
        )

    def record_sync(  # pylint: disable=too-many-arguments
        self,
        *,
        provider: str,
        ticker: str,
        lookback_days: int,
        status: str,
        requested_lookback_days: int | None,
        latest_trading_date: date | None,
        fetched_rows: int,
        stored_rows: int,
        error_summary: str | None = None,
        checked_at: datetime | None = None,
    ) -> None:
        """Record the latest reconciliation attempt."""
        provider_key = provider.strip()
        ticker_key = self._normalize_key(ticker)
        checked_at = checked_at or _utc_now()
        with self._open_connection() as conn:
            conn.execute(
                """
                INSERT INTO price_history_syncs
                    (provider, ticker, lookback_days, checked_at, status,
                     requested_lookback_days, latest_trading_date, fetched_rows,
                     stored_rows, error_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, ticker, lookback_days) DO UPDATE SET
                    checked_at = excluded.checked_at,
                    status = excluded.status,
                    requested_lookback_days = excluded.requested_lookback_days,
                    latest_trading_date = excluded.latest_trading_date,
                    fetched_rows = excluded.fetched_rows,
                    stored_rows = excluded.stored_rows,
                    error_summary = excluded.error_summary
                """,
                (
                    provider_key,
                    ticker_key,
                    lookback_days,
                    checked_at.isoformat(),
                    status,
                    requested_lookback_days,
                    _date_to_str(latest_trading_date),
                    fetched_rows,
                    stored_rows,
                    error_summary,
                ),
            )
            conn.commit()


def get_price_history_store(config=None) -> PriceHistoryStore:
    """Return the durable local daily-bar store for price context."""
    return PriceHistoryStore(_history_db_path(config))


def _sync_recent(sync: PriceHistorySync | None, *, ttl_seconds: int, now: datetime) -> bool:
    if sync is None:
        return False
    return (now - sync.checked_at).total_seconds() < ttl_seconds


def _fetch_days_for_reason(
    *,
    reason: str,
    lookback_days: int,
    latest_date: date | None,
    today: date,
) -> int:
    if reason in {"missing", "backfill"} or latest_date is None:
        return lookback_days
    age_days = max((today - latest_date).days, 0)
    return min(lookback_days, max(PRICE_HISTORY_TAIL_REFRESH_DAYS, age_days + 1))


def _reconciliation_reason(
    *,
    stats: PriceHistoryStats,
    history: pd.DataFrame,
    lookback_days: int,
    today: date,
) -> str | None:
    if history.empty or stats.row_count == 0:
        return "missing"
    if stats.row_count < lookback_days:
        return "backfill"
    if stats.latest_date is None or stats.latest_date < today:
        return "tail"
    return None


def reconcile_price_history(  # pylint: disable=too-many-locals
    *,
    ticker: str,
    provider,
    config,
    logger=None,
    store: PriceHistoryStore | None = None,
) -> PriceHistoryReconcileResult:
    """Ensure local daily OHLCV coverage and return bars for price-context calculation."""
    store = store or get_price_history_store(config)
    provider_name = provider.name
    today = config.today
    lookback_days = config.price_context_lookback_days
    now = _utc_now()
    history = store.load_recent_bars(
        provider=provider_name,
        ticker=ticker,
        lookback_days=lookback_days,
        end_date=today,
    )
    stats = store.stats(provider=provider_name, ticker=ticker)
    reason = _reconciliation_reason(
        stats=stats,
        history=history,
        lookback_days=lookback_days,
        today=today,
    )
    sync = store.get_sync(
        provider=provider_name,
        ticker=ticker,
        lookback_days=lookback_days,
    )
    if reason is None or _sync_recent(
        sync,
        ttl_seconds=config.provider_price_context_ttl,
        now=now,
    ):
        return PriceHistoryReconcileResult(history=history, fetched=False)

    requested_lookback_days = _fetch_days_for_reason(
        reason=reason,
        lookback_days=lookback_days,
        latest_date=stats.latest_date,
        today=today,
    )
    try:
        raw_history = provider.load_price_history(
            ticker,
            lookback_days=requested_lookback_days,
        )
        fetched_rows = len(raw_history) if isinstance(raw_history, pd.DataFrame) else 0
        stored_rows = store.upsert_bars(
            provider=provider_name,
            ticker=ticker,
            history=raw_history,
            fetched_at=now,
        )
        stats = store.stats(provider=provider_name, ticker=ticker)
        store.record_sync(
            provider=provider_name,
            ticker=ticker,
            lookback_days=lookback_days,
            status="ok",
            requested_lookback_days=requested_lookback_days,
            latest_trading_date=stats.latest_date,
            fetched_rows=fetched_rows,
            stored_rows=stored_rows,
            checked_at=now,
        )
        history = store.load_recent_bars(
            provider=provider_name,
            ticker=ticker,
            lookback_days=lookback_days,
            end_date=today,
        )
        return PriceHistoryReconcileResult(
            history=history,
            fetched=True,
            requested_lookback_days=requested_lookback_days,
            fetched_rows=fetched_rows,
            stored_rows=stored_rows,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        message = str(exc).splitlines()[0]
        stats = store.stats(provider=provider_name, ticker=ticker)
        store.record_sync(
            provider=provider_name,
            ticker=ticker,
            lookback_days=lookback_days,
            status="error",
            requested_lookback_days=requested_lookback_days,
            latest_trading_date=stats.latest_date,
            fetched_rows=0,
            stored_rows=0,
            error_summary=message,
            checked_at=now,
        )
        if logger:
            logger.warning("%s: price_history reconcile failed: %s", ticker, message)
        return PriceHistoryReconcileResult(
            history=history,
            fetched=False,
            requested_lookback_days=requested_lookback_days,
            error_summary=message,
        )
