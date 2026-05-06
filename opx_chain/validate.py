"""Shared row-level and file-level validation for canonical option data."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from opx_chain.schema import BOOLEAN_FIELDS


REQUIRED_CORE_FIELDS = (
    "data_source",
    "underlying_symbol",
    "contract_symbol",
    "option_type",
    "expiration_date",
    "strike",
    "underlying_price",
    "bid",
    "ask",
)
NUMERIC_FIELDS = (
    "strike",
    "underlying_price",
    "bid",
    "ask",
    "last_trade_price",
    "volume",
    "open_interest",
    "implied_volatility",
)
TIMESTAMP_FIELDS = (
    "option_quote_time",
    "underlying_price_time",
)
@dataclass(frozen=True)
class ValidationFinding:
    """One validation finding emitted during row or file checks."""

    severity: str
    code: str
    message: str
    row_index: int | None = None
    contract_symbol: str | None = None
    field: str | None = None

    def format_for_output(self) -> str:
        """Return a compact human-readable validation line."""
        bits = [self.severity.upper()]
        if self.row_index is not None:
            bits.append(f"row={self.row_index}")
        if self.contract_symbol:
            bits.append(f"contract={self.contract_symbol}")
        bits.append(f"code={self.code}")
        if self.field:
            bits.append(f"field={self.field}")
        bits.append(self.message)
        return " ".join(bits)


def _is_boolean_like(value) -> bool:
    return isinstance(value, (bool, np.bool_))


def _invalid_boolean_mask(df: pd.DataFrame, field: str) -> pd.Series:
    """Return rows where a present boolean field has a non-boolean value."""
    missing = _missing_mask(df, field)
    series = df[field]
    if pd.api.types.is_bool_dtype(series):
        return pd.Series(False, index=df.index)
    return ~missing & ~series.map(_is_boolean_like)


def _missing_mask(df: pd.DataFrame, field: str) -> pd.Series:
    """Return a boolean mask for missing or blank field values."""
    if field not in df.columns:
        return pd.Series(True, index=df.index)
    series = df[field]
    missing = series.isna()
    if series.dtype == object or pd.api.types.is_string_dtype(series):
        missing = missing | series.map(lambda value: isinstance(value, str) and not value.strip())
    return missing


def _numeric_series(df: pd.DataFrame, field: str) -> pd.Series:
    """Return a numeric series for a field, coercing invalid values to NaN."""
    if field not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=float)
    return pd.to_numeric(df[field], errors="coerce")


def _invalid_numeric_mask(df: pd.DataFrame, field: str) -> pd.Series:
    """Return rows where a present numeric field is not finite."""
    numeric = _numeric_series(df, field)
    return ~_missing_mask(df, field) & (numeric.isna() | ~np.isfinite(numeric))


def _contract_symbols(df: pd.DataFrame) -> pd.Series:
    """Return contract symbols aligned to df.index, or None when unavailable."""
    if "contract_symbol" not in df.columns:
        return pd.Series(None, index=df.index, dtype=object)
    return df["contract_symbol"]


def _make_finding(  # pylint: disable=too-many-arguments
    severity, code, message, *, row_index=None, contract_symbol=None, field=None
):
    return ValidationFinding(
        severity=severity,
        code=code,
        message=message,
        row_index=row_index,
        contract_symbol=contract_symbol,
        field=field,
    )


def _append_row_findings(  # pylint: disable=too-many-arguments
    findings: list[ValidationFinding],
    mask: pd.Series,
    contract_symbols: pd.Series,
    *,
    severity: str,
    code: str,
    message: str,
    field: str,
) -> None:
    """Append one finding for each row selected by mask."""
    for row_index in mask[mask].index:
        findings.append(
            _make_finding(
                severity,
                code,
                message,
                row_index=row_index,
                contract_symbol=contract_symbols.loc[row_index],
                field=field,
            )
        )


def validate_option_rows(df: pd.DataFrame) -> list[ValidationFinding]:
    """Validate individual option rows before shared post-download filtering."""
    findings: list[ValidationFinding] = []
    if df.empty:
        return findings

    contract_symbols = _contract_symbols(df)

    for field in REQUIRED_CORE_FIELDS:
        _append_row_findings(
            findings,
            _missing_mask(df, field),
            contract_symbols,
            severity="error",
            code="missing_required_field",
            message=f"Required field '{field}' is empty.",
            field=field,
        )

    if "option_type" in df.columns:
        invalid_option_type = (
            ~_missing_mask(df, "option_type")
            & ~df["option_type"].isin({"call", "put"})
        )
        _append_row_findings(
            findings,
            invalid_option_type,
            contract_symbols,
            severity="error",
            code="invalid_option_type",
            message="option_type must be 'call' or 'put'.",
            field="option_type",
        )

    if "expiration_date" in df.columns:
        parsed_expiration = pd.to_datetime(
            df["expiration_date"],
            format="%Y-%m-%d",
            errors="coerce",
        )
        invalid_expiration = ~_missing_mask(df, "expiration_date") & parsed_expiration.isna()
        _append_row_findings(
            findings,
            invalid_expiration,
            contract_symbols,
            severity="error",
            code="invalid_expiration_date",
            message="expiration_date must parse as YYYY-MM-DD.",
            field="expiration_date",
        )

    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        _append_row_findings(
            findings,
            _invalid_numeric_mask(df, field),
            contract_symbols,
            severity="error",
            code="invalid_numeric_field",
            message=f"Field '{field}' must be a finite number.",
            field=field,
        )

    for field in TIMESTAMP_FIELDS:
        if field not in df.columns:
            continue
        parsed_timestamp = pd.to_datetime(df[field], utc=True, errors="coerce")
        invalid_timestamp = ~_missing_mask(df, field) & parsed_timestamp.isna()
        _append_row_findings(
            findings,
            invalid_timestamp,
            contract_symbols,
            severity="error",
            code="invalid_timestamp_field",
            message=f"Field '{field}' must be a valid timestamp.",
            field=field,
        )

    for field in BOOLEAN_FIELDS:
        if field not in df.columns:
            continue
        invalid_boolean = _invalid_boolean_mask(df, field)
        _append_row_findings(
            findings,
            invalid_boolean,
            contract_symbols,
            severity="warning",
            code="invalid_boolean_field",
            message=f"Field '{field}' should be boolean-like.",
            field=field,
        )

    strike = _numeric_series(df, "strike")
    _append_row_findings(
        findings,
        strike.notna() & (strike <= 0),
        contract_symbols,
        severity="error",
        code="invalid_strike",
        message="strike must be greater than zero.",
        field="strike",
    )

    underlying_price = _numeric_series(df, "underlying_price")
    _append_row_findings(
        findings,
        underlying_price.notna() & (underlying_price <= 0),
        contract_symbols,
        severity="error",
        code="invalid_underlying_price",
        message="underlying_price must be greater than zero.",
        field="underlying_price",
    )

    bid = _numeric_series(df, "bid")
    ask = _numeric_series(df, "ask")
    _append_row_findings(
        findings,
        bid.notna() & (bid < 0),
        contract_symbols,
        severity="error",
        code="invalid_bid",
        message="bid must be non-negative.",
        field="bid",
    )
    _append_row_findings(
        findings,
        ask.notna() & (ask < 0),
        contract_symbols,
        severity="error",
        code="invalid_ask",
        message="ask must be non-negative.",
        field="ask",
    )
    _append_row_findings(
        findings,
        bid.notna() & ask.notna() & (bid > ask),
        contract_symbols,
        severity="error",
        code="invalid_quote_order",
        message="bid must be less than or equal to ask.",
        field="bid_ask",
    )

    return findings


def validate_export_frame(df: pd.DataFrame) -> list[ValidationFinding]:
    """Validate the combined export frame before CSV write."""
    findings: list[ValidationFinding] = []
    if df.empty:
        findings.append(
            _make_finding(
                "warning",
                "empty_export_frame",
                "Combined export frame is empty.",
            )
        )
        return findings

    for field in REQUIRED_CORE_FIELDS:
        if field not in df.columns:
            findings.append(
                _make_finding(
                    "error",
                    "missing_required_column",
                    f"Required column '{field}' is missing from the export frame.",
                    field=field,
                )
            )

    if "data_source" in df.columns and df["data_source"].dropna().nunique() > 1:
        findings.append(
            _make_finding(
                "error",
                "mixed_data_sources",
                "Export frame contains rows from multiple data sources.",
                field="data_source",
            )
        )

    if {"data_source", "contract_symbol"}.issubset(df.columns):
        duplicate_mask = df.duplicated(subset=["data_source", "contract_symbol"], keep=False)
        duplicate_symbols = df.loc[duplicate_mask, "contract_symbol"]
        for row_index, contract_symbol in duplicate_symbols.items():
            findings.append(
                _make_finding(
                    "error",
                    "duplicate_contract_row",
                    "Duplicate contract_symbol detected within the same data source.",
                    row_index=row_index,
                    contract_symbol=contract_symbol,
                    field="contract_symbol",
                )
            )

    return findings


def summarize_validation_findings(findings: list[ValidationFinding]) -> tuple[int, int]:
    """Return warning/error counts for a collection of findings."""
    warnings = sum(1 for finding in findings if finding.severity == "warning")
    errors = sum(1 for finding in findings if finding.severity == "error")
    return warnings, errors


def emit_validation_report(findings: list[ValidationFinding], *, logger=None) -> None:
    """Print and optionally log a validation summary and detailed findings."""
    warnings, errors = summarize_validation_findings(findings)
    print("Validation summary:")
    print(f"  warnings: {warnings}")
    print(f"  errors: {errors}")
    if logger:
        logger.info("validation status=completed warnings=%s errors=%s", warnings, errors)

    if not findings:
        return

    print("Validation findings:")
    for finding in findings:
        line = f"  {finding.format_for_output()}"
        print(line)
        if logger:
            log_method = logger.error if finding.severity == "error" else logger.warning
            log_method("validation_%s %s", finding.severity, finding.format_for_output())
