"""Gate 2 — distribute validated maridb-public outputs to app-specific R2 buckets.

Reads from maridb-public (single source of truth), validates, then copies to
arktrace-public or documaris-public via R2-to-R2 copy.

Failure aborts the copy; the previous version in the app bucket remains live.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

from pipelines.storage.config import (
    ARKTRACE_BUCKET,
    DOCUMARIS_BUCKET,
    _DEFAULT_BUCKET as MARIDB_BUCKET,
    output_uri,
    read_parquet,
    write_parquet,
)

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str]


def _validate(df: pl.DataFrame, required_columns: list[str], min_rows: int = 1) -> ValidationResult:
    errors: list[str] = []
    if len(df) < min_rows:
        errors.append(f"row count {len(df)} < minimum {min_rows}")
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        errors.append(f"missing required columns: {missing}")
    for col in required_columns:
        if col in df.columns and df[col].null_count() > 0:
            errors.append(f"null values in '{col}': {df[col].null_count()}")
    return ValidationResult(ok=len(errors) == 0, errors=errors)


def _read_from_maridb(key: str) -> pl.DataFrame | None:
    """Read a Parquet file from maridb-public by its key path."""
    uri = output_uri(key, bucket=MARIDB_BUCKET)
    return read_parquet(uri)


def _copy_to_bucket(source_key: str, dest_key: str, dest_bucket: str) -> bool:
    """Copy a file from maridb-public to a destination app bucket."""
    df = _read_from_maridb(source_key)
    if df is None:
        logger.error("Source not found in maridb-public: %s", source_key)
        return False
    dest_uri = output_uri(dest_key, bucket=dest_bucket)
    write_parquet(df, dest_uri)
    return True


# ---------------------------------------------------------------------------
# arktrace-public
# ---------------------------------------------------------------------------

def push_arktrace_watchlist(region: str) -> bool:
    """Copy validated watchlist from maridb-public → arktrace-public."""
    source_key = f"score/{region}_watchlist.parquet"
    df = _read_from_maridb(source_key)
    if df is None:
        logger.error("Watchlist not found in maridb-public: %s", source_key)
        return False

    result = _validate(df, required_columns=["mmsi", "confidence"], min_rows=1)
    if not result.ok:
        logger.warning("Watchlist (%s) failed Gate 2 — skipping: %s", region, result.errors)
        return True

    dest_key = f"watchlist/{region}_watchlist.parquet"
    ok = _copy_to_bucket(source_key, dest_key, ARKTRACE_BUCKET)
    if ok:
        logger.info("Pushed %s watchlist (%d rows) → arktrace-public/%s", region, len(df), dest_key)
    return ok


def push_arktrace_vessel_features() -> bool:
    """Copy validated vessel feature matrix from maridb-public → arktrace-public."""
    source_key = "features/vessel_features.parquet"
    df = _read_from_maridb(source_key)
    if df is None:
        logger.warning("vessel_features not in maridb-public — skipping (not yet implemented)")
        return True

    required = ["mmsi", "ais_gap_count_30d", "loitering_hours_30d", "sanctions_distance"]
    result = _validate(df, required_columns=required)
    if not result.ok:
        logger.error("Gate 2 failed for vessel_features: %s", result.errors)
        return False

    ok = _copy_to_bucket(source_key, "features/vessel_features.parquet", ARKTRACE_BUCKET)
    if ok:
        logger.info("Pushed vessel features (%d rows) → arktrace-public", len(df))
    return ok


def push_arktrace_ais_summaries() -> bool:
    """Copy validated AIS summaries from maridb-public → arktrace-public."""
    source_key = "ais-summaries/latest.parquet"
    df = _read_from_maridb(source_key)
    if df is None:
        logger.warning("ais-summaries not in maridb-public — skipping (not yet implemented)")
        return True

    result = _validate(df, required_columns=["vessel_id", "date", "positions_count"])
    if not result.ok:
        logger.error("Gate 2 failed for ais-summaries: %s", result.errors)
        return False

    ok = _copy_to_bucket(source_key, "ais-summaries/latest.parquet", ARKTRACE_BUCKET)
    if ok:
        logger.info("Pushed AIS summaries (%d rows) → arktrace-public", len(df))
    return ok


# ---------------------------------------------------------------------------
# documaris-public
# ---------------------------------------------------------------------------

def push_documaris_voyage_evidence() -> bool:
    """Copy validated voyage evidence from maridb-public → documaris-public."""
    source_key = "voyage-evidence/latest.parquet"
    df = _read_from_maridb(source_key)
    if df is None:
        logger.warning("voyage-evidence not in maridb-public — skipping (not yet implemented)")
        return True

    required = ["vessel_id", "voyage_id", "track_start_utc", "track_end_utc", "positions_count"]
    result = _validate(df, required_columns=required)
    if not result.ok:
        logger.error("Gate 2 failed for voyage-evidence: %s", result.errors)
        return False

    ok = _copy_to_bucket(source_key, "voyage-evidence/latest.parquet", DOCUMARIS_BUCKET)
    if ok:
        logger.info("Pushed voyage evidence (%d rows) → documaris-public", len(df))
    return ok


# ---------------------------------------------------------------------------
# Full distribute run
# ---------------------------------------------------------------------------

def distribute_all(regions: list[str] | None = None) -> dict[str, bool]:
    """Run all Gate 2 distribution steps. Returns {step: ok} map."""
    regions = regions or ["singapore", "japansea"]
    results: dict[str, bool] = {}

    for region in regions:
        results[f"watchlist_{region}"] = push_arktrace_watchlist(region)

    results["vessel_features"] = push_arktrace_vessel_features()
    results["ais_summaries"] = push_arktrace_ais_summaries()
    results["voyage_evidence"] = push_documaris_voyage_evidence()

    failed = [k for k, v in results.items() if not v]
    if failed:
        logger.error("Distribute completed with failures: %s", failed)
    else:
        logger.info("Distribute completed successfully")

    return results
