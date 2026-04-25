"""Distribute validated maridb outputs to app-specific public R2 buckets.

After maridb pipelines write to maridb-public, this module validates each
output and pushes derived data to the app-specific buckets:

  arktrace-public  — AIS event summaries, vessel feature matrix, watchlist inputs
  documaris-public — Regulatory KB updates, voyage evidence Parquet, form pre-fill data

Validation failures halt the push; the previous version in the target bucket
remains live.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

from pipelines.storage.config import (
    ARKTRACE_BUCKET,
    DOCUMARIS_BUCKET,
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
        errors.append(f"Row count {len(df)} < minimum {min_rows}")
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        errors.append(f"Missing columns: {missing}")
    null_counts = {c: df[c].null_count() for c in required_columns if c in df.columns}
    nulls = {c: n for c, n in null_counts.items() if n > 0}
    if nulls:
        errors.append(f"Null values in required columns: {nulls}")
    return ValidationResult(ok=len(errors) == 0, errors=errors)


def push_arktrace_ais_summaries(source_path: str) -> bool:
    """Push validated AIS event summaries to arktrace-public."""
    df = read_parquet(source_path)
    if df is None:
        logger.error("Source not found: %s", source_path)
        return False

    result = _validate(df, required_columns=["vessel_id", "date", "positions_count"])
    if not result.ok:
        logger.error("Validation failed for AIS summaries: %s", result.errors)
        return False

    dest = output_uri("ais-summaries/latest.parquet", bucket=ARKTRACE_BUCKET)
    write_parquet(df, dest)
    logger.info("Pushed AIS summaries (%d rows) → %s", len(df), dest)
    return True


def push_arktrace_vessel_features(source_path: str) -> bool:
    """Push validated vessel feature matrix to arktrace-public."""
    df = read_parquet(source_path)
    if df is None:
        logger.error("Source not found: %s", source_path)
        return False

    required = ["mmsi", "ais_gap_count_30d", "loitering_hours_30d", "sanctions_distance"]
    result = _validate(df, required_columns=required)
    if not result.ok:
        logger.error("Validation failed for vessel features: %s", result.errors)
        return False

    dest = output_uri("features/vessel_features.parquet", bucket=ARKTRACE_BUCKET)
    write_parquet(df, dest)
    logger.info("Pushed vessel features (%d rows) → %s", len(df), dest)
    return True


def push_documaris_voyage_evidence(source_path: str) -> bool:
    """Push validated voyage evidence Parquet to documaris-public."""
    df = read_parquet(source_path)
    if df is None:
        logger.error("Source not found: %s", source_path)
        return False

    required = ["vessel_id", "voyage_id", "track_start_utc", "track_end_utc", "positions_count"]
    result = _validate(df, required_columns=required)
    if not result.ok:
        logger.error("Validation failed for voyage evidence: %s", result.errors)
        return False

    dest = output_uri("voyage-evidence/latest.parquet", bucket=DOCUMARIS_BUCKET)
    write_parquet(df, dest)
    logger.info("Pushed voyage evidence (%d rows) → %s", len(df), dest)
    return True
