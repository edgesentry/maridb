"""Gate 1 validation — run before writing any output to maridb-public.

Each pipeline step calls validate_output() before uploading. Failure raises
PipelineValidationError; the upload is aborted and the previous version in
maridb-public remains live.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl


class PipelineValidationError(Exception):
    pass


@dataclass
class ValidationSpec:
    required_columns: list[str]
    min_rows: int = 1
    no_nulls_in: list[str] = field(default_factory=list)


# Per-output validation specs
SPECS: dict[str, ValidationSpec] = {
    "vessel_features": ValidationSpec(
        required_columns=["mmsi", "ais_gap_count_30d", "loitering_hours_30d", "sanctions_distance"],
        min_rows=1,
        no_nulls_in=["mmsi"],
    ),
    "watchlist": ValidationSpec(
        required_columns=["mmsi", "confidence"],
        min_rows=1,
        no_nulls_in=["mmsi", "confidence"],
    ),
    "ais_positions": ValidationSpec(
        required_columns=["mmsi", "timestamp", "lat", "lon"],
        min_rows=1,
        no_nulls_in=["mmsi", "timestamp", "lat", "lon"],
    ),
    "ais_summaries": ValidationSpec(
        required_columns=["vessel_id", "date", "positions_count"],
        min_rows=1,
        no_nulls_in=["vessel_id"],
    ),
    "voyage_evidence": ValidationSpec(
        required_columns=["vessel_id", "voyage_id", "track_start_utc", "track_end_utc", "positions_count"],
        min_rows=1,
        no_nulls_in=["vessel_id", "voyage_id"],
    ),
}


def validate_output(df: pl.DataFrame, output_type: str) -> None:
    """Validate pipeline output before writing to maridb-public.

    Raises PipelineValidationError with a descriptive message on any failure.
    """
    spec = SPECS.get(output_type)
    if spec is None:
        raise PipelineValidationError(f"No validation spec defined for output_type='{output_type}'")

    errors: list[str] = []

    # Row count
    if len(df) < spec.min_rows:
        errors.append(f"row count {len(df)} < minimum {spec.min_rows}")

    # Required columns present
    missing_cols = [c for c in spec.required_columns if c not in df.columns]
    if missing_cols:
        errors.append(f"missing required columns: {missing_cols}")

    # No nulls in critical columns
    for col in spec.no_nulls_in:
        if col in df.columns and df[col].null_count() > 0:
            errors.append(f"null values in required column '{col}': {df[col].null_count()} nulls")

    if errors:
        raise PipelineValidationError(
            f"Gate 1 validation failed for '{output_type}':\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
