"""Pandera DataFrame schemas for pipeline output validation.

Each schema defines the expected columns, types, and constraints for a
pipeline output. Call ``Schema.validate(df)`` to raise ``SchemaError`` on
violation, or ``Schema.validate(df, lazy=True)`` to collect all errors.

Usage::

    from pipelines.storage.schemas import WatchlistSchema
    WatchlistSchema.validate(watchlist_df)
"""

from __future__ import annotations

import pandera.polars as pa
from pandera.typing.polars import Series


class WatchlistSchema(pa.DataFrameModel):
    """Gate 1 + Gate 2 schema for region watchlist Parquet files."""

    mmsi: Series[str] = pa.Field(nullable=False, description="Maritime Mobile Service Identity")
    composite_score: Series[float] = pa.Field(
        ge=0.0, le=1.0, nullable=False,
        description="Alias for confidence; used by Gate 2 validation",
    )
    confidence: Series[float] = pa.Field(
        ge=0.0, le=1.0, nullable=False,
        description="Primary composite confidence score",
    )

    class Config:
        coerce = True


class VesselFeaturesSchema(pa.DataFrameModel):
    """Schema for vessel feature matrix distributed to arktrace-public."""

    mmsi: Series[str] = pa.Field(nullable=False)
    ais_gap_count_30d: Series[int] = pa.Field(ge=0, nullable=False)
    loitering_hours_30d: Series[float] = pa.Field(ge=0.0, nullable=False)
    sanctions_distance: Series[int] = pa.Field(ge=0, nullable=False)

    class Config:
        coerce = True


class AisSummariesSchema(pa.DataFrameModel):
    """Schema for AIS position summary distributed to arktrace-public."""

    vessel_id: Series[str] = pa.Field(nullable=False)
    date: Series[str] = pa.Field(nullable=False)
    positions_count: Series[int] = pa.Field(ge=0, nullable=False)

    class Config:
        coerce = True


class VoyageEvidenceSchema(pa.DataFrameModel):
    """Schema for voyage evidence distributed to documaris-public."""

    vessel_id: Series[str] = pa.Field(nullable=False)
    voyage_id: Series[str] = pa.Field(nullable=False)
    track_start_utc: Series[str] = pa.Field(nullable=False)
    track_end_utc: Series[str] = pa.Field(nullable=False)
    positions_count: Series[int] = pa.Field(ge=0, nullable=False)

    class Config:
        coerce = True


class SanctionsEntitiesSchema(pa.DataFrameModel):
    """Schema for the sanctions entities ingest table."""

    entity_id: Series[str] = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=True)
    mmsi: Series[str] = pa.Field(nullable=True)
    imo: Series[str] = pa.Field(nullable=True)
    flag: Series[str] = pa.Field(nullable=True)
    type: Series[str] = pa.Field(nullable=True)
    list_source: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
