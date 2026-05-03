"""Tests for Pandera DataFrame schemas in pipelines/storage/schemas.py."""

from __future__ import annotations

import pandera.errors
import polars as pl
import pytest

from pipelines.storage.schemas import (
    AisSummariesSchema,
    SanctionsEntitiesSchema,
    VesselFeaturesSchema,
    VoyageEvidenceSchema,
    WatchlistSchema,
)

# ---------------------------------------------------------------------------
# WatchlistSchema
# ---------------------------------------------------------------------------


class TestWatchlistSchema:
    def test_valid_watchlist(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789", "987654321"],
                "composite_score": [0.85, 0.42],
                "confidence": [0.85, 0.42],
            }
        )
        WatchlistSchema.validate(df)

    def test_rejects_missing_mmsi(self):
        df = pl.DataFrame(
            {
                "composite_score": [0.85],
                "confidence": [0.85],
            }
        )
        with pytest.raises(Exception):
            WatchlistSchema.validate(df)

    def test_rejects_null_mmsi(self):
        df = pl.DataFrame(
            {
                "mmsi": [None],
                "composite_score": [0.85],
                "confidence": [0.85],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            WatchlistSchema.validate(df)

    def test_rejects_score_out_of_range(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789"],
                "composite_score": [1.5],
                "confidence": [1.5],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            WatchlistSchema.validate(df)

    def test_rejects_negative_score(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789"],
                "composite_score": [-0.1],
                "confidence": [-0.1],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            WatchlistSchema.validate(df)

    def test_boundary_scores_valid(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789", "987654321"],
                "composite_score": [0.0, 1.0],
                "confidence": [0.0, 1.0],
            }
        )
        WatchlistSchema.validate(df)


# ---------------------------------------------------------------------------
# VesselFeaturesSchema
# ---------------------------------------------------------------------------


class TestVesselFeaturesSchema:
    def test_valid_features(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789"],
                "ais_gap_count_30d": [5],
                "loitering_hours_30d": [12.5],
                "sanctions_distance": [2],
            }
        )
        VesselFeaturesSchema.validate(df)

    def test_rejects_null_mmsi(self):
        df = pl.DataFrame(
            {
                "mmsi": [None],
                "ais_gap_count_30d": [5],
                "loitering_hours_30d": [12.5],
                "sanctions_distance": [2],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            VesselFeaturesSchema.validate(df)

    def test_rejects_negative_gap_count(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789"],
                "ais_gap_count_30d": [-1],
                "loitering_hours_30d": [0.0],
                "sanctions_distance": [0],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            VesselFeaturesSchema.validate(df)

    def test_rejects_negative_loitering(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789"],
                "ais_gap_count_30d": [0],
                "loitering_hours_30d": [-1.0],
                "sanctions_distance": [0],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            VesselFeaturesSchema.validate(df)


# ---------------------------------------------------------------------------
# AisSummariesSchema
# ---------------------------------------------------------------------------


class TestAisSummariesSchema:
    def test_valid_summaries(self):
        df = pl.DataFrame(
            {
                "vessel_id": ["IMO9876543"],
                "date": ["2026-04-25"],
                "positions_count": [1842],
            }
        )
        AisSummariesSchema.validate(df)

    def test_rejects_null_vessel_id(self):
        df = pl.DataFrame(
            {
                "vessel_id": [None],
                "date": ["2026-04-25"],
                "positions_count": [100],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            AisSummariesSchema.validate(df)

    def test_rejects_negative_positions(self):
        df = pl.DataFrame(
            {
                "vessel_id": ["IMO9876543"],
                "date": ["2026-04-25"],
                "positions_count": [-1],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            AisSummariesSchema.validate(df)


# ---------------------------------------------------------------------------
# VoyageEvidenceSchema
# ---------------------------------------------------------------------------


class TestVoyageEvidenceSchema:
    def test_valid_evidence(self):
        df = pl.DataFrame(
            {
                "vessel_id": ["IMO9876543"],
                "voyage_id": ["V001"],
                "track_start_utc": ["2026-04-23T10:00:00Z"],
                "track_end_utc": ["2026-04-28T05:55:00Z"],
                "positions_count": [1842],
            }
        )
        VoyageEvidenceSchema.validate(df)

    def test_rejects_null_voyage_id(self):
        df = pl.DataFrame(
            {
                "vessel_id": ["IMO9876543"],
                "voyage_id": [None],
                "track_start_utc": ["2026-04-23T10:00:00Z"],
                "track_end_utc": ["2026-04-28T05:55:00Z"],
                "positions_count": [100],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            VoyageEvidenceSchema.validate(df)

    def test_rejects_missing_column(self):
        df = pl.DataFrame(
            {
                "vessel_id": ["IMO9876543"],
                "voyage_id": ["V001"],
            }
        )
        with pytest.raises(Exception):
            VoyageEvidenceSchema.validate(df)


# ---------------------------------------------------------------------------
# SanctionsEntitiesSchema
# ---------------------------------------------------------------------------


class TestSanctionsEntitiesSchema:
    def test_valid_entities(self):
        df = pl.DataFrame(
            {
                "entity_id": ["ofac-001"],
                "name": ["VESSEL NAME"],
                "mmsi": ["123456789"],
                "imo": ["9876543"],
                "flag": ["IR"],
                "type": ["vessel"],
                "list_source": ["OFAC"],
            }
        )
        SanctionsEntitiesSchema.validate(df)

    def test_allows_nullable_mmsi(self):
        df = pl.DataFrame(
            {
                "entity_id": ["ofac-001"],
                "name": ["COMPANY NAME"],
                "mmsi": [None],
                "imo": [None],
                "flag": [None],
                "type": ["organization"],
                "list_source": ["EU"],
            }
        )
        SanctionsEntitiesSchema.validate(df)

    def test_rejects_null_entity_id(self):
        df = pl.DataFrame(
            {
                "entity_id": [None],
                "name": ["NAME"],
                "mmsi": [None],
                "imo": [None],
                "flag": [None],
                "type": ["vessel"],
                "list_source": ["OFAC"],
            }
        )
        with pytest.raises(pandera.errors.SchemaError):
            SanctionsEntitiesSchema.validate(df)
