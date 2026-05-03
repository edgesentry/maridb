"""Tests for GEBCO 200m bathymetry depth filter (issue #269).

Covers:
- compute_sts_candidates with / without deep_cells mask
- _load_deep_cells auto-detection from db_path
- build_sts_contacts_from_ais depth filtering
- sync_r2 push-gebco-masks / pull-gebco-masks
"""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import polars as pl

from pipelines.features.ais_behavior import (
    _load_deep_cells,
    compute_sts_candidates,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_STOPPED = 1  # "at anchor" nav_status — in STOPPED_STATUSES


def _make_ais_df(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal AIS DataFrame for STS testing."""
    return pl.DataFrame(
        rows,
        schema={
            "mmsi": pl.Utf8,
            "timestamp": pl.Datetime("us", "UTC"),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "sog": pl.Float32,
            "nav_status": pl.Int32,
        },
    )


def _ts(hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=UTC)


def _h3_cell(lat: float, lon: float) -> str:
    import h3

    try:
        return h3.latlng_to_cell(lat, lon, 8)
    except AttributeError:
        return h3.geo_to_h3(lat, lon, 8)


# ---------------------------------------------------------------------------
# compute_sts_candidates — no mask (current behavior unchanged)
# ---------------------------------------------------------------------------


def test_sts_candidates_no_mask_detects_colocation():
    """Two stopped vessels in same H3 cell+bucket → both get sts_candidate_count=1."""
    cell_lat, cell_lon = 1.0, 103.8  # Singapore Strait (shallow)
    df = _make_ais_df(
        [
            {
                "mmsi": "A",
                "timestamp": _ts(0, 0),
                "lat": cell_lat,
                "lon": cell_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
            {
                "mmsi": "B",
                "timestamp": _ts(0, 5),
                "lat": cell_lat,
                "lon": cell_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
        ]
    )
    result = compute_sts_candidates(df)
    assert set(result["mmsi"].to_list()) == {"A", "B"}
    assert all(result["sts_candidate_count"] >= 1)


def test_sts_candidates_no_mask_returns_empty_for_single_vessel():
    """Single vessel never co-locates → empty result."""
    df = _make_ais_df(
        [
            {
                "mmsi": "A",
                "timestamp": _ts(0),
                "lat": 1.0,
                "lon": 103.8,
                "sog": 0.0,
                "nav_status": _STOPPED,
            }
        ]
    )
    result = compute_sts_candidates(df)
    assert result.is_empty()


# ---------------------------------------------------------------------------
# compute_sts_candidates — with deep_cells mask
# ---------------------------------------------------------------------------


def test_sts_candidates_mask_filters_shallow_colocation():
    """Co-location in a shallow-water cell is excluded when mask is applied."""
    shallow_lat, shallow_lon = 1.0, 103.8  # Singapore Strait
    deep_lat, deep_lon = 5.0, 104.5  # Deep South China Sea

    deep_cell = _h3_cell(deep_lat, deep_lon)

    # Only the deep cell is in the mask; shallow cell is excluded
    deep_cells = frozenset([deep_cell])

    df = _make_ais_df(
        [
            {
                "mmsi": "A",
                "timestamp": _ts(0, 0),
                "lat": shallow_lat,
                "lon": shallow_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
            {
                "mmsi": "B",
                "timestamp": _ts(0, 5),
                "lat": shallow_lat,
                "lon": shallow_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
        ]
    )
    result = compute_sts_candidates(df, deep_cells=deep_cells)
    assert result.is_empty()


def test_sts_candidates_mask_keeps_deep_colocation():
    """Co-location in a deep-water cell passes through the mask."""
    deep_lat, deep_lon = 5.0, 104.5
    deep_cell = _h3_cell(deep_lat, deep_lon)
    deep_cells = frozenset([deep_cell])

    df = _make_ais_df(
        [
            {
                "mmsi": "A",
                "timestamp": _ts(0, 0),
                "lat": deep_lat,
                "lon": deep_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
            {
                "mmsi": "B",
                "timestamp": _ts(0, 5),
                "lat": deep_lat,
                "lon": deep_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
        ]
    )
    result = compute_sts_candidates(df, deep_cells=deep_cells)
    assert set(result["mmsi"].to_list()) == {"A", "B"}


def test_sts_candidates_mask_none_behaves_same_as_no_mask():
    """Passing deep_cells=None is identical to omitting it."""
    cell_lat, cell_lon = 1.0, 103.8
    df = _make_ais_df(
        [
            {
                "mmsi": "A",
                "timestamp": _ts(0, 0),
                "lat": cell_lat,
                "lon": cell_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
            {
                "mmsi": "B",
                "timestamp": _ts(0, 5),
                "lat": cell_lat,
                "lon": cell_lon,
                "sog": 0.0,
                "nav_status": _STOPPED,
            },
        ]
    )
    result_explicit = compute_sts_candidates(df, deep_cells=None)
    result_default = compute_sts_candidates(df)
    assert result_explicit.shape == result_default.shape


# ---------------------------------------------------------------------------
# _load_deep_cells
# ---------------------------------------------------------------------------


def test_load_deep_cells_returns_none_when_no_parquet(tmp_path):
    """Returns None when no mask file exists alongside the DB."""
    db_path = str(tmp_path / "singapore.duckdb")
    assert _load_deep_cells(db_path) is None


def test_load_deep_cells_loads_parquet(tmp_path):
    """Loads the frozenset from a parquet file named {stem}_deep_cells.parquet."""
    cells = ["8830b0c0c7fffff", "8830b0c0c7ffffe"]
    pl.DataFrame({"h3_cell": cells}).write_parquet(tmp_path / "singapore_deep_cells.parquet")

    result = _load_deep_cells(str(tmp_path / "singapore.duckdb"))
    assert result == frozenset(cells)


# ---------------------------------------------------------------------------
# build_sts_contacts_from_ais — depth filtering
# ---------------------------------------------------------------------------


def test_build_sts_contacts_filters_shallow_cells(tmp_path):
    """STS contacts in shallow-water cells are excluded when a mask exists."""
    from pipelines.ingest.vessel_registry import build_sts_contacts_from_ais

    db_path = str(tmp_path / "singapore.duckdb")
    shallow_lat, shallow_lon = 1.0, 103.8

    # Mask contains only a deep cell, so shallow co-location is excluded
    deep_cell = _h3_cell(5.0, 104.5)
    pl.DataFrame({"h3_cell": [deep_cell]}).write_parquet(tmp_path / "singapore_deep_cells.parquet")

    # Create DB with two vessels co-located in the shallow cell
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE ais_positions (mmsi VARCHAR, timestamp TIMESTAMPTZ, lat DOUBLE, lon DOUBLE)"
    )
    for mmsi in ("A", "B"):
        for minute in (0, 5, 35, 40):  # two 30-min buckets → 2 co-locations
            con.execute(
                "INSERT INTO ais_positions VALUES (?, ?, ?, ?)",
                [mmsi, f"2026-01-01 00:{minute:02d}:00+00:00", shallow_lat, shallow_lon],
            )
    con.close()

    contacts = build_sts_contacts_from_ais(db_path)
    # All positions are in the shallow cell which is NOT in the mask → no contacts
    assert contacts == []


def test_build_sts_contacts_no_mask_returns_contacts(tmp_path):
    """Without a mask file, contacts are returned as before."""
    from pipelines.ingest.vessel_registry import build_sts_contacts_from_ais

    db_path = str(tmp_path / "singapore.duckdb")
    lat, lon = 1.0, 103.8

    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE ais_positions (mmsi VARCHAR, timestamp TIMESTAMPTZ, lat DOUBLE, lon DOUBLE)"
    )
    for mmsi in ("A", "B"):
        for minute in (0, 5):
            con.execute(
                "INSERT INTO ais_positions VALUES (?, ?, ?, ?)",
                [mmsi, f"2026-01-01 00:{minute:02d}:00+00:00", lat, lon],
            )
    con.close()

    contacts = build_sts_contacts_from_ais(db_path)
    assert len(contacts) == 1
    assert {contacts[0]["src_id"], contacts[0]["dst_id"]} == {"A", "B"}


# ---------------------------------------------------------------------------
