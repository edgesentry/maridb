"""Tests for validate_lead_time_ofac.py and print_lead_time_report.py."""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

# Import the module functions directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.validate_lead_time_ofac import (
    _load_designation_dates,
    _load_watchlist,
    _prospective,
    _retrospective,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 5, 6, 0, 0, 0, tzinfo=UTC)
_LAST_SEEN_PRE = _NOW - timedelta(days=5)  # last seen 5 days ago → window_start = 35 days ago → lead = 25d
_LAST_SEEN_POST = _NOW + timedelta(days=0) - timedelta(days=200)  # last seen 200 days ago → negative lead


def _make_watchlist(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    return path


def _make_sanctions_jsonl(tmp_path: Path, entries: list[dict]) -> Path:
    p = tmp_path / "opensanctions_entities.jsonl"
    with p.open("w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")
    return p


# ---------------------------------------------------------------------------
# _load_designation_dates
# ---------------------------------------------------------------------------


def test_load_designation_dates_basic(tmp_path):
    jsonl = _make_sanctions_jsonl(tmp_path, [
        {
            "first_seen": "2026-04-15T00:00:00Z",
            "properties": {"mmsi": ["123456789"]},
        },
        {
            "first_seen": "2026-03-01T00:00:00Z",
            "properties": {"mmsi": ["987654321"]},
        },
    ])
    dates = _load_designation_dates(jsonl)
    assert "123456789" in dates
    assert "987654321" in dates
    assert dates["123456789"].year == 2026
    assert dates["123456789"].month == 4


def test_load_designation_dates_keeps_earliest(tmp_path):
    """When a vessel appears twice, keep the earlier date."""
    jsonl = _make_sanctions_jsonl(tmp_path, [
        {"first_seen": "2026-04-15T00:00:00Z", "properties": {"mmsi": ["111"]}},
        {"first_seen": "2025-01-10T00:00:00Z", "properties": {"mmsi": ["111"]}},
    ])
    dates = _load_designation_dates(jsonl)
    assert dates["111"].year == 2025


def test_load_designation_dates_missing_file(tmp_path):
    dates = _load_designation_dates(tmp_path / "nonexistent.jsonl")
    assert dates == {}


def test_load_designation_dates_skips_no_mmsi(tmp_path):
    jsonl = _make_sanctions_jsonl(tmp_path, [
        {"first_seen": "2026-04-15T00:00:00Z", "properties": {}},
        {"first_seen": "2026-04-15T00:00:00Z", "properties": {"mmsi": []}},
    ])
    dates = _load_designation_dates(jsonl)
    assert len(dates) == 0


def test_load_designation_dates_skips_bad_date(tmp_path):
    jsonl = _make_sanctions_jsonl(tmp_path, [
        {"first_seen": "not-a-date", "properties": {"mmsi": ["111"]}},
        {"first_seen": "2026-04-15T00:00:00Z", "properties": {"mmsi": ["222"]}},
    ])
    dates = _load_designation_dates(jsonl)
    assert "111" not in dates
    assert "222" in dates


# ---------------------------------------------------------------------------
# _load_watchlist
# ---------------------------------------------------------------------------


def test_load_watchlist_adds_rank(tmp_path):
    p = _make_watchlist(tmp_path / "watchlist.parquet", [
        {"mmsi": "aaa", "confidence": 0.9},
        {"mmsi": "bbb", "confidence": 0.5},
        {"mmsi": "ccc", "confidence": 0.1},
    ])
    df = _load_watchlist([p])
    assert "watchlist_rank" in df.columns
    # Highest confidence vessel should have rank 1
    top = df.sort("watchlist_rank").row(0, named=True)
    assert top["mmsi"] == "aaa"
    assert top["watchlist_rank"] == 1


def test_load_watchlist_deduplicates_by_mmsi(tmp_path):
    p = _make_watchlist(tmp_path / "watchlist.parquet", [
        {"mmsi": "aaa", "confidence": 0.9},
        {"mmsi": "aaa", "confidence": 0.5},
        {"mmsi": "bbb", "confidence": 0.3},
    ])
    df = _load_watchlist([p])
    assert df.height == 2
    row = df.filter(pl.col("mmsi") == "aaa").row(0, named=True)
    assert row["confidence"] == pytest.approx(0.9)


def test_load_watchlist_missing_path(tmp_path):
    df = _load_watchlist([tmp_path / "missing.parquet"])
    assert df.is_empty()


def test_load_watchlist_multiple_files(tmp_path):
    p1 = _make_watchlist(tmp_path / "a.parquet", [{"mmsi": "aaa", "confidence": 0.9}])
    p2 = _make_watchlist(tmp_path / "b.parquet", [{"mmsi": "bbb", "confidence": 0.5}])
    df = _load_watchlist([p1, p2])
    assert df.height == 2


# ---------------------------------------------------------------------------
# _retrospective
# ---------------------------------------------------------------------------


def _make_retro_watchlist(last_seen: datetime) -> pl.DataFrame:
    return pl.DataFrame({
        "mmsi": ["111111111"],
        "vessel_name": ["TEST VESSEL"],
        "flag": ["PA"],
        "watchlist_rank": [42],
        "confidence": [0.75],
        "behavioral_deviation_score": [0.6],
        "graph_risk_score": [0.5],
        "ais_gap_count_30d": [3],
        "sanctions_distance": [0],
        "last_seen": [last_seen],
    })


def test_retrospective_pre_designation(tmp_path):
    """Vessel whose detection window starts before designation → pre_designation=True."""
    # last_seen 5 days ago → window_start = last_seen - 30d = 35 days ago
    # designation = 10 days ago → lead = desig - window_start = 25 days → positive
    last_seen = _NOW - timedelta(days=5)
    desig_date = _NOW - timedelta(days=10)
    wl = _make_retro_watchlist(last_seen)
    designation_dates = {"111111111": desig_date}
    rows = _retrospective(wl, designation_dates, _NOW)
    assert len(rows) == 1
    assert rows[0]["pre_designation"] is True
    assert rows[0]["lead_days"] > 0
    assert rows[0]["watchlist_rank"] == 42


def test_retrospective_post_designation():
    """Vessel whose detection window starts after designation → pre_designation=False."""
    # last_seen = yesterday → window_start = yesterday - 30d = 31 days ago
    # designation = 100 days ago → lead = -69 days → negative
    last_seen = _NOW - timedelta(days=1)
    desig_date = _NOW - timedelta(days=100)
    wl = _make_retro_watchlist(last_seen)
    designation_dates = {"111111111": desig_date}
    rows = _retrospective(wl, designation_dates, _NOW)
    assert len(rows) == 1
    assert rows[0]["pre_designation"] is False
    assert rows[0]["lead_days"] < 0


def test_retrospective_skips_below_threshold():
    """Vessels below CONFIDENCE_THRESHOLD are excluded."""
    wl = pl.DataFrame({
        "mmsi": ["111111111"],
        "vessel_name": ["X"],
        "flag": [""],
        "watchlist_rank": [1],
        "confidence": [0.10],  # below default 0.25
        "behavioral_deviation_score": [0.0],
        "graph_risk_score": [0.0],
        "ais_gap_count_30d": [0],
        "sanctions_distance": [0],
        "last_seen": [_NOW - timedelta(days=5)],
    })
    rows = _retrospective(wl, {"111111111": _NOW - timedelta(days=10)}, _NOW)
    assert rows == []


def test_retrospective_skips_no_designation():
    """Vessels without a designation date are excluded."""
    wl = _make_retro_watchlist(_NOW - timedelta(days=5))
    rows = _retrospective(wl, {}, _NOW)
    assert rows == []


def test_retrospective_sorted_by_lead_days_descending():
    """Results sorted with highest lead time first."""
    wl = pl.DataFrame({
        "mmsi": ["aaa", "bbb"],
        "vessel_name": ["A", "B"],
        "flag": ["", ""],
        "watchlist_rank": [1, 2],
        "confidence": [0.6, 0.5],
        "behavioral_deviation_score": [0.5, 0.4],
        "graph_risk_score": [0.4, 0.3],
        "ais_gap_count_30d": [1, 2],
        "sanctions_distance": [0, 0],
        "last_seen": [
            _NOW - timedelta(days=5),   # window_start = 35d ago
            _NOW - timedelta(days=1),   # window_start = 31d ago
        ],
    })
    designation_dates = {
        "aaa": _NOW - timedelta(days=10),  # lead = 25d
        "bbb": _NOW - timedelta(days=10),  # lead = 21d
    }
    rows = _retrospective(wl, designation_dates, _NOW)
    assert rows[0]["mmsi"] == "aaa"
    assert rows[0]["lead_days"] > rows[1]["lead_days"]


# ---------------------------------------------------------------------------
# _prospective
# ---------------------------------------------------------------------------


def test_prospective_returns_high_score_no_sanctions():
    wl = pl.DataFrame({
        "mmsi": ["999888777"],
        "vessel_name": ["SUSPECT"],
        "flag": ["PA"],
        "confidence": [0.55],
        "ais_gap_count_30d": [5],
        "sanctions_distance": [99],
    })
    rows = _prospective(wl, {})
    assert len(rows) == 1
    assert rows[0]["mmsi"] == "999888777"


def test_prospective_excludes_already_designated():
    wl = pl.DataFrame({
        "mmsi": ["111111111"],
        "vessel_name": ["DESIGNATED"],
        "flag": [""],
        "confidence": [0.55],
        "ais_gap_count_30d": [5],
        "sanctions_distance": [99],
    })
    designation_dates = {"111111111": _NOW - timedelta(days=30)}
    rows = _prospective(wl, designation_dates)
    assert rows == []


def test_prospective_excludes_low_confidence():
    wl = pl.DataFrame({
        "mmsi": ["222222222"],
        "vessel_name": ["LOW"],
        "flag": [""],
        "confidence": [0.20],  # below UU_CONFIDENCE_THRESHOLD=0.30
        "ais_gap_count_30d": [5],
        "sanctions_distance": [99],
    })
    rows = _prospective(wl, {})
    assert rows == []


def test_prospective_excludes_sanctioned_vessel():
    """sanctions_distance != 99 → not an unknown-unknown."""
    wl = pl.DataFrame({
        "mmsi": ["333333333"],
        "vessel_name": ["SANCTIONED"],
        "flag": [""],
        "confidence": [0.55],
        "ais_gap_count_30d": [5],
        "sanctions_distance": [1],  # has a graph link
    })
    rows = _prospective(wl, {})
    assert rows == []


def test_prospective_sorted_by_confidence():
    wl = pl.DataFrame({
        "mmsi": ["aaa", "bbb"],
        "vessel_name": ["A", "B"],
        "flag": ["", ""],
        "confidence": [0.40, 0.70],
        "ais_gap_count_30d": [1, 2],
        "sanctions_distance": [99, 99],
    })
    rows = _prospective(wl, {})
    assert rows[0]["confidence"] > rows[1]["confidence"]


# ---------------------------------------------------------------------------
# CLI smoke tests
# ---------------------------------------------------------------------------


def test_validate_cli_empty_watchlist(tmp_path):
    """Script exits cleanly and writes empty report when watchlist has no data."""
    jsonl = _make_sanctions_jsonl(tmp_path, [
        {"first_seen": "2026-04-15T00:00:00Z", "properties": {"mmsi": ["111"]}},
    ])
    missing = tmp_path / "missing.parquet"
    out = tmp_path / "report.json"
    result = subprocess.run(
        [sys.executable, "scripts/validate_lead_time_ofac.py",
         "--watchlist", str(missing),
         "--jsonl", str(jsonl),
         "--out", str(out)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0
    assert out.exists()
    report = json.loads(out.read_text())
    assert report["watchlist_size"] == 0


def test_validate_cli_produces_json_report(tmp_path):
    """Script produces valid JSON report with expected keys."""
    last_seen = (_NOW - timedelta(days=5)).isoformat().replace("+00:00", "Z")
    wl = pl.DataFrame({
        "mmsi": ["123456789"],
        "vessel_name": ["VESSEL A"],
        "flag": ["PA"],
        "confidence": [0.60],
        "behavioral_deviation_score": [0.5],
        "graph_risk_score": [0.4],
        "ais_gap_count_30d": [2],
        "sanctions_distance": [0],
        "last_seen": [last_seen],
    })
    wl_path = tmp_path / "watchlist.parquet"
    wl.write_parquet(wl_path)

    desig_date = (_NOW - timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    jsonl = _make_sanctions_jsonl(tmp_path, [
        {"first_seen": desig_date, "properties": {"mmsi": ["123456789"]}},
    ])
    out = tmp_path / "report.json"
    result = subprocess.run(
        [sys.executable, "scripts/validate_lead_time_ofac.py",
         "--watchlist", str(wl_path),
         "--jsonl", str(jsonl),
         "--out", str(out)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(out.read_text())
    for key in ("matched_to_designation", "pre_designation_count", "pre_designation_vessels",
                "unknown_unknown_candidates", "mean_lead_days", "median_lead_days"):
        assert key in report, f"missing key: {key}"
    assert report["matched_to_designation"] == 1


def test_print_report_cli_runs(tmp_path):
    """print_lead_time_report.py runs without error against a minimal report."""
    report = {
        "generated_at_utc": "2026-05-06T00:00:00+00:00",
        "watchlist_size": 100,
        "designation_dates_loaded": 10,
        "matched_to_designation": 5,
        "pre_designation_count": 2,
        "post_designation_count": 3,
        "mean_lead_days": 28,
        "p25_lead_days": 22,
        "median_lead_days": 28,
        "p75_lead_days": 34,
        "unknown_unknown_candidates": 50,
        "pre_designation_vessels": [
            {
                "mmsi": "123456789", "vessel_name": "TEST", "flag": "PA",
                "watchlist_rank": 5, "confidence": 0.75,
                "designation_date_proxy": "2026-04-15",
                "detection_window_start": "2026-03-17",
                "lead_days": 29, "ais_gap_count_30d": 3, "sanctions_distance": 0,
            }
        ],
        "post_designation_vessels": [],
        "unknown_unknown_watch": [],
    }
    report_path = tmp_path / "lead_time_report.json"
    report_path.write_text(json.dumps(report))
    result = subprocess.run(
        [sys.executable, "scripts/print_lead_time_report.py", "--report", str(report_path)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "PRE-DESIGNATION" in result.stdout
    assert "CAP VISTA PITCH FRAMING" in result.stdout
    assert "123456789" in result.stdout


def test_print_report_cli_missing_file(tmp_path):
    """print_lead_time_report.py exits cleanly when report file is missing."""
    result = subprocess.run(
        [sys.executable, "scripts/print_lead_time_report.py",
         "--report", str(tmp_path / "missing.json")],
        capture_output=True, text=True,
    )
    assert result.returncode == 0
    assert "error" in result.stdout.lower()
