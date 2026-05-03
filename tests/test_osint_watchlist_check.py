"""Tests for scripts/osint_watchlist_check.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import osint_watchlist_check as owc


def _make_watchlist(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mmsi": [str(r["mmsi"]) for r in rows],
            "imo": [r.get("imo", "") for r in rows],
            "vessel_name": [r.get("vessel_name", "") for r in rows],
            "flag": [r.get("flag", "") for r in rows],
            "vessel_type": [r.get("vessel_type", "Unknown") for r in rows],
            "confidence": [float(r["confidence"]) for r in rows],
            "sanctions_distance": [int(r.get("sanctions_distance", 99)) for r in rows],
            "ais_gap_count_30d": [int(r.get("ais_gap_count_30d", 0)) for r in rows],
            "ais_gap_max_hours": [float(r.get("ais_gap_max_hours", 0)) for r in rows],
            "top_signals": [r.get("top_signals", None) for r in rows],
            "last_seen": [r.get("last_seen", None) for r in rows],
        }
    )


# --- _is_stateless ---

def test_stateless_unallocated_mid():
    assert owc._is_stateless("400123456") is True


def test_stateless_short_mmsi():
    assert owc._is_stateless("12") is True


def test_not_stateless_russia():
    assert owc._is_stateless("273449240") is False


def test_not_stateless_singapore():
    assert owc._is_stateless("563069100") is False


def test_not_stateless_marshall_islands():
    assert owc._is_stateless("538006982") is False


# --- URL generation ---

def test_mt_mmsi_url():
    url = owc._mt_mmsi_url("273449240")
    assert "marinetraffic.com" in url
    assert "273449240" in url


def test_mt_imo_url():
    url = owc._mt_imo_url("9354521")
    assert "marinetraffic.com" in url
    assert "9354521" in url


def test_vf_url():
    url = owc._vf_url("312171000")
    assert "vesselfinder.com" in url
    assert "312171000" in url


def test_ofac_url_named_vessel():
    url = owc._ofac_url("ANHONA", "312171000")
    assert "ANHONA" in url or "ANHONA" in url.upper()
    assert "ofac" in url


def test_ofac_url_unnamed_uses_mmsi():
    url = owc._ofac_url("", "312171000")
    assert "312171000" in url


# --- _parse_signals ---

def test_parse_signals_list():
    signals = [{"feature": "ais_gap_count_30d", "value": 5, "contribution": 5.0}]
    assert owc._parse_signals(signals) == signals


def test_parse_signals_json_string():
    raw = '[{"feature": "ais_gap_count_30d", "value": 5, "contribution": 5.0}]'
    result = owc._parse_signals(raw)
    assert result[0]["feature"] == "ais_gap_count_30d"


def test_parse_signals_none():
    assert owc._parse_signals(None) == []


def test_parse_signals_invalid_string():
    assert owc._parse_signals("not json") == []


# --- run() integration ---

def test_run_basic(tmp_path):
    rows = [
        {"mmsi": "273449240", "imo": "", "vessel_name": "", "flag": "RU", "confidence": 0.67, "sanctions_distance": 0, "ais_gap_count_30d": 10},
        {"mmsi": "312171000", "imo": "9354521", "vessel_name": "ANHONA", "flag": "BZ", "confidence": 0.52, "sanctions_distance": 0, "ais_gap_count_30d": 2},
        {"mmsi": "400789012", "imo": "", "vessel_name": "", "flag": "", "confidence": 0.61, "sanctions_distance": 0, "ais_gap_count_30d": 1},
        {"mmsi": "215800600", "imo": "", "vessel_name": "", "flag": "", "confidence": 0.41, "sanctions_distance": 99, "ais_gap_count_30d": 0},
    ]
    df = _make_watchlist(rows)
    pq_path = tmp_path / "watchlist.parquet"
    df.write_parquet(pq_path)
    out_path = tmp_path / "out.json"

    result = owc.run(pq_path, top_n=10, output_path=out_path)

    assert out_path.exists()
    assert result["total_vessels_in_watchlist"] == 4
    assert result["summary"]["sanctions_distance_0_count"] == 3
    assert result["summary"]["stateless_mmsi_count"] == 2  # 400789012 and 215800600

    data = json.loads(out_path.read_text())
    assert data["vessels"][0]["mmsi"] == "273449240"


def test_run_top_n_limit(tmp_path):
    rows = [
        {"mmsi": f"27344924{i}", "confidence": 0.9 - i * 0.01, "sanctions_distance": 99}
        for i in range(10)
    ]
    df = _make_watchlist(rows)
    pq_path = tmp_path / "watchlist.parquet"
    df.write_parquet(pq_path)
    out_path = tmp_path / "out.json"

    result = owc.run(pq_path, top_n=3, output_path=out_path)

    assert len(result["vessels"]) == 3
    assert result["top_n"] == 3


def test_run_ranks_by_confidence(tmp_path):
    rows = [
        {"mmsi": "111111111", "confidence": 0.30, "sanctions_distance": 99},
        {"mmsi": "222222222", "confidence": 0.90, "sanctions_distance": 99},
        {"mmsi": "333333333", "confidence": 0.60, "sanctions_distance": 99},
    ]
    df = _make_watchlist(rows)
    pq_path = tmp_path / "watchlist.parquet"
    df.write_parquet(pq_path)
    out_path = tmp_path / "out.json"

    result = owc.run(pq_path, top_n=3, output_path=out_path)

    assert result["vessels"][0]["mmsi"] == "222222222"
    assert result["vessels"][0]["rank"] == 1


def test_run_missing_watchlist(tmp_path):
    with pytest.raises(SystemExit):
        owc.run(tmp_path / "nonexistent.parquet", top_n=50, output_path=tmp_path / "out.json")


def test_vessel_links_present(tmp_path):
    rows = [{"mmsi": "312171000", "imo": "9354521", "vessel_name": "ANHONA", "flag": "BZ", "confidence": 0.52, "sanctions_distance": 0}]
    df = _make_watchlist(rows)
    pq_path = tmp_path / "watchlist.parquet"
    df.write_parquet(pq_path)
    out_path = tmp_path / "out.json"

    result = owc.run(pq_path, top_n=5, output_path=out_path)

    v = result["vessels"][0]
    assert "marinetraffic_mmsi" in v["links"]
    assert "marinetraffic_imo" in v["links"]
    assert "vesselfinder" in v["links"]
    assert "ofac_search" in v["links"]
