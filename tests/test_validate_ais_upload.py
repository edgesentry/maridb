"""Unit tests for scripts/validate_ais_upload.py.

All tests run offline — no R2 credentials required.
R2 read/write functions are monkeypatched.
"""

from __future__ import annotations

import importlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest


def _make_df(**overrides) -> pl.DataFrame:
    now = datetime.now(UTC)
    data = {
        "mmsi": ["123456789", "987654321"],
        "timestamp": [now - timedelta(hours=1), now - timedelta(hours=2)],
        "lat": [35.0, 1.3],
        "lon": [139.0, 103.8],
    }
    data.update(overrides)
    return pl.DataFrame(data)


@pytest.fixture()
def mod():
    if "validate_ais_upload" in sys.modules:
        del sys.modules["validate_ais_upload"]
    spec = importlib.util.spec_from_file_location(
        "validate_ais_upload",
        Path(__file__).parent.parent / "scripts" / "validate_ais_upload.py",
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ---------------------------------------------------------------------------
# _validate_region
# ---------------------------------------------------------------------------


class TestValidateRegion:
    def test_passes_clean_data(self, mod):
        df = _make_df()
        result = mod._validate_region(df, "singapore", "2026-04-29", is_recent=True)
        assert result["pass"] is True

    def test_fails_missing_column(self, mod):
        df = _make_df().drop("lat")
        result = mod._validate_region(df, "singapore", "2026-04-29", is_recent=True)
        assert result["pass"] is False
        assert "lat" in result["checks"]["schema"]["missing_columns"]

    def test_passes_zero_rows(self, mod):
        df = pl.DataFrame({"mmsi": [], "timestamp": [], "lat": [], "lon": []})
        result = mod._validate_region(df, "singapore", "2026-04-29", is_recent=True)
        assert result["pass"] is True
        assert result["checks"]["row_count"]["rows"] == 0

    def test_fails_mmsi_nulls(self, mod):
        df = _make_df(mmsi=[None, "987654321"])
        result = mod._validate_region(df, "europe", "2026-04-29", is_recent=True)
        assert result["pass"] is False

    def test_fails_out_of_range_coords(self, mod):
        df = _make_df(lat=[999.0, 1.3], lon=[139.0, 103.8])
        result = mod._validate_region(df, "japansea", "2026-04-29", is_recent=True)
        assert result["pass"] is False

    def test_fails_stale_timestamps_only_for_recent(self, mod):
        old = datetime.now(UTC) - timedelta(days=5)
        df = _make_df(timestamp=[old, old])
        result = mod._validate_region(df, "blacksea", "2026-04-29", is_recent=True)
        assert result["pass"] is False
        assert result["checks"]["timestamp_recency"]["stale_fraction"] == 1.0

    def test_skips_recency_for_older_days(self, mod):
        old = datetime.now(UTC) - timedelta(days=5)
        df = _make_df(timestamp=[old, old])
        result = mod._validate_region(df, "blacksea", "2026-04-27", is_recent=False)
        assert "timestamp_recency" not in result["checks"]

    def test_fails_high_duplicate_rate(self, mod):
        now = datetime.now(UTC)
        df = pl.DataFrame(
            {
                "mmsi": ["123456789"] * 200 + ["999999999"],
                "timestamp": [now] * 201,
                "lat": [35.0] * 201,
                "lon": [139.0] * 201,
            }
        )
        result = mod._validate_region(df, "europe", "2026-04-29", is_recent=True)
        assert result["pass"] is False


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------


class TestMain:
    def _setup(
        self, mod, region_data_by_date: dict, tmp_path: Path, monkeypatch, validate_days: int = 3
    ) -> int:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("VALIDATE_DAYS", str(validate_days))
        monkeypatch.setenv("REPORT_LOCAL_PATH", str(tmp_path / "report.json"))

        class _FakeFS:
            def open_output_stream(self, path):
                import io

                return io.BytesIO()

        monkeypatch.setattr(mod, "_build_fs", lambda: _FakeFS())

        def fake_read(fs, bucket, key):
            date_part = next(
                (p.removeprefix("date=") for p in key.split("/") if p.startswith("date=")), None
            )
            region = next(
                (p.removeprefix("region=") for p in key.split("/") if p.startswith("region=")), None
            )
            day_data = region_data_by_date.get(date_part, {})
            return day_data.get(region)

        monkeypatch.setattr(mod, "_read_parquet_from_r2", fake_read)
        return mod.main()

    def _all_days_data(self, mod, days: int = 3) -> dict:
        good = _make_df()
        now = datetime.now(UTC)
        return {
            (now - timedelta(days=i)).date().isoformat(): {r: good for r in mod._ACTIVE_REGIONS}
            for i in range(days, 0, -1)
        }

    def test_passes_when_all_days_good(self, mod, tmp_path, monkeypatch):
        data = self._all_days_data(mod)
        assert self._setup(mod, data, tmp_path, monkeypatch) == 0

    def test_fails_when_most_recent_day_fails(self, mod, tmp_path, monkeypatch):
        now = datetime.now(UTC)
        good = _make_df()
        data = {
            (now - timedelta(days=3)).date().isoformat(): {r: good for r in mod._ACTIVE_REGIONS},
            (now - timedelta(days=2)).date().isoformat(): {r: good for r in mod._ACTIVE_REGIONS},
            (now - timedelta(days=1)).date().isoformat(): {},  # most recent: all MISSING
        }
        assert self._setup(mod, data, tmp_path, monkeypatch) == 1

    def test_passes_when_one_older_day_fails(self, mod, tmp_path, monkeypatch):
        now = datetime.now(UTC)
        good = _make_df()
        data = {
            (now - timedelta(days=3)).date().isoformat(): {},  # oldest day missing (late arrival)
            (now - timedelta(days=2)).date().isoformat(): {r: good for r in mod._ACTIVE_REGIONS},
            (now - timedelta(days=1)).date().isoformat(): {r: good for r in mod._ACTIVE_REGIONS},
        }
        # 2/3 days pass, most recent passes → overall PASS
        assert self._setup(mod, data, tmp_path, monkeypatch) == 0

    def test_report_has_day_results(self, mod, tmp_path, monkeypatch):
        data = self._all_days_data(mod)
        self._setup(mod, data, tmp_path, monkeypatch)
        report = json.loads((tmp_path / "report.json").read_text())
        assert len(report["day_results"]) == 3
        assert "overall_pass" in report

    def test_validate_days_configurable(self, mod, tmp_path, monkeypatch):
        data = self._all_days_data(mod, days=5)
        self._setup(mod, data, tmp_path, monkeypatch, validate_days=5)
        report = json.loads((tmp_path / "report.json").read_text())
        assert len(report["day_results"]) == 5
