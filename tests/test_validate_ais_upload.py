"""Unit tests for scripts/validate_ais_upload.py.

All tests run offline — no R2 credentials required.
The _read_parquet_from_r2 and R2 upload functions are monkeypatched.
"""

from __future__ import annotations

import importlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(**overrides) -> pl.DataFrame:
    """Return a minimal valid AIS positions DataFrame."""
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
    """Import validate_ais_upload fresh (avoids env-var side effects)."""
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
        result = mod._validate_region(df, "singapore", "2026-04-29")
        assert result["pass"] is True

    def test_fails_missing_column(self, mod):
        df = _make_df().drop("lat")
        result = mod._validate_region(df, "singapore", "2026-04-29")
        assert result["pass"] is False
        assert "lat" in result["checks"]["schema"]["missing_columns"]

    def test_fails_zero_rows(self, mod):
        df = pl.DataFrame({"mmsi": [], "timestamp": [], "lat": [], "lon": []})
        result = mod._validate_region(df, "singapore", "2026-04-29")
        assert result["pass"] is False
        assert result["checks"]["row_count"]["pass"] is False

    def test_fails_mmsi_nulls(self, mod):
        df = _make_df(mmsi=[None, "987654321"])
        result = mod._validate_region(df, "europe", "2026-04-29")
        assert result["pass"] is False
        assert result["checks"]["mmsi_null_rate"]["pass"] is False

    def test_fails_out_of_range_coords(self, mod):
        df = _make_df(lat=[999.0, 1.3], lon=[139.0, 103.8])
        result = mod._validate_region(df, "japansea", "2026-04-29")
        assert result["pass"] is False
        assert result["checks"]["coordinates"]["out_of_range"] > 0

    def test_fails_stale_timestamps(self, mod):
        old = datetime.now(UTC) - timedelta(days=5)
        df = _make_df(timestamp=[old, old])
        result = mod._validate_region(df, "blacksea", "2026-04-29")
        assert result["pass"] is False
        assert result["checks"]["timestamp_recency"]["stale_fraction"] == 1.0

    def test_fails_high_duplicate_rate(self, mod):
        now = datetime.now(UTC)
        df = pl.DataFrame({
            "mmsi": ["123456789"] * 200 + ["999999999"],
            "timestamp": [now] * 200 + [now],
            "lat": [35.0] * 201,
            "lon": [139.0] * 201,
        })
        result = mod._validate_region(df, "europe", "2026-04-29")
        assert result["pass"] is False
        assert result["checks"]["duplicate_rate"]["duplicate_rate"] > 0.01


# ---------------------------------------------------------------------------
# main() integration (R2 calls monkeypatched)
# ---------------------------------------------------------------------------

class TestMain:
    def _run(self, mod, region_data: dict, tmp_path: Path, monkeypatch) -> int:
        """Run main() with R2 reads and writes monkeypatched."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("VALIDATE_DATE", "2026-04-29")
        monkeypatch.setenv("REPORT_LOCAL_PATH", str(tmp_path / "report.json"))

        def fake_read(bucket, key):
            region = next(
                (p.removeprefix("region=") for p in key.split("/") if p.startswith("region=")),
                None,
            )
            return region_data.get(region)

        monkeypatch.setattr(mod, "_read_parquet_from_r2", fake_read)

        # Suppress R2 upload
        import pyarrow.fs as pafs
        class _FakeFS:
            def open_output_stream(self, path):
                import io
                return io.BytesIO()
        monkeypatch.setattr(pafs, "S3FileSystem", lambda **_: _FakeFS())

        return mod.main()

    def test_passes_when_enough_active_regions(self, mod, tmp_path, monkeypatch):
        good = _make_df()
        data = {r: good for r in mod._ACTIVE_REGIONS}
        rc = self._run(mod, data, tmp_path, monkeypatch)
        assert rc == 0

    def test_fails_when_too_few_active_regions(self, mod, tmp_path, monkeypatch):
        good = _make_df()
        # Only 2 active regions have data
        data = {"japansea": good, "singapore": good}
        rc = self._run(mod, data, tmp_path, monkeypatch)
        assert rc == 1

    def test_report_written_to_disk(self, mod, tmp_path, monkeypatch):
        good = _make_df()
        data = {r: good for r in mod._ACTIVE_REGIONS}
        self._run(mod, data, tmp_path, monkeypatch)
        report_path = tmp_path / "report.json"
        assert report_path.exists()
        report = json.loads(report_path.read_text())
        assert "overall_pass" in report
        assert report["target_date"] == "2026-04-29"

    def test_missing_region_marked_as_fail(self, mod, tmp_path, monkeypatch):
        good = _make_df()
        data = {r: good for r in mod._ACTIVE_REGIONS}
        self._run(mod, data, tmp_path, monkeypatch)
        report = json.loads((tmp_path / "report.json").read_text())
        missing = [r for r in report["region_results"] if r.get("error") == "file not found in R2"]
        # inactive regions not in data dict should show as missing
        inactive = set(mod._ALL_REGIONS) - set(mod._ACTIVE_REGIONS)
        assert all(
            any(m["region"] == r for m in missing) for r in inactive
        )
