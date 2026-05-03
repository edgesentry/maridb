"""Smoke tests for score, analysis, and validation modules."""

import polars as pl
import pytest

from pipelines.storage.validate import PipelineValidationError, validate_output


class TestGate1Validation:
    def test_rejects_empty_dataframe(self):
        df = pl.DataFrame({"mmsi": [], "confidence": []})
        with pytest.raises(PipelineValidationError, match="row count"):
            validate_output(df, "watchlist")

    def test_rejects_missing_column(self):
        df = pl.DataFrame({"mmsi": ["123456789"]})
        with pytest.raises(PipelineValidationError, match="missing required columns"):
            validate_output(df, "watchlist")

    def test_rejects_null_in_required_column(self):
        df = pl.DataFrame({"mmsi": [None], "confidence": [0.5]})
        with pytest.raises(PipelineValidationError, match="null values"):
            validate_output(df, "watchlist")

    def test_accepts_valid_watchlist(self):
        df = pl.DataFrame({"mmsi": ["123456789"], "confidence": [0.72]})
        validate_output(df, "watchlist")  # should not raise

    def test_accepts_valid_vessel_features(self):
        df = pl.DataFrame(
            {
                "mmsi": ["123456789"],
                "ais_gap_count_30d": [3],
                "loitering_hours_30d": [12.5],
                "sanctions_distance": [2],
            }
        )
        validate_output(df, "vessel_features")

    def test_accepts_valid_voyage_evidence(self):
        df = pl.DataFrame(
            {
                "vessel_id": ["IMO9876543"],
                "voyage_id": ["V001"],
                "track_start_utc": ["2026-04-23T10:00:00Z"],
                "track_end_utc": ["2026-04-28T05:55:00Z"],
                "positions_count": [1842],
            }
        )
        validate_output(df, "voyage_evidence")

    def test_unknown_output_type_raises(self):
        df = pl.DataFrame({"col": [1]})
        with pytest.raises(PipelineValidationError, match="No validation spec"):
            validate_output(df, "unknown_type")


class TestGate2Distribute:
    def test_push_rejects_missing_columns(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        from pipelines.distribute.push import _validate

        df = pl.DataFrame({"mmsi": ["123"]})
        result = _validate(df, required_columns=["mmsi", "confidence"])
        assert not result.ok

    def test_push_accepts_valid_watchlist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        from pipelines.distribute.push import _validate

        df = pl.DataFrame({"mmsi": ["123456789"], "confidence": [0.72]})
        result = _validate(df, required_columns=["mmsi", "confidence"])
        assert result.ok

    def test_local_distribute_watchlist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        monkeypatch.setenv("USE_S3", "0")

        # Write watchlist to maridb-public location (DATA_DIR/score/...)
        source_dir = tmp_path / "score"
        source_dir.mkdir()
        df = pl.DataFrame({"mmsi": ["123456789"], "confidence": [0.72]})
        df.write_parquet(source_dir / "singapore_watchlist.parquet")

        from pipelines.distribute.push import push_arktrace_watchlist

        ok = push_arktrace_watchlist("singapore")
        assert ok

        dest = tmp_path / "watchlist" / "singapore_watchlist.parquet"
        assert dest.exists()
        written = pl.read_parquet(dest)
        assert written["mmsi"][0] == "123456789"


class TestScoreImports:
    def test_import_anomaly(self):
        import pipelines.score.anomaly  # noqa: F401

    def test_import_composite(self):
        import pipelines.score.composite  # noqa: F401

    def test_import_watchlist(self):
        import pipelines.score.watchlist  # noqa: F401

    def test_import_mpol_baseline(self):
        import pipelines.score.mpol_baseline  # noqa: F401


class TestAnalysisImports:
    def test_import_causal(self):
        import pipelines.analysis.causal  # noqa: F401

    def test_import_label_propagation(self):
        import pipelines.analysis.label_propagation  # noqa: F401

    def test_import_monitor(self):
        import pipelines.analysis.monitor  # noqa: F401
