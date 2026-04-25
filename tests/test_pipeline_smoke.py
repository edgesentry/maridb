"""Smoke tests for maridb pipelines.

Verify that pipeline modules import cleanly, storage config resolves correctly,
and the distribute validation gate rejects bad data without hitting R2.
"""

import os

import polars as pl
import pytest

from pipelines.storage import config


class TestStorageConfig:
    def test_s3_off_by_default(self):
        assert not config.is_s3()

    def test_local_output_uri(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        uri = config.output_uri("vessels/snapshot.parquet")
        assert uri.startswith(str(tmp_path))
        assert "vessels/snapshot.parquet" in uri

    def test_s3_output_uri(self, monkeypatch):
        monkeypatch.setenv("USE_S3", "1")
        uri = config.output_uri("vessels/snapshot.parquet")
        assert uri.startswith("s3://maridb-public/")

    def test_s3_output_uri_custom_bucket(self, monkeypatch):
        monkeypatch.setenv("USE_S3", "1")
        uri = config.output_uri("ais-summaries/latest.parquet", bucket="arktrace-public")
        assert uri.startswith("s3://arktrace-public/")

    def test_bucket_constants(self):
        assert config.ARKTRACE_BUCKET == "arktrace-public"
        assert config.DOCUMARIS_BUCKET == "documaris-public"


class TestDistributeValidation:
    def test_push_rejects_missing_columns(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        from pipelines.distribute.push import _validate

        df = pl.DataFrame({"vessel_id": ["IMO9876543"], "date": ["2026-04-25"]})
        result = _validate(df, required_columns=["vessel_id", "date", "positions_count"])
        assert not result.ok
        assert any("positions_count" in e for e in result.errors)

    def test_push_rejects_null_in_required_column(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        from pipelines.distribute.push import _validate

        df = pl.DataFrame({
            "vessel_id": ["IMO9876543", None],
            "date": ["2026-04-25", "2026-04-25"],
            "positions_count": [100, 200],
        })
        result = _validate(df, required_columns=["vessel_id", "date", "positions_count"])
        assert not result.ok
        assert any("vessel_id" in e for e in result.errors)

    def test_push_rejects_empty_dataframe(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        from pipelines.distribute.push import _validate

        df = pl.DataFrame({"vessel_id": [], "date": [], "positions_count": []}).cast(
            {"positions_count": pl.Int64}
        )
        result = _validate(df, required_columns=["vessel_id", "date", "positions_count"], min_rows=1)
        assert not result.ok

    def test_push_accepts_valid_data(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))

        from pipelines.distribute.push import _validate

        df = pl.DataFrame({
            "vessel_id": ["IMO9876543"],
            "date": ["2026-04-25"],
            "positions_count": [1842],
        })
        result = _validate(df, required_columns=["vessel_id", "date", "positions_count"])
        assert result.ok
        assert result.errors == []

    def test_local_push_voyage_evidence(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        monkeypatch.setenv("USE_S3", "0")

        # Write a valid source parquet locally
        source = tmp_path / "voyage_evidence.parquet"
        df = pl.DataFrame({
            "vessel_id": ["IMO9876543"],
            "voyage_id": ["V001"],
            "track_start_utc": ["2026-04-23T10:00:00Z"],
            "track_end_utc": ["2026-04-28T05:55:00Z"],
            "positions_count": [1842],
        })
        df.write_parquet(source)

        from pipelines.distribute.push import push_documaris_voyage_evidence

        ok = push_documaris_voyage_evidence(str(source))
        assert ok

        # Verify output landed in DATA_DIR
        dest = tmp_path / "voyage-evidence" / "latest.parquet"
        assert dest.exists()
        written = pl.read_parquet(dest)
        assert len(written) == 1
        assert written["vessel_id"][0] == "IMO9876543"


class TestIngestImports:
    """Verify all ingest modules import without error."""

    def test_import_ais_stream(self):
        import pipelines.ingest.ais_stream  # noqa: F401

    def test_import_vessel_registry(self):
        import pipelines.ingest.vessel_registry  # noqa: F401

    def test_import_sanctions(self):
        import pipelines.ingest.sanctions  # noqa: F401

    def test_import_schema(self):
        import pipelines.ingest.schema  # noqa: F401


class TestFeaturesImports:
    """Verify all feature modules import without error."""

    def test_import_ais_behavior(self):
        import pipelines.features.ais_behavior  # noqa: F401

    def test_import_identity(self):
        import pipelines.features.identity  # noqa: F401

    def test_import_build_matrix(self):
        import pipelines.features.build_matrix  # noqa: F401
