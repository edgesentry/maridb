"""Tests for sync_r2.py push-ais-parquet / pull-ais-parquet commands."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import scripts.sync_r2 as sync_r2


def _make_duckdb(path: Path, n_rows: int = 5) -> Path:
    """Create a minimal DuckDB with ais_positions rows."""
    import duckdb
    from datetime import UTC, datetime, timedelta

    con = duckdb.connect(str(path))
    con.execute("""
        CREATE TABLE ais_positions (
            mmsi VARCHAR, timestamp TIMESTAMPTZ, lat DOUBLE, lon DOUBLE,
            sog FLOAT, cog FLOAT, nav_status TINYINT, ship_type TINYINT
        )
    """)
    base = datetime(2026, 4, 1, tzinfo=UTC)
    rows = [(f"24000{i:04d}", base + timedelta(hours=i), 1.3, 103.8, 12.0, 180.0, 0, 70)
            for i in range(n_rows)]
    con.executemany("INSERT INTO ais_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()
    return path


class TestPushAisParquet:
    def test_skips_small_db(self, tmp_path):
        """push-ais-parquet returns 1 when no eligible (>=1MB) DBs are found."""
        import duckdb
        db = tmp_path / "singapore.duckdb"
        con = duckdb.connect(str(db))
        con.execute("CREATE TABLE ais_positions (mmsi VARCHAR, timestamp TIMESTAMPTZ, "
                    "lat DOUBLE, lon DOUBLE, sog FLOAT, cog FLOAT, nav_status TINYINT, "
                    "ship_type TINYINT)")
        con.close()
        # File is < 1 MB so _ais_db_candidates skips it
        args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore")
        rc = sync_r2.cmd_push_ais_parquet(args)
        assert rc == 1  # no eligible files

    def test_uploads_new_date_partitions(self, tmp_path):
        """push-ais-parquet uploads date partitions not yet in R2."""
        db = _make_duckdb(tmp_path / "singapore.duckdb", n_rows=10)

        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = []  # no existing partitions

        written_keys = []

        def fake_write_table(table, r2_key, filesystem, compression):
            written_keys.append(r2_key)

        args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore")
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        assert rc == 0
        assert len(written_keys) > 0
        assert all("maridb-public/ais/region=singapore/date=" in k for k in written_keys)
        assert all(k.endswith("/positions.parquet") for k in written_keys)

    def test_skips_dates_already_in_r2(self, tmp_path):
        """push-ais-parquet skips date partitions that already exist in R2."""
        db = _make_duckdb(tmp_path / "singapore.duckdb", n_rows=5)

        mock_fs = MagicMock()
        existing_info = MagicMock()
        existing_info.path = "maridb-public/ais/region=singapore/date=2026-04-01/positions.parquet"
        mock_fs.get_file_info.return_value = [existing_info]

        written_keys = []

        def fake_write_table(table, r2_key, filesystem, compression):
            written_keys.append(r2_key)

        args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore")
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        assert rc == 0
        assert not any("date=2026-04-01" in k for k in written_keys)

    def test_no_eligible_dbs(self, tmp_path):
        args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore")
        rc = sync_r2.cmd_push_ais_parquet(args)
        assert rc == 1


class TestPullAisParquet:
    def test_downloads_new_partitions(self, tmp_path):
        """pull-ais-parquet downloads partitions not yet present locally."""
        mock_fs = MagicMock()
        import pyarrow.fs as pafs

        remote_info = MagicMock()
        remote_info.path = "maridb-public/ais/region=singapore/date=2026-04-25/positions.parquet"
        remote_info.size = 1024
        remote_info.type = pafs.FileType.File
        mock_fs.get_file_info.return_value = [remote_info]

        table = pa.table({"mmsi": ["123456789"], "timestamp": pa.array([0], type=pa.timestamp("us", tz="UTC")),
                          "lat": [1.3], "lon": [103.8], "sog": [12.0], "cog": [180.0],
                          "nav_status": [0], "ship_type": [70]})
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch("pyarrow.parquet.read_table", return_value=table), \
             patch("pyarrow.parquet.write_table"):
            args = argparse.Namespace(data_dir=str(tmp_path), regions=None, days=60)
            rc = sync_r2.cmd_pull_ais_parquet(args)

        assert rc == 0

    def test_skips_existing_local(self, tmp_path):
        """pull-ais-parquet skips partitions already present with matching size."""
        local = tmp_path / "ais" / "region=singapore" / "date=2026-04-25"
        local.mkdir(parents=True)
        (local / "positions.parquet").write_bytes(b"x" * 512)

        mock_fs = MagicMock()
        import pyarrow.fs as pafs

        remote_info = MagicMock()
        remote_info.path = "maridb-public/ais/region=singapore/date=2026-04-25/positions.parquet"
        remote_info.size = 512  # same size -> skip
        remote_info.type = pafs.FileType.File
        mock_fs.get_file_info.return_value = [remote_info]

        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch("pyarrow.parquet.read_table") as mock_read:
            args = argparse.Namespace(data_dir=str(tmp_path), regions=None, days=60)
            rc = sync_r2.cmd_pull_ais_parquet(args)

        mock_read.assert_not_called()
        assert rc == 0

    def test_filters_by_days(self, tmp_path):
        """pull-ais-parquet skips partitions older than --days."""
        mock_fs = MagicMock()
        import pyarrow.fs as pafs

        old_info = MagicMock()
        old_info.path = "maridb-public/ais/region=singapore/date=2020-01-01/positions.parquet"
        old_info.size = 1024
        old_info.type = pafs.FileType.File
        mock_fs.get_file_info.return_value = [old_info]

        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch("pyarrow.parquet.read_table") as mock_read:
            args = argparse.Namespace(data_dir=str(tmp_path), regions=None, days=30)
            rc = sync_r2.cmd_pull_ais_parquet(args)

        mock_read.assert_not_called()
        assert rc == 0
