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


def _make_duckdb(path: Path, n_rows: int = 5, with_vessel_meta: bool = False) -> Path:
    """Create a minimal DuckDB with ais_positions (and optionally vessel_meta) rows."""
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

    if with_vessel_meta:
        con.execute("""
            CREATE TABLE vessel_meta (
                mmsi VARCHAR, imo VARCHAR, name VARCHAR, flag VARCHAR, ship_type INTEGER
            )
        """)
        for i in range(n_rows):
            con.execute(
                "INSERT INTO vessel_meta VALUES (?, ?, ?, ?, ?)",
                [f"24000{i:04d}", f"900000{i}", f"VESSEL {i}", "SG", 70],
            )
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
        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        rc = sync_r2.cmd_push_ais_parquet(args)
        assert rc == 1  # no eligible files

    def test_uploads_new_date_partitions(self, tmp_path):
        """push-ais-parquet uploads date partitions not yet in R2."""
        db = _make_duckdb(tmp_path / "singapore.duckdb", n_rows=10)

        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = []  # no existing partitions

        written_keys = []

        def fake_write_table(table, path, filesystem=None, compression=None):
            if filesystem is not None:  # only track R2 uploads, not local staging writes
                written_keys.append(path)

        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
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

        def fake_write_table(table, path, filesystem=None, compression=None):
            if filesystem is not None:
                written_keys.append(path)

        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        assert rc == 0
        assert not any("date=2026-04-01" in k for k in written_keys)

    def test_no_eligible_dbs(self, tmp_path):
        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        rc = sync_r2.cmd_push_ais_parquet(args)
        assert rc == 1

    def test_uploads_sentinel_when_db_empty(self, tmp_path):
        """push-ais-parquet uploads an empty sentinel for today when DB has 0 rows."""
        import duckdb
        from datetime import date

        db = tmp_path / "singapore.duckdb"
        con = duckdb.connect(str(db))
        con.execute("""
            CREATE TABLE ais_positions (
                mmsi VARCHAR, timestamp TIMESTAMPTZ, lat DOUBLE, lon DOUBLE,
                sog FLOAT, cog FLOAT, nav_status TINYINT, ship_type TINYINT
            )
        """)
        con.close()

        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = []
        written_keys = []

        def fake_write_table(table, path, filesystem=None, compression=None):
            if filesystem is not None:
                written_keys.append((path, table.num_rows))

        today = date.today().isoformat()
        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        assert rc == 0
        sentinel_keys = [k for k, rows in written_keys if f"date={today}" in k and rows == 0]
        assert len(sentinel_keys) == 1, f"expected 1 sentinel, got {written_keys}"

    def test_uploads_sentinel_when_today_missing_from_r2(self, tmp_path):
        """push-ais-parquet uploads empty sentinel for today even if DB has older rows."""
        from datetime import date

        db = _make_duckdb(tmp_path / "singapore.duckdb", n_rows=5)  # rows on 2026-04-01

        mock_fs = MagicMock()
        # Simulate R2 already having 2026-04-01 but NOT today
        existing_info = MagicMock()
        existing_info.path = "maridb-public/ais/region=singapore/date=2026-04-01/positions.parquet"
        mock_fs.get_file_info.return_value = [existing_info]

        written_keys = []

        def fake_write_table(table, path, filesystem=None, compression=None):
            if filesystem is not None:
                written_keys.append((path, table.num_rows))

        today = date.today().isoformat()
        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        assert rc == 0
        sentinel_keys = [k for k, rows in written_keys if f"date={today}" in k and rows == 0]
        assert len(sentinel_keys) == 1, f"expected 1 today sentinel, got {written_keys}"


class TestVesselMetaPush:
    def test_pushes_vessel_meta_when_present(self, tmp_path):
        """push-ais-parquet uploads vessel_meta.parquet to maridb-public/vessel_meta/."""
        db = _make_duckdb(tmp_path / "singapore.duckdb", n_rows=10, with_vessel_meta=True)

        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = []

        written_keys = []

        def fake_write_table(table, path, filesystem=None, compression=None):
            if filesystem is not None:
                written_keys.append(path)

        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        assert rc == 0
        vm_keys = [k for k in written_keys if "vessel_meta" in k]
        assert len(vm_keys) == 1
        assert vm_keys[0] == "maridb-public/vessel_meta/region=singapore.parquet"

    def test_skips_vessel_meta_when_empty(self, tmp_path):
        """push-ais-parquet skips vessel_meta upload when table is empty."""
        db = _make_duckdb(tmp_path / "singapore.duckdb", n_rows=10, with_vessel_meta=False)

        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = []

        written_keys = []

        def fake_write_table(table, path, filesystem=None, compression=None):
            if filesystem is not None:
                written_keys.append(path)

        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        assert rc == 0
        assert not any("vessel_meta" in k for k in written_keys)

    def test_skips_vessel_meta_on_schema_validation_failure(self, tmp_path):
        """push-ais-parquet skips vessel_meta upload when schema validation fails."""
        import duckdb
        db = tmp_path / "singapore.duckdb"
        con = duckdb.connect(str(db))
        con.execute("""
            CREATE TABLE ais_positions (
                mmsi VARCHAR, timestamp TIMESTAMPTZ, lat DOUBLE, lon DOUBLE,
                sog FLOAT, cog FLOAT, nav_status TINYINT, ship_type TINYINT
            )
        """)
        from datetime import UTC, datetime
        con.execute("INSERT INTO ais_positions VALUES (?,?,?,?,?,?,?,?)",
                    ["123456789", datetime(2026,4,1,tzinfo=UTC), 1.3, 103.8, 12.0, 180.0, 0, 70])
        # vessel_meta missing required mmsi column → validation should fail
        con.execute("CREATE TABLE vessel_meta (bad_col VARCHAR)")
        con.execute("INSERT INTO vessel_meta VALUES ('oops')")
        con.close()

        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = []
        written_keys = []

        def fake_write_table(table, path, filesystem=None, compression=None):
            if filesystem is not None:
                written_keys.append(path)

        args = argparse.Namespace(
            data_dir=str(tmp_path), staging_dir=str(tmp_path / "staging"), regions="singapore"
        )
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch.object(sync_r2, "_ais_db_candidates", return_value=[db]), \
             patch("pyarrow.parquet.write_table", side_effect=fake_write_table):
            rc = sync_r2.cmd_push_ais_parquet(args)

        # rc may be 0 (positions skipped due to date filter) or non-zero; key check is no vm upload
        assert not any("vessel_meta" in k for k in written_keys)


class TestVesselMetaPull:
    def test_downloads_vessel_meta(self, tmp_path):
        """pull-ais-parquet downloads vessel_meta parquet for each region."""
        import pyarrow.fs as pafs

        mock_fs = MagicMock()
        pos_info = MagicMock()
        pos_info.path = "maridb-public/ais/region=singapore/date=2026-04-25/positions.parquet"
        pos_info.size = 512
        pos_info.type = pafs.FileType.File

        vm_info = MagicMock()
        vm_info.path = "maridb-public/vessel_meta/region=singapore.parquet"
        vm_info.size = 256
        vm_info.type = pafs.FileType.File

        # get_file_info: first call lists ais/, second call checks vm file exists
        mock_fs.get_file_info.side_effect = [
            [pos_info],           # listing ais/
            [vm_info],            # checking vessel_meta file info
        ]

        pos_table = pa.table({
            "mmsi": ["123456789"], "timestamp": pa.array([0], type=pa.timestamp("us", tz="UTC")),
            "lat": [1.3], "lon": [103.8], "sog": [12.0], "cog": [180.0],
            "nav_status": [0], "ship_type": [70],
        })
        vm_table = pa.table({
            "mmsi": ["123456789"], "imo": ["9000001"], "name": ["TEST VESSEL"],
            "flag": ["SG"], "ship_type": [70],
        })

        read_calls = []

        def fake_read_table(path, filesystem=None):
            read_calls.append(path)
            if "vessel_meta" in path:
                return vm_table
            return pos_table

        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch("pyarrow.parquet.read_table", side_effect=fake_read_table), \
             patch("pyarrow.parquet.write_table"):
            args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore", days=60)
            rc = sync_r2.cmd_pull_ais_parquet(args)

        assert rc == 0
        assert any("vessel_meta" in p for p in read_calls)

    def test_skips_vessel_meta_already_present(self, tmp_path):
        """pull-ais-parquet skips vessel_meta if local file matches remote size."""
        import pyarrow.fs as pafs

        local_vm = tmp_path / "vessel_meta" / "region=singapore.parquet"
        local_vm.parent.mkdir(parents=True)
        local_vm.write_bytes(b"x" * 256)

        mock_fs = MagicMock()
        mock_fs.get_file_info.side_effect = [
            [],       # no positions
            [MagicMock(type=pafs.FileType.File, size=256,
                       path="maridb-public/vessel_meta/region=singapore.parquet")],
        ]

        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch("pyarrow.parquet.read_table") as mock_read:
            args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore", days=60)
            sync_r2.cmd_pull_ais_parquet(args)

        # vessel_meta should not be re-downloaded (same size)
        vm_reads = [c for c in mock_read.call_args_list if "vessel_meta" in str(c)]
        assert len(vm_reads) == 0


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
        import pyarrow.fs as pafs

        local = tmp_path / "ais" / "region=singapore" / "date=2026-04-25"
        local.mkdir(parents=True)
        (local / "positions.parquet").write_bytes(b"x" * 512)

        mock_fs = MagicMock()
        remote_info = MagicMock()
        remote_info.path = "maridb-public/ais/region=singapore/date=2026-04-25/positions.parquet"
        remote_info.size = 512  # same size → skip
        remote_info.type = pafs.FileType.File
        not_found = MagicMock()
        not_found.type = pafs.FileType.NotFound
        # First call: list ais/; subsequent calls: vessel_meta not found
        mock_fs.get_file_info.side_effect = [[remote_info], [not_found]]

        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch("pyarrow.parquet.read_table") as mock_read:
            args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore", days=60)
            rc = sync_r2.cmd_pull_ais_parquet(args)

        mock_read.assert_not_called()
        assert rc == 0

    def test_filters_by_days(self, tmp_path):
        """pull-ais-parquet skips partitions older than --days."""
        import pyarrow.fs as pafs

        mock_fs = MagicMock()
        old_info = MagicMock()
        old_info.path = "maridb-public/ais/region=singapore/date=2020-01-01/positions.parquet"
        old_info.size = 1024
        old_info.type = pafs.FileType.File
        not_found = MagicMock()
        not_found.type = pafs.FileType.NotFound
        mock_fs.get_file_info.side_effect = [[old_info], [not_found]]

        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs), \
             patch("pyarrow.parquet.read_table") as mock_read:
            args = argparse.Namespace(data_dir=str(tmp_path), regions="singapore", days=30)
            rc = sync_r2.cmd_pull_ais_parquet(args)

        mock_read.assert_not_called()
        assert rc == 0
