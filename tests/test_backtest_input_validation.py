"""Tests that run_public_backtest_batch fails fast when input files are missing."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest


def _run_backtest(args: list[str], env_extra: dict | None = None) -> subprocess.CompletedProcess:
    import os
    env = {**os.environ, **(env_extra or {})}
    return subprocess.run(
        [sys.executable, "scripts/run_public_backtest_batch.py"] + args,
        capture_output=True,
        text=True,
        env=env,
    )


class TestMissingWatchlistFails:
    def test_exits_nonzero_when_watchlist_missing(self, tmp_path, monkeypatch):
        """Job must fail (exit 1) when a requested region's watchlist does not exist."""
        monkeypatch.setenv("MARIDB_DATA_DIR", str(tmp_path))

        # Create sanctions DB so prepare_public_sanctions_db doesn't fail
        import duckdb
        db = tmp_path / "public_eval.duckdb"
        con = duckdb.connect(str(db))
        con.execute("""
            CREATE TABLE sanctions_entities (
                entity_id VARCHAR, name VARCHAR, mmsi VARCHAR,
                imo VARCHAR, flag VARCHAR, type VARCHAR, list_source VARCHAR
            )
        """)
        con.execute("INSERT INTO sanctions_entities VALUES ('e1','TEST','123456789','','','vessel','OFAC')")
        con.close()

        result = _run_backtest(
            ["--regions", "singapore", "--skip-pipeline", "--min-known-cases", "1"],
            env_extra={"MARIDB_DATA_DIR": str(tmp_path)},
        )
        assert result.returncode != 0, "Expected non-zero exit when watchlist is missing"
        assert "watchlist not found" in result.stdout or "watchlist not found" in result.stderr

    def test_exits_nonzero_when_all_watchlists_too_small(self, tmp_path, monkeypatch):
        """Job must fail when all watchlists exist but are below min-watchlist-size."""
        monkeypatch.setenv("MARIDB_DATA_DIR", str(tmp_path))

        # Write a tiny watchlist (1 vessel < default min-watchlist-size of 10)
        score_dir = tmp_path / "score"
        score_dir.mkdir()
        pl.DataFrame({
            "mmsi": ["123456789"],
            "confidence": [0.9],
            "composite_score": [0.9],
        }).write_parquet(score_dir / "singapore_watchlist.parquet")

        import duckdb
        db = tmp_path / "public_eval.duckdb"
        con = duckdb.connect(str(db))
        con.execute("""
            CREATE TABLE sanctions_entities (
                entity_id VARCHAR, name VARCHAR, mmsi VARCHAR,
                imo VARCHAR, flag VARCHAR, type VARCHAR, list_source VARCHAR
            )
        """)
        con.execute("INSERT INTO sanctions_entities VALUES ('e1','TEST','123456789','','','vessel','OFAC')")
        con.close()

        result = _run_backtest(
            ["--regions", "singapore", "--skip-pipeline",
             "--min-known-cases", "1", "--min-watchlist-size", "100"],
            env_extra={"MARIDB_DATA_DIR": str(tmp_path)},
        )
        assert result.returncode != 0, "Expected non-zero exit when all watchlists too small"

    def test_watchlist_path_checks_score_subdir_first(self, tmp_path, monkeypatch):
        """_watchlist_path() should prefer score/ subdir over flat top-level."""
        monkeypatch.setenv("MARIDB_DATA_DIR", str(tmp_path))

        # Write to score/ subdir
        score_dir = tmp_path / "score"
        score_dir.mkdir()
        score_path = score_dir / "singapore_watchlist.parquet"
        pl.DataFrame({"mmsi": ["x"], "confidence": [0.5], "composite_score": [0.5]}).write_parquet(score_path)

        # Also write a different file at flat path to confirm score/ takes priority
        flat_path = tmp_path / "singapore_watchlist.parquet"
        pl.DataFrame({"mmsi": ["y"], "confidence": [0.1], "composite_score": [0.1]}).write_parquet(flat_path)

        import importlib, os, sys
        # Reload module with updated env so _data_dir picks up tmp_path
        os.environ["MARIDB_DATA_DIR"] = str(tmp_path)
        if "scripts.run_public_backtest_batch" in sys.modules:
            del sys.modules["scripts.run_public_backtest_batch"]
        sys.path.insert(0, str(Path(__file__).parents[1]))
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "rbb", Path(__file__).parents[1] / "scripts" / "run_public_backtest_batch.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        resolved = mod._watchlist_path("singapore")
        assert resolved == score_path, f"Expected score/ path, got {resolved}"
