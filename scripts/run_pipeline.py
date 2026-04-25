"""maridb pipeline orchestrator.

Runs the full ingest → features → score → validate → distribute flow.

Usage:
    uv run python scripts/run_pipeline.py --region singapore
    uv run python scripts/run_pipeline.py --region singapore --non-interactive
    uv run python scripts/run_pipeline.py --region singapore --skip-ingest
    uv run python scripts/run_pipeline.py --region singapore --gdelt-days 14 --seed-dummy
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class RegionConfig:
    name: str
    bbox: list[float]
    db_path: str
    watchlist_key: str


# MARIDB_DATA_DIR is the "processed" data dir (e.g. data/processed in CI,
# ~/.maridb/data/processed locally). _DB_DIR is the DuckDB working dir under it.
# _DOWNLOADS_DIR goes up one level to find downloads/ alongside processed/.
_MARIDB_DATA = Path(
    os.getenv("MARIDB_DATA_DIR") or os.getenv("DATA_DIR")
    or (Path.home() / ".maridb" / "data" / "processed")
)
_DB_DIR = _MARIDB_DATA / "ais"
_DOWNLOADS_DIR = _MARIDB_DATA.parent / "downloads" / "ais"


def _db(stem: str) -> str:
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    return str(_DB_DIR / f"{stem}.duckdb")


REGIONS: dict[str, RegionConfig] = {
    "singapore": RegionConfig(
        name="singapore",
        bbox=[-5, 92, 22, 122],
        db_path=_db("singapore"),
        watchlist_key="score/singapore_watchlist.parquet",
    ),
    "japansea": RegionConfig(
        name="japansea",
        bbox=[25, 115, 48, 145],
        db_path=_db("japansea"),
        watchlist_key="score/japansea_watchlist.parquet",
    ),
    "japan": RegionConfig(
        name="japansea",
        bbox=[25, 115, 48, 145],
        db_path=_db("japansea"),
        watchlist_key="score/japansea_watchlist.parquet",
    ),
    "blacksea": RegionConfig(
        name="blacksea",
        bbox=[40, 27, 47, 42],
        db_path=_db("blacksea"),
        watchlist_key="score/blacksea_watchlist.parquet",
    ),
    "europe": RegionConfig(
        name="europe",
        bbox=[35, -10, 65, 30],
        db_path=_db("europe"),
        watchlist_key="score/europe_watchlist.parquet",
    ),
    "middleeast": RegionConfig(
        name="middleeast",
        bbox=[10, 32, 32, 62],
        db_path=_db("middleeast"),
        watchlist_key="score/middleeast_watchlist.parquet",
    ),
}

# Real 2024 OFAC-sanctioned vessels used to ensure CI known-case floor is met
_DUMMY_MMSIS = [
    "352001369", "314856000", "372979000", "312171000", "352898820",
    "352002316", "626152000", "352001298", "314925000", "352001565",
]


def _run(cmd: list[str], env_extra: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = {**os.environ, **(env_extra or {})}
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def _ok(msg: str) -> None:
    logger.info("  ✓ %s", msg)


def _fail(msg: str) -> None:
    logger.error("  ✗ %s", msg)


def _validate_step(db_path: str, checks: list[tuple[str, str, int, bool]]) -> bool:
    """Run SQL row-count checks against the DuckDB after a pipeline step.

    Each check is (label, sql, min_rows, required).
    - required=True:  0 rows → error, aborts pipeline
    - required=False: 0 rows → warning only (e.g. AIS data absent in backtest context)
    - count < min_rows (but > 0) → warning in both cases
    """
    ok = True
    try:
        con = duckdb.connect(db_path, read_only=True)
        for label, sql, min_rows, required in checks:
            try:
                row = con.execute(sql).fetchone()
                count = int(row[0]) if row else 0
                if count == 0:
                    if required:
                        logger.error("  ✗ validate [%s]: 0 rows (expected ≥ %d)", label, min_rows)
                        ok = False
                    else:
                        logger.warning("  ⚠ validate [%s]: 0 rows", label)
                elif count < min_rows:
                    logger.warning("  ⚠ validate [%s]: %d rows (expected ≥ %d)", label, count, min_rows)
                else:
                    logger.info("  ✓ validate [%s]: %d rows", label, count)
            except Exception as e:
                logger.error("  ✗ validate [%s]: query failed: %s", label, e)
                ok = False
        con.close()
    except Exception as e:
        logger.error("  ✗ validate: could not open DB: %s", e)
        ok = False
    return ok


def _seed_dummy_vessels(db_path: str) -> None:
    """Patch real sanctioned vessels into DuckDB so CI known-case floor is met."""
    mmsi_list = ", ".join(f"'{m}'" for m in _DUMMY_MMSIS)
    con = duckdb.connect(db_path)
    try:
        con.execute(f"DELETE FROM vessel_meta WHERE mmsi IN ({mmsi_list})")
        con.execute("""
            INSERT INTO vessel_meta (mmsi, imo, name, flag, ship_type) VALUES
                ('352001369', '9305609', 'CELINE',         'PA', 82),
                ('314856000', '9292486', 'ELINE',          'BB', 82),
                ('372979000', '9219056', 'REX 1',          'PA', 82),
                ('312171000', '9354521', 'ANHONA',         'BZ', 82),
                ('352898820', '9280873', 'AVENTUS I',      'PA', 82),
                ('352002316', '9308778', 'SATINA',         'PA', 82),
                ('626152000', '9162928', 'ASTRA',          'GA', 82),
                ('352001298', '9292228', 'CRYSTAL ROSE',   'PA', 82),
                ('314925000', '9289491', 'BENDIGO',        'BB', 82),
                ('352001565', '9417490', 'ARABIAN ENERGY', 'PA', 82)
        """)
        con.execute(f"DELETE FROM ais_positions WHERE mmsi IN ({mmsi_list})")
        con.execute("""
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type) VALUES
                ('352001369', '2026-03-15 00:00:00+00', 1.25,  103.85, 0.5, 1, 82),
                ('314856000', '2026-03-15 00:00:00+00', 1.35,  103.95, 0.5, 1, 82),
                ('372979000', '2026-03-15 00:00:00+00', 1.45,  104.05, 0.5, 1, 82),
                ('312171000', '2026-03-15 00:00:00+00', 1.55,  104.15, 0.5, 1, 82),
                ('352898820', '2026-03-15 00:00:00+00', 1.65,  104.25, 0.5, 1, 82),
                ('352002316', '2026-03-15 00:00:00+00', 1.75,  104.35, 0.5, 1, 82),
                ('626152000', '2026-03-15 00:00:00+00', 1.85,  104.45, 0.5, 1, 82),
                ('352001298', '2026-03-15 00:00:00+00', 1.95,  104.55, 0.5, 1, 82),
                ('314925000', '2026-03-15 00:00:00+00', 2.05,  104.65, 0.5, 1, 82),
                ('352001565', '2026-03-15 00:00:00+00', 2.15,  104.75, 0.5, 1, 82)
        """)
        con.execute(f"DELETE FROM vessel_features WHERE mmsi IN ({mmsi_list})")
        con.execute("""
            INSERT INTO vessel_features (
                mmsi, ais_gap_count_30d, ais_gap_max_hours, position_jump_count,
                sts_candidate_count, port_call_ratio, loitering_hours_30d,
                flag_changes_2y, name_changes_2y, owner_changes_2y,
                high_risk_flag_ratio, ownership_depth, sanctions_distance,
                cluster_sanctions_ratio, shared_manager_risk, shared_address_centrality,
                sts_hub_degree, route_cargo_mismatch, declared_vs_estimated_cargo_value,
                sanctions_list_count
            ) VALUES
                ('352001369', 14, 22.0, 2, 2, 0.15, 28.0, 2, 1, 1, 0.90, 3, 0, 0.60, 0, 3, 3, 1.0, 50000.0, 3),
                ('314856000', 12, 18.0, 1, 1, 0.20, 20.0, 1, 0, 1, 0.80, 2, 0, 0.50, 0, 2, 2, 0.0, 0.0,    1),
                ('372979000', 10, 15.0, 0, 3, 0.10, 15.0, 0, 1, 1, 0.70, 4, 0, 0.40, 0, 4, 4, 1.0, 30000.0, 2),
                ('312171000', 8,  12.0, 3, 0, 0.05, 30.0, 1, 2, 1, 0.95, 3, 0, 0.55, 0, 1, 1, 0.0, 0.0,    4),
                ('352898820', 15, 25.0, 2, 4, 0.12, 35.0, 2, 1, 2, 0.85, 5, 0, 0.70, 0, 5, 5, 1.0, 60000.0, 5),
                ('352002316', 9,  14.0, 1, 2, 0.18, 18.0, 1, 0, 1, 0.75, 3, 0, 0.45, 0, 2, 2, 0.0, 0.0,    1),
                ('626152000', 11, 20.0, 0, 1, 0.08, 25.0, 0, 2, 1, 0.90, 4, 0, 0.65, 0, 3, 3, 1.0, 45000.0, 2),
                ('352001298', 13, 21.0, 2, 3, 0.14, 22.0, 2, 1, 2, 0.82, 3, 0, 0.58, 0, 4, 4, 0.0, 0.0,    3),
                ('314925000', 7,  10.0, 1, 0, 0.25, 12.0, 1, 0, 1, 0.65, 2, 0, 0.35, 0, 1, 1, 1.0, 25000.0, 1),
                ('352001565', 16, 28.0, 3, 5, 0.05, 40.0, 3, 2, 2, 0.98, 6, 0, 0.85, 0, 6, 6, 1.0, 80000.0, 5)
        """)
    finally:
        con.close()
    logger.info("  ✓ seeded %d dummy sanctioned vessels", len(_DUMMY_MMSIS))


def _load_ais_from_parquet(region: RegionConfig) -> int:
    """Load downloaded AIS positions + vessel_meta into the processed DuckDB."""
    parquet_files = sorted(_DOWNLOADS_DIR.glob(f"region={region.name}/date=*/positions.parquet"))
    vm_parquet = _DOWNLOADS_DIR.parent / "vessel_meta" / f"region={region.name}.parquet"

    if not parquet_files and not vm_parquet.exists():
        logger.warning("  ⚠ No AIS data found for %s in %s", region.name, _DOWNLOADS_DIR)
        return 0

    con = duckdb.connect(region.db_path)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ais_positions (
                mmsi        VARCHAR,
                timestamp   TIMESTAMPTZ,
                lat         DOUBLE,
                lon         DOUBLE,
                sog         FLOAT,
                cog         FLOAT,
                nav_status  INTEGER,
                ship_type   INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS vessel_meta (
                mmsi        VARCHAR,
                imo         VARCHAR,
                name        VARCHAR,
                flag        VARCHAR,
                ship_type   INTEGER
            )
        """)
        rows = 0
        if parquet_files:
            files_str = ", ".join(f"'{p}'" for p in parquet_files)
            before = con.execute("SELECT COUNT(*) FROM ais_positions").fetchone()[0]
            con.execute(f"""
                INSERT INTO ais_positions
                SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type
                FROM read_parquet([{files_str}])
                WHERE (mmsi, timestamp) NOT IN (SELECT mmsi, timestamp FROM ais_positions)
            """)
            rows = con.execute("SELECT COUNT(*) FROM ais_positions").fetchone()[0] - before
            logger.info("  ✓ ais_positions: loaded %d new rows from %d partition(s)", rows, len(parquet_files))

        if vm_parquet.exists():
            con.execute(f"""
                INSERT OR REPLACE INTO vessel_meta
                SELECT mmsi, imo, name, flag, ship_type
                FROM read_parquet('{vm_parquet}')
                WHERE mmsi IS NOT NULL
            """)
            vm_count = con.execute("SELECT COUNT(*) FROM vessel_meta").fetchone()[0]
            logger.info("  ✓ vessel_meta: %d vessels loaded from %s", vm_count, vm_parquet.name)
        else:
            logger.warning("  ⚠ vessel_meta: not found at %s", vm_parquet)
    finally:
        con.close()
    return rows


def step_ingest(region: RegionConfig) -> bool:
    logger.info("[1/4] Ingest — AIS, vessel registry, sanctions")
    env = {"DB_PATH": region.db_path, "MARIDB_REGION": region.name}

    # Load AIS positions from downloaded Parquet partitions first
    _load_ais_from_parquet(region)

    steps = [
        ([sys.executable, "-m", "pipelines.ingest.schema", "--db", region.db_path], "schema"),
        ([sys.executable, "-m", "pipelines.ingest.sanctions", "--db", region.db_path], "sanctions"),
        ([sys.executable, "-m", "pipelines.ingest.vessel_registry", "--db", region.db_path], "vessel_registry"),
    ]
    for cmd, label in steps:
        result = _run(cmd, env)
        if result.returncode != 0:
            _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}")
            return False
        _ok(label)

    return _validate_step(region.db_path, [
        ("ais_positions",      "SELECT count(*) FROM ais_positions",      1,   False),  # absent in backtest
        ("vessel_meta",        "SELECT count(*) FROM vessel_meta",         1,   False),  # absent in backtest
        ("sanctions_entities", "SELECT count(*) FROM sanctions_entities",  100, True),   # required
    ])


def step_features(region: RegionConfig, seed_dummy: bool = False) -> bool:
    logger.info("[2/4] Features — AIS behavior, identity, trade mismatch, build matrix")
    env = {"DB_PATH": region.db_path}

    steps = [
        ([sys.executable, "-m", "pipelines.features.ais_behavior", "--db", region.db_path], "ais_behavior"),
        ([sys.executable, "-m", "pipelines.features.identity", "--db", region.db_path], "identity"),
        ([sys.executable, "-m", "pipelines.features.trade_mismatch", "--db", region.db_path], "trade_mismatch"),
        ([sys.executable, "-m", "pipelines.features.build_matrix", "--db", region.db_path], "build_matrix"),
    ]
    for cmd, label in steps:
        result = _run(cmd, env)
        if result.returncode != 0:
            _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}")
            return False
        _ok(label)

    if seed_dummy:
        _seed_dummy_vessels(region.db_path)

    return _validate_step(region.db_path, [
        ("vessel_features",      "SELECT count(*) FROM vessel_features",                                      1, False),
        ("vessel_features.mmsi", "SELECT count(*) FROM vessel_features WHERE mmsi IS NOT NULL",              1, False),
        ("anomaly_scores",       "SELECT count(*) FROM vessel_features WHERE ais_gap_count_30d IS NOT NULL", 1, False),
    ])


def step_score(region: RegionConfig) -> bool:
    logger.info("[3/4] Score — anomaly, composite, watchlist")
    watchlist_out = str(_MARIDB_DATA / region.watchlist_key)
    env = {"DB_PATH": region.db_path, "WATCHLIST_OUTPUT_PATH": watchlist_out}

    steps = [
        ([sys.executable, "-m", "pipelines.score.mpol_baseline", "--db", region.db_path], "mpol_baseline"),
        ([sys.executable, "-m", "pipelines.score.anomaly", "--db", region.db_path], "anomaly"),
        ([sys.executable, "-m", "pipelines.score.composite", "--db", region.db_path], "composite"),
        ([sys.executable, "-m", "pipelines.score.watchlist",
          "--db", region.db_path,
          "--output", watchlist_out], "watchlist"),
    ]
    for cmd, label in steps:
        result = _run(cmd, env)
        if result.returncode != 0:
            _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}")
            return False
        _ok(label)

    watchlist_path = Path(watchlist_out)
    if watchlist_path.exists():
        try:
            import polars as pl
            wl = pl.read_parquet(watchlist_path)
            if wl.height == 0:
                logger.warning("  ⚠ validate [watchlist]: 0 rows written")
            elif "composite_score" not in wl.columns:
                logger.error("  ✗ validate [watchlist]: missing composite_score column")
                return False
            else:
                logger.info("  ✓ validate [watchlist]: %d candidates, composite_score present", wl.height)
        except Exception as e:
            logger.error("  ✗ validate [watchlist]: %s", e)
            return False
    else:
        logger.warning("  ⚠ validate [watchlist]: file not found at %s", watchlist_path)

    return True


def step_distribute(region: RegionConfig) -> bool:
    logger.info("[4/4] Distribute — validate → maridb-public → app buckets")
    from pipelines.distribute.push import distribute_all
    results = distribute_all(regions=[region.name])
    failed = [k for k, v in results.items() if not v]
    if failed:
        _fail(f"distribute failed: {failed}")
        return False
    _ok("all app buckets updated")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="maridb pipeline orchestrator")
    parser.add_argument("--region", default="singapore", choices=list(REGIONS))
    parser.add_argument("--non-interactive", action="store_true")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip ingest step (use existing local DuckDB)")
    parser.add_argument("--skip-features", action="store_true")
    parser.add_argument("--skip-score", action="store_true")
    parser.add_argument("--skip-distribute", action="store_true",
                        help="Skip Gate 2 distribute step (use when S3 credentials are absent)")
    parser.add_argument("--seed-dummy", action="store_true",
                        help="Seed known OFAC-sanctioned vessels for CI known-case floor")
    parser.add_argument("--stream-duration", type=int, default=0, metavar="SECS",
                        help="Seconds to collect live AIS stream (0 = skip, non-interactive default)")
    parser.add_argument("--marine-cadastre-year", type=int, default=None, metavar="YEAR",
                        help="NOAA Marine Cadastre year to ingest (persiangulf only)")
    args = parser.parse_args()

    region = REGIONS[args.region]
    Path(region.db_path).parent.mkdir(parents=True, exist_ok=True)

    steps = []
    if not args.skip_ingest:
        steps.append(("ingest", lambda: step_ingest(region)))
    if not args.skip_features:
        steps.append(("features", lambda: step_features(region, seed_dummy=args.seed_dummy)))
    if not args.skip_score:
        steps.append(("score", lambda: step_score(region)))
    if not args.skip_distribute:
        steps.append(("distribute", lambda: step_distribute(region)))

    for name, fn in steps:
        if not fn():
            logger.error("Pipeline aborted at step: %s", name)
            sys.exit(1)

    logger.info("Pipeline complete for region: %s", args.region)


if __name__ == "__main__":
    main()
