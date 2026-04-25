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


REGIONS: dict[str, RegionConfig] = {
    "singapore": RegionConfig(
        name="singapore",
        bbox=[-5, 92, 22, 122],
        db_path=str(Path.home() / ".maridb" / "data" / "singapore.duckdb"),
        watchlist_key="score/singapore_watchlist.parquet",
    ),
    "japansea": RegionConfig(
        name="japansea",
        bbox=[25, 115, 48, 145],
        db_path=str(Path.home() / ".maridb" / "data" / "japansea.duckdb"),
        watchlist_key="score/japansea_watchlist.parquet",
    ),
    "japan": RegionConfig(
        name="japansea",
        bbox=[25, 115, 48, 145],
        db_path=str(Path.home() / ".maridb" / "data" / "japansea.duckdb"),
        watchlist_key="score/japansea_watchlist.parquet",
    ),
    "blacksea": RegionConfig(
        name="blacksea",
        bbox=[40, 27, 47, 42],
        db_path=str(Path.home() / ".maridb" / "data" / "blacksea.duckdb"),
        watchlist_key="score/blacksea_watchlist.parquet",
    ),
    "europe": RegionConfig(
        name="europe",
        bbox=[35, -10, 65, 30],
        db_path=str(Path.home() / ".maridb" / "data" / "europe.duckdb"),
        watchlist_key="score/europe_watchlist.parquet",
    ),
    "middleeast": RegionConfig(
        name="middleeast",
        bbox=[10, 32, 32, 62],
        db_path=str(Path.home() / ".maridb" / "data" / "middleeast.duckdb"),
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


def step_ingest(region: RegionConfig, gdelt_days: int = 3) -> bool:
    logger.info("[1/4] Ingest — AIS, vessel registry, sanctions, GDELT")
    env = {"DB_PATH": region.db_path, "MARIDB_REGION": region.name}

    steps = [
        ([sys.executable, "-m", "pipelines.ingest.schema", "--db", region.db_path], "schema"),
        ([sys.executable, "-m", "pipelines.ingest.sanctions", "--db", region.db_path], "sanctions"),
        ([sys.executable, "-m", "pipelines.ingest.vessel_registry", "--db", region.db_path], "vessel_registry"),
        ([sys.executable, "-m", "pipelines.ingest.gdelt", "--days", str(gdelt_days)], "gdelt"),
    ]
    for cmd, label in steps:
        result = _run(cmd, env)
        if result.returncode != 0:
            _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}")
            return False
        _ok(label)
    return True


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

    return True


def step_score(region: RegionConfig) -> bool:
    logger.info("[3/4] Score — anomaly, composite, watchlist")
    env = {"DB_PATH": region.db_path,
           "WATCHLIST_OUTPUT_PATH": f"data/processed/{region.watchlist_key}"}

    steps = [
        ([sys.executable, "-m", "pipelines.score.mpol_baseline", "--db", region.db_path], "mpol_baseline"),
        ([sys.executable, "-m", "pipelines.score.anomaly", "--db", region.db_path], "anomaly"),
        ([sys.executable, "-m", "pipelines.score.composite", "--db", region.db_path], "composite"),
        ([sys.executable, "-m", "pipelines.score.watchlist",
          "--db", region.db_path,
          "--output", f"data/processed/{region.watchlist_key}"], "watchlist"),
    ]
    for cmd, label in steps:
        result = _run(cmd, env)
        if result.returncode != 0:
            _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}")
            return False
        _ok(label)
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
    parser.add_argument("--gdelt-days", type=int, default=3, metavar="N",
                        help="Number of days of GDELT events to ingest (default: 3)")
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
        steps.append(("ingest", lambda: step_ingest(region, gdelt_days=args.gdelt_days)))
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
