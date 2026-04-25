"""maridb pipeline orchestrator.

Runs the full ingest → features → score → validate → distribute flow.

Usage:
    uv run python scripts/run_pipeline.py --region singapore
    uv run python scripts/run_pipeline.py --region singapore --non-interactive
    uv run python scripts/run_pipeline.py --region singapore --skip-ingest
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

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
}


def _run(cmd: list[str], env_extra: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = {**os.environ, **(env_extra or {})}
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def _ok(msg: str) -> None:
    logger.info("  ✓ %s", msg)


def _fail(msg: str) -> None:
    logger.error("  ✗ %s", msg)


def step_ingest(region: RegionConfig) -> bool:
    logger.info("[1/4] Ingest — AIS, vessel registry, sanctions")
    env = {"DB_PATH": region.db_path, "MARIDB_REGION": region.name}

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
    return True


def step_features(region: RegionConfig) -> bool:
    logger.info("[2/4] Features — AIS behavior, identity, trade mismatch, build matrix")
    env = {"DB_PATH": region.db_path}

    steps = [
        (
            [sys.executable, "-m", "pipelines.features.ais_behavior",
             "--db", region.db_path,
             "--bbox", *[str(x) for x in region.bbox]],
            "ais_behavior",
        ),
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
    args = parser.parse_args()

    region = REGIONS[args.region]
    Path(region.db_path).parent.mkdir(parents=True, exist_ok=True)

    steps = []
    if not args.skip_ingest:
        steps.append(("ingest", lambda: step_ingest(region)))
    if not args.skip_features:
        steps.append(("features", lambda: step_features(region)))
    if not args.skip_score:
        steps.append(("score", lambda: step_score(region)))
    steps.append(("distribute", lambda: step_distribute(region)))

    for name, fn in steps:
        if not fn():
            logger.error("Pipeline aborted at step: %s", name)
            sys.exit(1)

    logger.info("Pipeline complete for region: %s", args.region)


if __name__ == "__main__":
    main()
