"""Sync processed pipeline artifacts to/from Cloudflare R2 (or any S3-compatible store).

Storage layout in R2
--------------------
  maridb-public/                ← maridb master data bucket (no sub-prefix needed)
    latest                        ← plain-text file: "20260412T120000Z"
    20260412T120000Z.zip           ← single generation zip (1 kept by default)
    gdelt.lance.zip                ← shared; push separately with `push-gdelt`

Each generation is a single .zip file, so push/pull is always 1 object.
3 generations are kept by default (--keep 3): ~2 GB/snapshot × 3 ≈ 6 GB,
well within the 10 GB R2 free tier.  Pass --keep N to adjust.

The bucket is fully public (Cloudflare R2 → Settings → Public Access).
Users can pull without any credentials — just set S3_BUCKET and S3_ENDPOINT
in .env.

Why gdelt.lance is separate
---------------------------
At 1.2 GB it dominates the bucket.  It only changes when GDELT news data is
re-ingested, not on every pipeline run.  Keeping it outside the rotation
avoids duplication and keeps snapshot pushes fast.

Files included in snapshots
---------------------------
  INCLUDED  — what the API reads at runtime:
    *.duckdb                 (all except backtest_demo, public_eval)
    candidate_watchlist.parquet
    {region}_watchlist.parquet × 5
    causal_effects.parquet
    {region}_causal_effects.parquet × 5
    validation_metrics.json
    {region}_graph/  × 5    (Lance Graph ownership chains)

  EXCLUDED  — intermediate pipeline artefacts (not read by the API):
    anomaly_scores.parquet, composite_scores.parquet, mpol_baseline.parquet
    mpol_graph/              (used during feature engineering, not serving)

  EXCLUDED from rotation zip — distributed separately or not at all:
    public_eval.duckdb       distributed as a standalone R2 object (push/pull-sanctions-db)
    backtest_demo.duckdb     local test fixture only
    backtest_*.json, evaluation_manifest_*.json, backtracking_report.*
    eval_labels_public_*.csv, prelabel_evaluation.json, public_eval_metadata.json
    *.bak

  EXCLUDED  — Lance internal history (not needed to read the dataset):
    */_transactions/*        write-coordination logs, only needed during writes
    */_versions/*.manifest   old version manifests; only the latest is kept per dataset

Commands
--------
  push                upload snapshot as a single zip to R2, prune old zips
  pull                download + extract latest (or named) snapshot zip → data/processed/
  push-gdelt          upload gdelt.lance as gdelt.lance.zip (run after re-ingesting GDELT data)
  pull-gdelt          download + extract gdelt.lance.zip → data/processed/gdelt.lance
  push-sanctions-db   upload public_eval.duckdb (OpenSanctions DB) to R2
  pull-sanctions-db   download public_eval.duckdb from R2 — needed for integration tests
  push-watchlists     upload *_watchlist.parquet files as watchlists.zip (<1 MB) — run after
                      a real pipeline run so CI can pull real watchlists for the backtest
  pull-watchlists     download watchlists.zip from R2 and extract into data/processed/ — used
                      by data-publish CI job (replaces seeded pipeline run)
  push-demo           upload fixed-key demo bundle (candidate_watchlist.parquet,
                      composite_scores.parquet, causal_effects.parquet,
                      validation_metrics.json) to R2 — requires credentials; run after
                      a real pipeline run or from the data-publish CI job
  pull-demo           download the demo bundle from R2 into data/processed/ — no credentials
                      required; intended for developers who want to run the dashboard
                      without running the full pipeline locally
  pull-reviews        download merged reviews from R2 and upsert into the local DuckDB
                      vessel_reviews table (read-only; merging is a commercial feature)
                      AWS_SECRET_ACCESS_KEY — same key used for maridb-public)
                      bucket into _inputs/custom_feeds/ — same AWS_* credentials; skips
                      gracefully when absent (forks / local dev without access)
  push-ducklake-public  upload DuckLake catalog.duckdb + data/ Parquet files to
                      maridb-public/ (overwrites on every run — no rotation)
                      skips files that are older than the local copy
  list                show all snapshot zips and shared objects in R2

Env vars (loaded from .env automatically)
------------------------------------------
  S3_BUCKET               R2 bucket name. Default: maridb-public
  S3_ENDPOINT             R2 endpoint URL. Default: maridb-public R2 endpoint
  AWS_REGION              Default: "auto" (correct for R2)
  AWS_ACCESS_KEY_ID       R2 access key ID (required for push commands and pull-custom-feeds)
  AWS_SECRET_ACCESS_KEY   R2 secret access key (required for push commands and pull-custom-feeds)
                          The same key must have Object Read & Write on both maridb-public
                          they only pull from the public bucket anonymously.

Examples
--------
  uv run python scripts/sync_r2.py push                      # push new zip, prune old
  uv run python scripts/sync_r2.py push-gdelt                # upload/update gdelt.lance.zip
  uv run python scripts/sync_r2.py push-sanctions-db         # upload/update public_eval.duckdb
  uv run python scripts/sync_r2.py push-watchlists           # upload *_watchlist.parquet (<1 MB)
  uv run python scripts/sync_r2.py push-demo                 # upload demo bundle (CI runs this)
  uv run python scripts/sync_r2.py pull                      # pull latest (no credentials needed)
  uv run python scripts/sync_r2.py pull --timestamp 20260411T080000Z
  uv run python scripts/sync_r2.py pull-gdelt                # pull gdelt.lance.zip
  uv run python scripts/sync_r2.py pull-sanctions-db         # pull public_eval.duckdb for tests
  uv run python scripts/sync_r2.py pull-watchlists           # pull watchlists.zip (used by CI)
  uv run python scripts/sync_r2.py pull-demo                 # pull demo bundle (no credentials)
  uv run python scripts/sync_r2.py pull-reviews              # download merged public reviews from R2
  uv run python scripts/sync_r2.py push-ducklake-public      # upload DuckLake catalog to public bucket
  uv run python scripts/sync_r2.py list                      # show all generations in R2
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import sys
import tempfile
import zipfile as zipmod
from datetime import UTC, datetime
from pathlib import Path

# Load .env before resolving any defaults that read env vars.
try:
    from dotenv import load_dotenv as _load_dotenv

    _load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def _resolve_default_data_dir() -> str:
    """Return the default data directory.

    Resolution order:
    1. ``MARIDB_DATA_DIR`` env var (explicit override)
    2. ``~/.indago/data`` (canonical user-level data location)
    """
    import os as _os

    if explicit := _os.getenv("MARIDB_DATA_DIR"):
        return str(Path(explicit).expanduser())
    return str(Path.home() / ".maridb" / "data")


_DEFAULT_DATA_DIR = _resolve_default_data_dir()
_DEFAULT_KEEP = 3  # ~2 GB/snapshot × 3 ≈ 6 GB; well within the 10 GB free tier
_DEFAULT_BUCKET = "maridb-public"
_DEFAULT_ENDPOINT = "https://b8a0b09feb89390fb6e8cf4ef9294f48.r2.cloudflarestorage.com"
# Public custom domain — used for unauthenticated (curl/urllib) downloads only.
# The S3 API (push, pull with credentials) still uses _DEFAULT_ENDPOINT.
_PUBLIC_BASE_URL = "https://maridb-public.edgesentry.io"
# The maridb-public bucket contains all pipeline output (master data lake).
# so no sub-prefix is needed — all objects live at the bucket root.
_LATEST_KEY = "latest"  # plain-text pointer to newest timestamp
_GDELT_R2_KEY = "gdelt.lance.zip"  # single zip for gdelt
_SANCTIONS_DB_R2_KEY = "public_eval.duckdb"  # OpenSanctions DB; separate from rotation zip
_WATCHLISTS_R2_KEY = "watchlists.zip"  # lightweight bundle of *_watchlist.parquet files
_DEMO_R2_KEY = "demo.zip"  # fixed-key public demo bundle; overwritten on every push-demo

# DuckLake catalog keys — public bucket (root)
# catalog.duckdb is a fixed-key file overwritten on every push-ducklake-* run.
_DUCKLAKE_CATALOG_KEY = "catalog.duckdb"  # public: maridb-public/catalog.duckdb
_DUCKLAKE_DATA_PREFIX = "data/"  # public: maridb-public/data/...

# Private bucket for proprietary customer feeds (e.g. Cap Vista MPOL data).
# Uses separate credentials so it is never confused with the public bucket.
# Custom domain for the private bucket — used as the S3 endpoint for CI reads/writes.
# The domain IS the bucket, so S3 paths are bare keys (no bucket-name prefix).

# Files included in the demo bundle — lightweight artifacts that let developers run the
# dashboard without re-running the full pipeline.  No heavy DuckDB or Lance files.
_DEMO_FILES = [
    "candidate_watchlist.parquet",
    "composite_scores.parquet",
    "causal_effects.parquet",
    "score_history.parquet",
    "validation_metrics.json",
]

# Maps user-facing region name → file prefix used in data/processed/
# e.g. "japan" → files are japansea.duckdb, japansea_graph/, japansea_watchlist.parquet
_REGION_PREFIX: dict[str, str] = {
    "singapore": "singapore",
    "japan": "japansea",
    "middleeast": "middleeast",
    "europe": "europe",
    "persiangulf": "persiangulf",
    "gulfofguinea": "gulfofguinea",
    "gulfofaden": "gulfofaden",
    "gulfofmexico": "gulfofmexico",
}

# Files always downloaded regardless of region (shared by the API across all regions)
_SHARED_FILES = {
    "mpol.duckdb",
    "candidate_watchlist.parquet",
    "causal_effects.parquet",
    "validation_metrics.json",
}
_GDELT_LOCAL_DIR = "gdelt.lance"
_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

# Files / patterns excluded from snapshots (intermediate and evaluation artefacts).
# Matched against the relative path from data/processed/.
_SNAPSHOT_EXCLUDE: list[str] = [
    # Intermediate pipeline outputs (not read by the API)
    "anomaly_scores.parquet",
    "composite_scores.parquet",
    "mpol_baseline.parquet",
    "mpol_graph/*",  # Lance graph used during feature engineering only
    # Evaluation / backtest artefacts
    "backtest_demo.duckdb",
    "public_eval.duckdb",  # distributed separately — see push-sanctions-db / pull-sanctions-db
    "backtest_*.json",
    "backtracking_report.*",
    "evaluation_manifest_*.json",
    "eval_labels_public_*.csv",
    "prelabel_evaluation.json",
    "public_eval_metadata.json",
    # GDELT — stored separately outside snapshots
    "gdelt.lance",
    "gdelt.lance/*",
    # Scratch / backup files
    "*.bak",
    ".gitkeep",
]


def _is_excluded(rel: str) -> bool:
    """Return True if the relative path should be excluded from snapshots."""
    for pattern in _SNAPSHOT_EXCLUDE:
        if fnmatch.fnmatch(rel, pattern):
            return True
        # Also match directory prefixes (e.g. "gdelt.lance/v1/..." matches "gdelt.lance/*")
        if fnmatch.fnmatch(rel.split("/")[0], pattern.rstrip("/*")):
            first = pattern.rstrip("/*")
            if rel == first or rel.startswith(first + "/"):
                return True
    # Lance transaction logs — only needed during writes, not for read-only use
    parts = rel.split("/")
    if "_transactions" in parts:
        return True
    return False


def _collect_snapshot_files(data_dir: Path) -> dict[str, int]:
    """List files for a snapshot, keeping only the latest Lance manifest per dataset.

    Lance stores version history under ``<dataset>/_versions/*.manifest``.
    Old manifests are dead weight for read-only deployments — only the latest
    (highest filename) is needed to open each dataset.  This function scans the
    _versions directories and drops all but the newest manifest.
    """
    # First pass: collect everything that passes the basic exclusion filter.
    all_files = _list_local(data_dir, exclude_fn=_is_excluded)

    # Second pass: for each _versions/ directory found, keep only the latest manifest.
    # Group manifest paths by their parent _versions/ directory.
    versions_dirs: dict[str, list[str]] = {}
    for rel in list(all_files):
        parts = rel.split("/")
        if "_versions" in parts:
            vi = parts.index("_versions")
            versions_dir = "/".join(parts[: vi + 1])
            versions_dirs.setdefault(versions_dir, []).append(rel)

    for versions_dir, manifests in versions_dirs.items():
        if len(manifests) <= 1:
            continue
        # Keep only the manifest with the lexicographically largest filename
        # (Lance version numbers encoded as zero-padded uint64 strings).
        latest = max(manifests, key=lambda p: Path(p).name)
        for m in manifests:
            if m != latest:
                del all_files[m]

    return all_files


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------


def _build_r2_fs(
    anonymous: bool = False, endpoint: str | None = None
):  # -> pyarrow.fs.S3FileSystem
    """Build an S3FileSystem for R2.

    Pass ``anonymous=True`` for public-bucket reads that need no credentials.
    Pass ``endpoint`` to override the default R2 endpoint (e.g. a custom domain).
    """
    import pyarrow.fs as pafs

    endpoint = endpoint or os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    if not endpoint:
        # Plain AWS S3
        kwargs: dict = {"region": os.getenv("AWS_REGION", "us-east-1")}
        if not anonymous:
            kwargs["access_key"] = os.environ["AWS_ACCESS_KEY_ID"]
            kwargs["secret_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
        return pafs.S3FileSystem(anonymous=anonymous, **kwargs)

    host = endpoint.split("://", 1)[-1].rstrip("/")
    scheme = "https" if endpoint.startswith("https://") else "http"
    kwargs = {
        "endpoint_override": host,
        "scheme": scheme,
        "region": os.getenv("AWS_REGION", "auto"),
    }
    if not anonymous:
        kwargs["access_key"] = os.environ["AWS_ACCESS_KEY_ID"]
        kwargs["secret_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
    return pafs.S3FileSystem(anonymous=anonymous, **kwargs)


def _list_local(data_dir: Path, exclude_fn=None) -> dict[str, int]:
    """Return {relative_path: size_bytes} for files under data_dir."""
    result: dict[str, int] = {}
    for p in sorted(data_dir.rglob("*")):
        if not p.is_file():
            continue
        rel = str(p.relative_to(data_dir))
        if exclude_fn and exclude_fn(rel):
            continue
        result[rel] = p.stat().st_size
    return result


def _r2_zip_path(bucket: str, timestamp: str) -> str:
    return f"{bucket}/{timestamp}.zip"


def _read_latest(fs, bucket: str) -> str | None:
    try:
        with fs.open_input_stream(f"{bucket}/{_LATEST_KEY}") as f:
            return f.read().decode().strip() or None
    except Exception:
        return None


def _write_latest(fs, bucket: str, timestamp: str) -> None:
    with fs.open_output_stream(f"{bucket}/{_LATEST_KEY}") as f:
        f.write(timestamp.encode())


def _list_timestamps(fs, bucket: str) -> list[str]:
    """Return timestamp names (without .zip) sorted oldest-first.

    Uses recursive=True and filters to root-level files only because
    pyarrow S3FileSystem does not enumerate flat (no-slash) keys when
    recursive=False is set on a bucket-root FileSelector.
    """
    import pyarrow.fs as pafs

    selector = pafs.FileSelector(f"{bucket}/", recursive=True)
    try:
        infos = fs.get_file_info(selector)
    except Exception:
        return []
    pat = re.compile(r"^\d{8}T\d{6}Z\.zip$")
    # Keep only root-level files: path == bucket/filename (no extra slash)
    names = [
        Path(i.path).name for i in infos if i.type == pafs.FileType.File and i.path.count("/") == 1
    ]
    return sorted(n.removesuffix(".zip") for n in names if pat.match(n))


def _delete_timestamp(fs, bucket: str, timestamp: str) -> int:
    """Delete the snapshot zip for a given timestamp; return 1 on success."""
    r2_path = _r2_zip_path(bucket, timestamp)
    try:
        fs.delete_file(r2_path)
        return 1
    except Exception:
        return 0


def _upload_file(fs, local_path: Path, r2_path: str, _retries: int = 3) -> int:
    # R2 occasionally returns InvalidPart during CompleteMultipartUpload — a
    # transient condition where uploaded parts are not found on completion.
    # Retry the entire upload from scratch; do not resume partial parts.
    if _retries < 1:
        raise ValueError("_retries must be >= 1")
    for attempt in range(1, _retries + 1):
        try:
            with local_path.open("rb") as src:
                with fs.open_output_stream(r2_path) as dst:
                    total = 0
                    while chunk := src.read(_CHUNK_SIZE):
                        dst.write(chunk)
                        total += len(chunk)
            return total
        except OSError as exc:
            if "InvalidPart" in str(exc) and attempt < _retries:
                print(
                    f"  R2 InvalidPart on attempt {attempt}/{_retries} — retrying upload ...",
                    flush=True,
                )
                continue
            raise


def _download_file(fs, r2_path: str, local_path: Path) -> int:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with fs.open_input_stream(r2_path) as src:
        with local_path.open("wb") as dst:
            total = 0
            while chunk := src.read(_CHUNK_SIZE):
                dst.write(chunk)
                total += len(chunk)
    return total


def _region_filter_names(names: list[str], regions: list[str]) -> list[str]:
    """Filter zip entry names to shared files + per-region files only."""
    prefixes = tuple(f"{_REGION_PREFIX[r]}_" for r in regions)
    stems = tuple(f"{_REGION_PREFIX[r]}." for r in regions)
    result = []
    for name in names:
        top = name.split("/")[0]
        if top in _SHARED_FILES:
            result.append(name)
        elif any(top.startswith(p) for p in prefixes):
            result.append(name)
        elif any(top.startswith(s) for s in stems):
            result.append(name)
    return result


def _create_snapshot_zip(local_files: dict[str, int], data_dir: Path, zip_path: Path) -> None:
    """Pack local_files into a ZIP_STORED archive at zip_path.

    ZIP_STORED skips redundant compression — parquet and duckdb files are
    already internally compressed, so deflating them again wastes CPU for
    negligible (or negative) size gains.
    """
    with zipmod.ZipFile(zip_path, "w", compression=zipmod.ZIP_STORED, allowZip64=True) as zf:
        for rel in sorted(local_files):
            zf.write(data_dir / rel, arcname=rel)


def _pull_zip(
    fs,
    bucket: str,
    timestamp: str,
    data_dir: Path,
    regions: list[str],
) -> int:
    """Download snapshot zip and extract region-filtered files. Returns bytes downloaded."""
    r2_path = _r2_zip_path(bucket, timestamp)

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        downloaded = _download_file(fs, r2_path, tmp_path)
        data_dir.mkdir(parents=True, exist_ok=True)

        with zipmod.ZipFile(tmp_path, "r") as zf:
            all_names = zf.namelist()
            to_extract = _region_filter_names(all_names, regions)
            for name in to_extract:
                zf.extract(name, data_dir)

        return downloaded
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_push(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    keep = args.keep

    if not data_dir.exists():
        print(f"Error: data directory does not exist: {data_dir}", file=sys.stderr)
        return 1

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    fs = _build_r2_fs()

    print(f"Scanning {data_dir} (excluding intermediate, evaluation, and stale Lance history) ...")
    local_files = _collect_snapshot_files(data_dir)

    if not local_files:
        print("No files to upload after exclusions.", file=sys.stderr)
        return 1

    total_size = sum(local_files.values())
    print(f"{len(local_files)} files ({total_size / 1_048_576:.1f} MB) → {timestamp}.zip\n")

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        print("Creating zip archive (ZIP_STORED — no redundant compression) ...")
        _create_snapshot_zip(local_files, data_dir, tmp_path)
        zip_size = tmp_path.stat().st_size
        print(f"Archive: {zip_size / 1_048_576:.1f} MB\n")

        r2_path = _r2_zip_path(bucket, timestamp)
        print(f"Uploading {timestamp}.zip ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    _write_latest(fs, bucket, timestamp)

    print(f"\n{timestamp}.zip pushed ({uploaded / 1_048_576:.1f} MB).")
    print(f"latest → {timestamp}")

    # Prune old generations beyond keep limit
    all_ts = _list_timestamps(fs, bucket)
    to_delete = all_ts[:-keep] if len(all_ts) > keep else []
    if to_delete:
        print(f"\nPruning {len(to_delete)} old generation(s) (keeping {keep}):")
        for old in to_delete:
            n = _delete_timestamp(fs, bucket, old)
            print(f"  deleted {old}.zip" + (" ✓" if n else " (not found)"))
    else:
        print(f"\n{len(all_ts)}/{keep} generation slot(s) used — nothing to prune.")

    print("\nTip: to also push updated GDELT news data, run:")
    print("  uv run python scripts/sync_r2.py push-gdelt")
    return 0


def cmd_pull(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)

    # Use anonymous access when credentials are absent (public bucket)
    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    # Resolve timestamp
    timestamp = args.timestamp
    if not timestamp:
        timestamp = _read_latest(fs, bucket)
        if not timestamp:
            print(
                "No 'latest' pointer found in R2. Run a push first or pass --timestamp explicitly.",
                file=sys.stderr,
            )
            return 1
        print(f"Latest: {timestamp}")

    # Parse and validate regions
    if args.region.lower() == "all":
        regions = list(_REGION_PREFIX)
    else:
        regions = [r.strip().lower() for r in args.region.split(",")]
        unknown = [r for r in regions if r not in _REGION_PREFIX]
        if unknown:
            print(
                f"Error: unknown region(s): {', '.join(unknown)}\n"
                f"Available: {', '.join(_REGION_PREFIX)}",
                file=sys.stderr,
            )
            return 1

    print(f"Downloading {timestamp}.zip ...")
    try:
        downloaded = _pull_zip(fs, bucket, timestamp, data_dir, regions)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"\nDone. {downloaded / 1_048_576:.1f} MB downloaded, extracted to {data_dir}/")
    print(f"Region(s): {', '.join(regions)}")
    print("\nOptional extras:")
    print(
        "  uv run python scripts/sync_r2.py pull-sanctions-db  # OpenSanctions DB for integration tests"
    )
    print(
        "  uv run python scripts/sync_r2.py pull-gdelt         # GDELT news data (analyst briefs)"
    )
    print("\nOpen the dashboard:")
    print("  https://arktrace.edgesentry.io")
    return 0


def cmd_push_gdelt(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    gdelt_dir = data_dir / _GDELT_LOCAL_DIR

    if not gdelt_dir.exists():
        print(f"Error: {gdelt_dir} does not exist. Run the GDELT ingest first:", file=sys.stderr)
        print("  uv run python src/ingest/gdelt.py", file=sys.stderr)
        return 1

    fs = _build_r2_fs()
    r2_path = f"{bucket}/{_GDELT_R2_KEY}"

    if not args.force:
        import pyarrow.fs as pafs

        try:
            infos = fs.get_file_info([r2_path])
            if infos[0].type == pafs.FileType.File:
                print(
                    f"gdelt.lance.zip already exists in R2 ({infos[0].size / 1_048_576:.1f} MB). "
                    "Use --force to re-upload."
                )
                return 0
        except Exception:
            pass

    print(f"Scanning {gdelt_dir} ...")
    local_files = _list_local(gdelt_dir)
    if not local_files:
        print("gdelt.lance directory is empty.", file=sys.stderr)
        return 1

    total_size = sum(local_files.values())
    print(f"{len(local_files)} files ({total_size / 1_048_576:.1f} MB) → gdelt.lance.zip\n")

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        print("Creating gdelt.lance.zip (ZIP_STORED) ...")
        with zipmod.ZipFile(tmp_path, "w", compression=zipmod.ZIP_STORED, allowZip64=True) as zf:
            for rel in sorted(local_files):
                zf.write(gdelt_dir / rel, arcname=rel)
        zip_size = tmp_path.stat().st_size
        print(f"Archive: {zip_size / 1_048_576:.1f} MB\n")

        print("Uploading gdelt.lance.zip ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"\nDone. Uploaded {uploaded / 1_048_576:.1f} MB to R2 {r2_path}")
    return 0


def cmd_pull_gdelt(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    gdelt_dir = data_dir / _GDELT_LOCAL_DIR

    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)
    r2_path = f"{bucket}/{_GDELT_R2_KEY}"

    import pyarrow.fs as pafs

    try:
        infos = fs.get_file_info([r2_path])
        if infos[0].type == pafs.FileType.NotFound:
            raise FileNotFoundError
        zip_size_mb = infos[0].size / 1_048_576
    except Exception:
        print(
            "No gdelt.lance.zip found in R2. Push it first with:\n"
            "  uv run python scripts/sync_r2.py push-gdelt",
            file=sys.stderr,
        )
        return 1

    print(f"Downloading gdelt.lance.zip ({zip_size_mb:.1f} MB) ...")

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        downloaded = _download_file(fs, r2_path, tmp_path)
        gdelt_dir.mkdir(parents=True, exist_ok=True)
        print("Extracting ...")
        with zipmod.ZipFile(tmp_path, "r") as zf:
            zf.extractall(gdelt_dir)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"\nDone. {downloaded / 1_048_576:.1f} MB downloaded to {gdelt_dir}/")
    return 0


def cmd_push_sanctions_db(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    local_path = Path(args.data_dir) / "public_eval.duckdb"

    if not local_path.exists():
        print(
            f"Error: {local_path} does not exist. Generate it first:\n"
            "  uv run python scripts/prepare_public_sanctions_db.py",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()
    r2_path = f"{bucket}/{_SANCTIONS_DB_R2_KEY}"

    if not args.force:
        import pyarrow.fs as pafs

        try:
            infos = fs.get_file_info([r2_path])
            if infos[0].type == pafs.FileType.File:
                print(
                    f"public_eval.duckdb already exists in R2 ({infos[0].size / 1_048_576:.1f} MB). "
                    "Use --force to re-upload."
                )
                return 0
        except Exception:
            pass

    size_mb = local_path.stat().st_size / 1_048_576
    print(f"Uploading public_eval.duckdb ({size_mb:.1f} MB) → R2 {r2_path} ...")
    uploaded = _upload_file(fs, local_path, r2_path)
    print(f"Done. {uploaded / 1_048_576:.1f} MB uploaded.")
    return 0


def cmd_pull_sanctions_db(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    local_path = Path(args.data_dir) / "public_eval.duckdb"

    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)
    r2_path = f"{bucket}/{_SANCTIONS_DB_R2_KEY}"

    import pyarrow.fs as pafs

    try:
        infos = fs.get_file_info([r2_path])
        if infos[0].type == pafs.FileType.NotFound:
            raise FileNotFoundError
        size_mb = infos[0].size / 1_048_576
    except Exception:
        print(
            "No public_eval.duckdb found in R2. Push it first with:\n"
            "  uv run python scripts/sync_r2.py push-sanctions-db",
            file=sys.stderr,
        )
        return 1

    print(f"Downloading public_eval.duckdb ({size_mb:.1f} MB) ...")
    downloaded = _download_file(fs, r2_path, local_path)
    print(f"Done. {downloaded / 1_048_576:.1f} MB downloaded to {local_path}")
    print("\nYou can now run the public-data integration test:")
    print("  RUN_PUBLIC_DATA_TESTS=1 uv run pytest tests/test_public_data_backtest_integration.py")
    return 0


def cmd_push_watchlists(args: argparse.Namespace) -> int:
    """Upload *_watchlist.parquet + candidate_watchlist.parquet as watchlists.zip.

    The resulting zip is tiny (< 1 MB) and is pulled by the data-publish CI job
    via ``pull-watchlists`` so the backtest can use real watchlists instead of
    seeded dummy data.
    """
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    r2_path = f"{bucket}/{_WATCHLISTS_R2_KEY}"

    # Search both data_dir root and data_dir/score/ (where run_pipeline.py writes them).
    # Use rglob to find all *_watchlist.parquet recursively; arcname=f.name in the zip
    # ensures they all extract to the top level of data_dir on pull.
    seen: set[Path] = set()
    watchlist_files = []
    for f in sorted(data_dir.rglob("*_watchlist.parquet")):
        if f not in seen:
            seen.add(f)
            watchlist_files.append(f)

    if not watchlist_files:
        print(
            f"No watchlist parquets found in {data_dir}. "
            "Run the pipeline for at least one region first.",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with zipmod.ZipFile(tmp_path, "w", compression=zipmod.ZIP_STORED) as zf:
            for f in watchlist_files:
                zf.write(f, arcname=f.name)
                print(f"  + {f.name} ({f.stat().st_size / 1024:.1f} KB)")
        size_mb = tmp_path.stat().st_size / 1_048_576
        print(f"Uploading watchlists.zip ({size_mb:.2f} MB) → R2 {r2_path} ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {uploaded / 1_048_576:.2f} MB uploaded.")
    return 0


def cmd_pull_watchlists(args: argparse.Namespace) -> int:
    """Download watchlists.zip from R2 and extract into data/processed/."""
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    r2_path = f"{bucket}/{_WATCHLISTS_R2_KEY}"

    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    import pyarrow.fs as pafs

    try:
        infos = fs.get_file_info([r2_path])
        if infos[0].type == pafs.FileType.NotFound:
            raise FileNotFoundError
        size_mb = infos[0].size / 1_048_576
    except Exception:
        print(
            "No watchlists.zip found in R2. Push real watchlists first with:\n"
            "  uv run python scripts/sync_r2.py push-watchlists",
            file=sys.stderr,
        )
        return 1

    print(f"Downloading watchlists.zip ({size_mb:.2f} MB) ...")
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        downloaded = _download_file(fs, r2_path, tmp_path)
        data_dir.mkdir(parents=True, exist_ok=True)
        with zipmod.ZipFile(tmp_path, "r") as zf:
            zf.extractall(data_dir)
            names = zf.namelist()
        print(f"Extracted {len(names)} files to {data_dir}/: {', '.join(names)}")
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {downloaded / 1_048_576:.2f} MB downloaded.")
    return 0


def cmd_push_demo(args: argparse.Namespace) -> int:
    """Upload the fixed-key demo bundle to R2 (requires credentials).

    Overwrites demo.zip on every run — there is no rotation; the file always
    reflects the most recent pipeline run.  Intended to be called from the
    data-publish CI job after the main push step.
    """
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    r2_path = f"{bucket}/{_DEMO_R2_KEY}"

    missing = [f for f in _DEMO_FILES if not (data_dir / f).exists()]
    if missing:
        print(
            f"Error: the following demo files are missing from {data_dir}:\n"
            + "".join(f"  {f}\n" for f in missing)
            + "Run the pipeline first or check _DEMO_FILES in sync_r2.py.",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with zipmod.ZipFile(tmp_path, "w", compression=zipmod.ZIP_STORED) as zf:
            for name in _DEMO_FILES:
                local = data_dir / name
                zf.write(local, arcname=name)
                print(f"  + {name} ({local.stat().st_size / 1024:.1f} KB)")
        size_mb = tmp_path.stat().st_size / 1_048_576
        print(f"Uploading demo.zip ({size_mb:.2f} MB) → R2 {r2_path} ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {uploaded / 1_048_576:.2f} MB uploaded.")
    print(
        "\nDevelopers can now fetch the demo bundle without credentials:\n"
        "  uv run python scripts/sync_r2.py pull-demo\n"
        "  # or: bash scripts/fetch_demo_data.sh"
    )
    return 0


def cmd_pull_demo(args: argparse.Namespace) -> int:
    """Download the demo bundle from the public custom domain (no credentials required).

    Uses a plain HTTPS GET against maridb-public.edgesentry.io so callers
    don't need pyarrow or R2 credentials — only stdlib urllib is required.
    """
    import urllib.error
    import urllib.request

    data_dir = Path(args.data_dir)
    url = f"{_PUBLIC_BASE_URL}/{_DEMO_R2_KEY}"

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        print(f"Downloading {_DEMO_R2_KEY} from {_PUBLIC_BASE_URL} …")
        urllib.request.urlretrieve(url, tmp_path)
        size_mb = tmp_path.stat().st_size / 1_048_576
        print(f"  {size_mb:.2f} MB downloaded.")
        data_dir.mkdir(parents=True, exist_ok=True)
        with zipmod.ZipFile(tmp_path, "r") as zf:
            names = zf.namelist()
            zf.extractall(data_dir)
        print(f"Extracted {len(names)} files to {data_dir}/: {', '.join(names)}")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            print(
                "demo.zip not found at public URL. The app owner needs to run:\n"
                "  uv run python scripts/run_pipeline.py --region singapore --non-interactive\n"
                "  uv run python scripts/sync_r2.py push-demo",
                file=sys.stderr,
            )
        else:
            print(f"HTTP {exc.code} downloading demo bundle: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error downloading demo bundle: {exc}", file=sys.stderr)
        return 1
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {size_mb:.2f} MB downloaded.")
    print("\nData synced. Open the dashboard:\n  https://arktrace.edgesentry.io")
    return 0


def cmd_push_ducklake_public(args: argparse.Namespace) -> int:
    """Upload DuckLake catalog.duckdb + data/ Parquet files to maridb-public/.

    Objects written:
      maridb-public/catalog.duckdb          ← DuckLake metadata catalog
      maridb-public/data/main/<table>/*.parquet  ← materialised Parquet files

    Run ``scripts/checkpoint_ducklake.py`` first to materialise the catalog.
    The catalog is overwritten on every push (no rotation).
    """
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    catalog_dir = Path(args.catalog_dir)
    catalog_file = catalog_dir / "catalog.duckdb"
    parquet_dir = catalog_dir / "data"

    if not catalog_file.exists():
        print(
            f"Error: {catalog_file} not found.  Run checkpoint_ducklake.py first:\n"
            "  uv run python scripts/checkpoint_ducklake.py",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()

    # Upload catalog.duckdb
    catalog_r2 = f"{bucket}/{_DUCKLAKE_CATALOG_KEY}"
    sz = catalog_file.stat().st_size
    print(f"Uploading catalog.duckdb ({sz / 1024:.1f} KB) → {catalog_r2} ...")
    _upload_file(fs, catalog_file, catalog_r2)
    print("  ✓")

    # Upload all Parquet files under data/
    parquets = sorted(parquet_dir.rglob("*.parquet")) if parquet_dir.exists() else []
    if not parquets:
        print("[warn] No Parquet files found under data/ — CHECKPOINT may not have run.")
    else:
        total_bytes = 0
        for p in parquets:
            rel = p.relative_to(catalog_dir)
            r2_path = f"{bucket}/{rel}"
            sz = p.stat().st_size
            total_bytes += sz
            print(f"  {rel}  ({sz / 1024:.1f} KB) → {r2_path} ...")
            _upload_file(fs, p, r2_path)
        print(f"Uploaded {len(parquets)} Parquet file(s) ({total_bytes / 1_048_576:.2f} MB)  ✓")

    # Upload ducklake_manifest.json — consumed by the browser OPFS sync (Phase 2)
    manifest_file = catalog_dir / "ducklake_manifest.json"
    if manifest_file.exists():
        manifest_r2 = f"{bucket}/ducklake_manifest.json"
        sz = manifest_file.stat().st_size
        print(f"Uploading ducklake_manifest.json ({sz} B) → {manifest_r2} ...")
        _upload_file(fs, manifest_file, manifest_r2)
        print("  ✓")
    else:
        print("[warn] ducklake_manifest.json not found — browser OPFS sync will not work.")

    print(
        f"\nDone. Public DuckLake catalog available at:\n"
        f"  {_PUBLIC_BASE_URL}/{_DUCKLAKE_CATALOG_KEY}\n"
        f"  {_PUBLIC_BASE_URL}/ducklake_manifest.json  ← browser OPFS sync manifest\n"
        f"  {_PUBLIC_BASE_URL}/{_DUCKLAKE_DATA_PREFIX}..."
    )
    return 0


_DUCKLAKE_AIS_CATALOG = "ducklake/catalog.duckdb"
_DUCKLAKE_AIS_DATA = "ducklake/data"
_AIS_DB_MIN_SIZE_BYTES = 1_048_576  # skip placeholder DBs smaller than 1 MB


def _ais_db_candidates(data_dir: Path, regions: list[str] | None) -> list[Path]:
    """Return regional .duckdb paths that are large enough to be non-empty.

    If *regions* is given, only those region prefixes are considered.
    Otherwise every non-excluded .duckdb in data_dir is a candidate.
    """
    _EXCLUDE_STEMS = {
        "backtest_demo",
        "public_eval",
        "mpol",
        "catalog",
    }
    candidates = []
    if regions:
        stems = [_REGION_PREFIX.get(r, r) for r in regions]
        paths = [data_dir / f"{s}.duckdb" for s in stems]
    else:
        paths = sorted(data_dir.glob("*.duckdb"))

    for p in paths:
        if not p.exists():
            continue
        if p.stem in _EXCLUDE_STEMS:
            continue
        if p.stat().st_size < _AIS_DB_MIN_SIZE_BYTES:
            continue
        candidates.append(p)
    return candidates


def _ducklake_upload(local_catalog: Path, local_data: Path, bucket: str, fs: object) -> int:
    """Upload DuckLake catalog + changed Parquet files to R2. Returns upload count."""
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    uploaded = 0

    # Upload catalog.duckdb
    r2_catalog_key = f"{bucket}/{_DUCKLAKE_AIS_CATALOG}"
    with open(local_catalog, "rb") as fh:
        with fs.open_output_stream(r2_catalog_key) as out:  # type: ignore[attr-defined]
            out.write(fh.read())
    print(f"  catalog → {r2_catalog_key}")
    uploaded += 1

    # Upload Parquet files — skip files already in R2 with same size
    existing: dict[str, int] = {}  # r2_key → size_bytes
    try:
        selector = pafs.FileSelector(f"{bucket}/{_DUCKLAKE_AIS_DATA}/", recursive=True)
        for info in fs.get_file_info(selector):
            if info.type == pafs.FileType.File:
                existing[info.path] = info.size
    except Exception:
        pass

    for parquet_file in sorted(local_data.rglob("*.parquet")):
        rel = parquet_file.relative_to(local_data)
        r2_key = f"{bucket}/{_DUCKLAKE_AIS_DATA}/{rel}"
        local_size = parquet_file.stat().st_size
        if existing.get(r2_key) == local_size:
            continue  # unchanged
        table = pq.read_table(parquet_file)
        pq.write_table(table, r2_key, filesystem=fs, compression="snappy")
        print(f"  data/{rel} ({table.num_rows:,} rows) → {r2_key}")
        uploaded += 1

    return uploaded


def cmd_push_ais_parquet(args: argparse.Namespace) -> int:
    """Export new ais_positions rows to date-partitioned Parquet, stage locally, then upload.

    Flow: raw/ais/{region}.duckdb → staging/ais/region=*/date=*/positions.parquet → R2

    Incremental by date: only uploads date partitions not already in R2.
    Staging directory acts as a local mirror before upload; already-staged
    partitions are re-uploaded if missing from R2.
    """
    import duckdb as _duckdb
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    data_dir = Path(args.data_dir)
    staging_dir = Path(args.staging_dir)
    regions: list[str] | None = (
        [r.strip() for r in args.regions.split(",")] if args.regions else None
    )
    candidates = _ais_db_candidates(data_dir, regions)
    if not candidates:
        print("No eligible AIS .duckdb files found.", file=sys.stderr)
        return 1

    fs = _build_r2_fs()
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    total_uploaded = 0

    for db_path in candidates:
        stem = db_path.stem
        # Use stem as the canonical region name in R2 paths so push and pull are consistent.
        # _REGION_PREFIX maps display names (e.g. "japan") to file stems (e.g. "japansea");
        # the R2 partition key must match the stem so pull-ais-parquet can find the files.
        region = stem
        print(f"[{region}] Reading {db_path.name} ...")
        try:
            con = _duckdb.connect(str(db_path), read_only=True)
            row_count = con.execute("SELECT COUNT(*) FROM ais_positions").fetchone()[0]
        except Exception as exc:
            print(f"  [skip] cannot open DB: {exc}", file=sys.stderr)
            continue
        from datetime import UTC as _UTC
        from datetime import datetime as _datetime

        today_str = _datetime.now(_UTC).date().isoformat()
        if row_count == 0:
            # Upload an empty Parquet for today so the validation job can confirm
            # the upload pipeline ran (file missing = pipeline broken; 0 rows = no vessels).
            r2_key = f"{bucket}/ais/region={region}/date={today_str}/positions.parquet"
            empty_table = con.execute(
                "SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type "
                "FROM ais_positions WHERE 1=0"
            ).to_arrow_table()
            local_path = (
                staging_dir / f"region={region}" / f"date={today_str}" / "positions.parquet"
            )
            local_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(empty_table, local_path, compression="snappy")
            try:
                pq.write_table(empty_table, r2_key, filesystem=fs, compression="snappy")
                print(f"  {today_str} (0 rows) -> uploaded empty sentinel to R2")
                total_uploaded += 1
            except Exception as exc:
                print(f"  [warn] failed to upload empty sentinel: {exc}", file=sys.stderr)
            continue
        dates = [
            r[0].strftime("%Y-%m-%d")
            for r in con.execute(
                "SELECT DISTINCT CAST(timezone('UTC', timestamp) AS DATE) FROM ais_positions ORDER BY 1"
            ).fetchall()
        ]
        print(f"  {row_count:,} rows across {len(dates)} date(s)")
        r2_prefix = f"{bucket}/ais/region={region}/"
        existing_r2: set[str] = set()
        try:
            for info in fs.get_file_info(pafs.FileSelector(r2_prefix, recursive=True)):
                for part in info.path.split("/"):
                    if part.startswith("date="):
                        existing_r2.add(part.removeprefix("date="))
        except Exception:
            pass
        to_upload = [d for d in dates if d not in existing_r2]
        # If today has no rows yet, upload an empty sentinel so validation
        # can confirm the pipeline ran for this region.
        if today_str not in existing_r2 and today_str not in dates:
            empty_table = con.execute(
                "SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type "
                "FROM ais_positions WHERE 1=0"
            ).to_arrow_table()
            local_path = (
                staging_dir / f"region={region}" / f"date={today_str}" / "positions.parquet"
            )
            local_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(empty_table, local_path, compression="snappy")
            r2_key = f"{bucket}/ais/region={region}/date={today_str}/positions.parquet"
            try:
                pq.write_table(empty_table, r2_key, filesystem=fs, compression="snappy")
                print(f"  {today_str} (0 rows) -> uploaded empty sentinel to R2")
                total_uploaded += 1
            except Exception as exc:
                print(f"  [warn] failed to upload empty sentinel: {exc}", file=sys.stderr)
        if not to_upload:
            print(f"  all {len(dates)} date(s) already in R2 - skipping")
        else:
            print(
                f"  staging + uploading {len(to_upload)} new date(s) ({len(existing_r2)} already in R2) ..."
            )
            for date_str in to_upload:
                table = con.execute(
                    "SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type "
                    "FROM ais_positions WHERE CAST(timezone('UTC', timestamp) AS DATE) = ? ORDER BY mmsi, timestamp",
                    [date_str],
                ).to_arrow_table()
                local_path = (
                    staging_dir / f"region={region}" / f"date={date_str}" / "positions.parquet"
                )
                local_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(table, local_path, compression="snappy")
                r2_key = f"{bucket}/ais/region={region}/date={date_str}/positions.parquet"
                try:
                    pq.write_table(table, r2_key, filesystem=fs, compression="snappy")
                    print(f"    {date_str} ({table.num_rows:,} rows) -> staging + {r2_key}")
                    total_uploaded += 1
                except Exception as exc:
                    print(f"    [error] {date_str}: {exc}", file=sys.stderr)

        # Always push vessel_meta (full snapshot — overwrite on every run)
        try:
            vm_count = con.execute("SELECT COUNT(*) FROM vessel_meta").fetchone()[0]
            vm_table = con.execute(
                "SELECT mmsi, imo, name, flag, ship_type FROM vessel_meta WHERE mmsi IS NOT NULL"
            ).to_arrow_table()
        except Exception as exc:
            print(f"  [skip] vessel_meta: could not read ({exc})", file=sys.stderr)
            vm_count = 0
            vm_table = None
        if vm_count > 0 and vm_table is not None:
            # Validate schema before upload
            try:
                import polars as _pl

                from pipelines.storage.schemas import VesselMetaSchema

                VesselMetaSchema.validate(_pl.from_arrow(vm_table))
            except Exception as exc:
                print(f"  [skip] vessel_meta validation failed: {exc}", file=sys.stderr)
                con.close()
                continue
            vm_r2_key = f"{bucket}/vessel_meta/region={region}.parquet"
            try:
                pq.write_table(vm_table, vm_r2_key, filesystem=fs, compression="snappy")
                print(f"  vessel_meta ({vm_count:,} vessels) -> {vm_r2_key}")
                total_uploaded += 1
            except Exception as exc:
                print(f"  [error] vessel_meta: {exc}", file=sys.stderr)
        else:
            if vm_table is not None:
                print("  vessel_meta: empty — skipping")

        con.close()
    print(f"Done. {total_uploaded} file(s) uploaded to {bucket}")
    return 0


def cmd_pull_ais_parquet(args: argparse.Namespace) -> int:
    """Download AIS date-partitioned Parquet from maridb-public/ais/ to local data dir.

    Downloads only the last --days days (default 60). Skips partitions already
    present locally by size check.
    Layout: <data-dir>/ais/region=<region>/date=YYYY-MM-DD/positions.parquet
    """
    from datetime import date, timedelta

    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    data_dir = Path(args.data_dir)
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    regions_filter: set[str] | None = (
        {r.strip() for r in args.regions.split(",")} if args.regions else None
    )
    cutoff = (date.today() - timedelta(days=args.days)).isoformat()
    fs = _build_r2_fs()
    total = 0
    try:
        all_files = fs.get_file_info(pafs.FileSelector(f"{bucket}/ais/", recursive=True))
    except Exception as exc:
        print(f"Error listing {bucket}/ais/: {exc}", file=sys.stderr)
        return 1
    for info in all_files:
        if not info.path.endswith(".parquet"):
            continue
        parts = info.path.split("/")
        region_part = next((p for p in parts if p.startswith("region=")), None)
        date_part = next((p for p in parts if p.startswith("date=")), None)
        if not region_part or not date_part:
            continue
        region = region_part.removeprefix("region=")
        date_str = date_part.removeprefix("date=")
        if regions_filter and region not in regions_filter:
            continue
        if date_str < cutoff:
            continue
        local_path = (
            data_dir / "ais" / f"region={region}" / f"date={date_str}" / "positions.parquet"
        )
        if local_path.exists() and local_path.stat().st_size == info.size:
            continue
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            table = pq.read_table(info.path, filesystem=fs)
            pq.write_table(table, local_path, compression="snappy")
            print(f"  {region}/{date_str} ({table.num_rows:,} rows) -> {local_path}")
            total += 1
        except Exception as exc:
            print(f"  [error] {region}/{date_str}: {exc}", file=sys.stderr)

    # Also pull vessel_meta snapshots for each requested region
    regions_to_pull = list(regions_filter) if regions_filter else None
    if regions_to_pull is None:
        # infer from what exists on R2
        try:
            vm_infos = fs.get_file_info(
                pafs.FileSelector(f"{bucket}/vessel_meta/", recursive=False)
            )
            regions_to_pull = [
                Path(i.path).stem.removeprefix("region=")
                for i in vm_infos
                if i.type == pafs.FileType.File and i.path.endswith(".parquet")
            ]
        except Exception:
            regions_to_pull = []
    for region in regions_to_pull or []:
        r2_key = f"{bucket}/vessel_meta/region={region}.parquet"
        local_vm = data_dir / "vessel_meta" / f"region={region}.parquet"
        try:
            infos = fs.get_file_info([r2_key])
            if infos[0].type != pafs.FileType.File:
                continue
            if local_vm.exists() and local_vm.stat().st_size == infos[0].size:
                continue
            local_vm.parent.mkdir(parents=True, exist_ok=True)
            vm_table = pq.read_table(r2_key, filesystem=fs)
            pq.write_table(vm_table, local_vm, compression="snappy")
            print(f"  vessel_meta/{region} ({vm_table.num_rows:,} vessels) -> {local_vm}")
            total += 1
        except Exception as exc:
            print(f"  [warn] vessel_meta/{region}: {exc}", file=sys.stderr)

    print(f"Done. {total} file(s) downloaded.")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    latest = _read_latest(fs, bucket)
    timestamps = _list_timestamps(fs, bucket)

    if not timestamps:
        print("No generations found in R2. Run: uv run python scripts/sync_r2.py push")
        return 0

    import pyarrow.fs as pafs

    print(f"{'TIMESTAMP':<22}  {'SIZE':>10}  NOTE")
    print("-" * 46)
    for ts in reversed(timestamps):  # newest first
        r2_path = _r2_zip_path(bucket, ts)
        try:
            infos = fs.get_file_info([r2_path])
            mb = infos[0].size / 1_048_576 if infos[0].type == pafs.FileType.File else 0.0
        except Exception:
            mb = 0.0
        note = "<- latest" if ts == latest else ""
        print(f"{ts:<22}  {mb:>8.1f} MB  {note}")

    print()
    for r2_key, label, push_cmd in [
        (_GDELT_R2_KEY, "gdelt.lance.zip", "push-gdelt"),
        (_SANCTIONS_DB_R2_KEY, "public_eval.duckdb", "push-sanctions-db"),
    ]:
        r2_path = f"{bucket}/{r2_key}"
        try:
            infos = fs.get_file_info([r2_path])
            if infos[0].type == pafs.FileType.File:
                mb = infos[0].size / 1_048_576
                print(f"{label:<28}  {mb:>6.1f} MB  (shared, outside rotation)")
            else:
                print(f"{label:<28}  (not yet uploaded — run {push_cmd})")
        except Exception:
            print(f"{label:<28}  (not yet uploaded — run {push_cmd})")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _check_env(require_credentials: bool = True) -> bool:
    required = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] if require_credentials else []
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"Error: missing env vars: {', '.join(missing)}", file=sys.stderr)
        print("Set them in .env or export them. See .env.example for reference.", file=sys.stderr)
        return False
    return True


_ARKTRACE_PUBLIC_BUCKET = "arktrace-public"
_ARKTRACE_PUBLIC_BASE_URL = "https://arktrace-public.edgesentry.io"

# Tables exposed to the arktrace browser app via ducklake_manifest.json.
# Each entry maps a local filename glob → (r2 key, DuckDB-WASM register_as name).
_ARKTRACE_TABLES: list[tuple[str, str, str]] = [
    # (glob pattern, r2 key template, register_as)
    # region is substituted at runtime for region-scoped files
    ("*_watchlist.parquet", "score/{filename}", "watchlist.parquet"),
    ("composite_scores.parquet", "score/{filename}", "composite_scores.parquet"),
    ("causal_effects.parquet", "score/{filename}", "causal_effects.parquet"),
    ("score_history.parquet", "score/{filename}", "score_history.parquet"),
    ("validation_metrics.parquet", "score/{filename}", "validation_metrics.parquet"),
]

# Region prefix → region tag used in the manifest (matches arktrace app config)
_MANIFEST_REGION_TAG: dict[str, str] = {
    "singapore": "singapore",
    "japansea": "japansea",
    "japan": "japansea",
    "blacksea": "blacksea",
    "europe": "europe",
    "middleeast": "middleeast",
}


def cmd_push_arktrace(args: argparse.Namespace) -> int:
    """Copy pipeline outputs from maridb-public → arktrace-public and write manifest.

    Steps:
      1. Upload score/*_watchlist.parquet + shared score Parquets to arktrace-public/score/
      2. Write ducklake_manifest.json listing every uploaded file so opfs.ts can
         discover and cache them without any app-side changes (arktrace#519).

    The manifest format matches what opfs.ts already expects:
      { "files": [{ "key", "url", "size_bytes", "register_as", "region"? }] }
    """
    import json as _json

    data_dir = Path(args.data_dir)
    fs = _build_r2_fs()
    manifest_files: list[dict] = []

    # Collect all score Parquets to upload
    upload_pairs: list[
        tuple[Path, str, str, str | None]
    ] = []  # (local, r2_key, register_as, region)

    # Region-scoped watchlists: singapore_watchlist.parquet → region tag "singapore"
    for wl_path in sorted(data_dir.glob("*_watchlist.parquet")):
        stem = wl_path.stem.replace("_watchlist", "")
        region_tag = _MANIFEST_REGION_TAG.get(stem)
        r2_key = f"score/{wl_path.name}"
        upload_pairs.append((wl_path, r2_key, "watchlist.parquet", region_tag))

    # Shared (non-region) score files
    for name, register_as in [
        ("composite_scores.parquet", "composite_scores.parquet"),
        ("causal_effects.parquet", "causal_effects.parquet"),
        ("score_history.parquet", "score_history.parquet"),
        ("validation_metrics.parquet", "validation_metrics.parquet"),
    ]:
        p = data_dir / name
        if p.exists():
            upload_pairs.append((p, f"score/{name}", register_as, None))

    if not upload_pairs:
        print(
            f"No score Parquets found in {data_dir}. Run the pipeline first.",
            file=sys.stderr,
        )
        return 1

    total_bytes = 0
    for local_path, r2_key, register_as, region_tag in upload_pairs:
        if not local_path.exists():
            print(f"  [skip] {local_path.name} — not found", file=sys.stderr)
            continue
        size = local_path.stat().st_size
        r2_path = f"{_ARKTRACE_PUBLIC_BUCKET}/{r2_key}"
        print(f"  {local_path.name} ({size / 1024:.1f} KB) → {r2_path}")
        _upload_file(fs, local_path, r2_path)
        total_bytes += size
        entry: dict = {
            "key": r2_key,
            "url": f"{_ARKTRACE_PUBLIC_BASE_URL}/{r2_key}",
            "size_bytes": size,
            "register_as": register_as,
        }
        if region_tag:
            entry["region"] = region_tag
        manifest_files.append(entry)

    # Write manifest under both names:
    #   manifest.json          — new canonical name (arktrace app will migrate to this)
    #   ducklake_manifest.json — legacy name kept until arktrace app stops reading it
    manifest = {"files": manifest_files}
    manifest_json = _json.dumps(manifest, indent=2)
    manifest_tmp = Path(tempfile.mktemp(suffix=".json"))
    manifest_tmp.write_text(manifest_json)
    for manifest_key in ["manifest.json", "ducklake_manifest.json"]:
        r2_path = f"{_ARKTRACE_PUBLIC_BUCKET}/{manifest_key}"
        print(f"  {manifest_key} ({len(manifest_json)} B) → {r2_path}")
        _upload_file(fs, manifest_tmp, r2_path)
    manifest_tmp.unlink(missing_ok=True)

    print(
        f"\nDone. Pushed {len(manifest_files)} file(s) ({total_bytes / 1_048_576:.2f} MB) "
        f"+ manifest → arktrace-public\n"
        f"  {_ARKTRACE_PUBLIC_BASE_URL}/manifest.json\n"
        f"  {_ARKTRACE_PUBLIC_BASE_URL}/ducklake_manifest.json  (legacy — remove once arktrace migrated)"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync processed pipeline artifacts to/from Cloudflare R2.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    push_p = sub.add_parser("push", help="Push new generation zip to R2, prune old zips")
    push_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    push_p.add_argument("--keep", type=int, default=_DEFAULT_KEEP, metavar="N")
    push_p.add_argument("--force", action="store_true")

    pull_p = sub.add_parser("pull", help="Download + extract latest (or named) snapshot zip")
    pull_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    pull_p.add_argument("--timestamp", default=None, metavar="TIMESTAMP")
    pull_p.add_argument("--force", action="store_true")

    push_ais_p = sub.add_parser(
        "push-ais-parquet",
        help="Export ais_positions to date-partitioned Parquet and upload to maridb-public/ais/",
    )
    push_ais_p.add_argument(
        "--data-dir",
        default=str(Path.home() / ".maridb" / "data" / "raw" / "ais"),
        metavar="DIR",
        help="Directory containing raw AIS .duckdb files (default: ~/.indago/data/raw/ais)",
    )
    push_ais_p.add_argument(
        "--staging-dir",
        default=str(Path.home() / ".maridb" / "data" / "staging" / "ais"),
        metavar="DIR",
        help="Local staging directory for Parquet partitions before upload (default: ~/.indago/data/staging/ais)",
    )
    push_ais_p.add_argument(
        "--regions",
        default=None,
        metavar="REGIONS",
        help=f"Comma-separated region names (default: all). Known: {', '.join(_REGION_PREFIX)}",
    )

    pull_ais_p = sub.add_parser(
        "pull-ais-parquet",
        help="Download AIS date-partitioned Parquet from maridb-public/ais/ to local dir",
    )
    pull_ais_p.add_argument(
        "--data-dir",
        default=str(Path.home() / ".maridb" / "data" / "downloads"),
        metavar="DIR",
        help="Local directory to download partitions into (default: ~/.indago/data/downloads)",
    )
    pull_ais_p.add_argument("--regions", default=None, metavar="REGIONS")
    pull_ais_p.add_argument(
        "--days",
        type=int,
        default=60,
        metavar="N",
        help="Download only partitions from the last N days (default: 60)",
    )

    gdelt_push_p = sub.add_parser("push-gdelt", help="Upload gdelt.lance as gdelt.lance.zip")
    gdelt_push_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    gdelt_push_p.add_argument(
        "--force", action="store_true", help="Re-upload even if already in R2"
    )
    gdelt_pull_p = sub.add_parser("pull-gdelt", help="Download gdelt.lance.zip and extract")
    gdelt_pull_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_sd_p = sub.add_parser("push-sanctions-db", help="Upload public_eval.duckdb to R2")
    push_sd_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    push_sd_p.add_argument("--force", action="store_true")
    pull_sd_p = sub.add_parser("pull-sanctions-db", help="Download public_eval.duckdb from R2")
    pull_sd_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_wl_p = sub.add_parser(
        "push-watchlists", help="Upload *_watchlist.parquet files as watchlists.zip"
    )
    push_wl_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    push_wl_p.add_argument("--force", action="store_true")
    pull_wl_p = sub.add_parser("pull-watchlists", help="Download watchlists.zip and extract")
    pull_wl_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_demo_p = sub.add_parser("push-demo", help="Upload demo bundle to R2")
    push_demo_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    push_demo_p.add_argument("--force", action="store_true")
    pull_demo_p = sub.add_parser(
        "pull-demo", help="Download demo bundle from R2 (no credentials needed)"
    )
    pull_demo_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_dl_p = sub.add_parser(
        "push-ducklake-public", help="Upload DuckLake catalog + Parquet to maridb-public/"
    )
    push_dl_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_arktrace_p = sub.add_parser(
        "push-arktrace",
        help="Copy score Parquets to arktrace-public and write ducklake_manifest.json (arktrace#519)",
    )
    push_arktrace_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    sub.add_parser("list", help="List snapshot zips and shared objects in R2")

    args = parser.parse_args()
    read_only = args.command in (
        "pull",
        "pull-gdelt",
        "pull-sanctions-db",
        "pull-watchlists",
    )
    if not _check_env(require_credentials=not read_only):
        return 1

    dispatch = {
        "push": cmd_push,
        "pull": cmd_pull,
        "push-ais-parquet": cmd_push_ais_parquet,
        "pull-ais-parquet": cmd_pull_ais_parquet,
        "push-gdelt": cmd_push_gdelt,
        "pull-gdelt": cmd_pull_gdelt,
        "push-sanctions-db": cmd_push_sanctions_db,
        "pull-sanctions-db": cmd_pull_sanctions_db,
        "push-watchlists": cmd_push_watchlists,
        "pull-watchlists": cmd_pull_watchlists,
        "push-demo": cmd_push_demo,
        "pull-demo": cmd_pull_demo,
        "push-ducklake-public": cmd_push_ducklake_public,
        "push-arktrace": cmd_push_arktrace,
        "list": cmd_list,
    }
    return dispatch[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
