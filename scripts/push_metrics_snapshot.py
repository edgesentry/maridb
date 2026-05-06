"""Push a dated metrics snapshot to R2 and maintain a 7-entry rolling index.

Called after the regression gate passes in the data-publish CI workflow.
Writes one JSON file per day and keeps only the last 7 days in the index.
Older files are deleted from R2 to keep storage near zero.

Layout in maridb-public:
    metrics/YYYYMMDD.json      — daily snapshot
    metrics/index.json         — list of last 7 dated keys (newest first)

Usage
-----
    uv run python scripts/push_metrics_snapshot.py
    uv run python scripts/push_metrics_snapshot.py --dry-run
    uv run python scripts/push_metrics_snapshot.py --date 2026-05-06

Environment (same as sync_r2.py)
---------------------------------
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION        (default: auto)
    S3_BUCKET         (default: maridb-public)
    S3_ENDPOINT       (default: maridb-public R2 endpoint)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_BUCKET = "maridb-public"
_DEFAULT_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "https://4d28b4cbebe4bcbc11d45e99fda3a44c.r2.cloudflarestorage.com",
)
_METRICS_PREFIX = "metrics"
_INDEX_KEY = f"{_METRICS_PREFIX}/index.json"
_MAX_HISTORY = 7


def _processed_dir() -> Path:
    """Return the processed data directory, relative to cwd (tests can chdir)."""
    return Path.cwd() / "data" / "processed"


# ---------------------------------------------------------------------------
# Collect metrics from local pipeline outputs
# ---------------------------------------------------------------------------

def _collect_snapshot(date_str: str) -> dict:
    """Build a snapshot dict from available pipeline output files."""
    snap: dict = {"date": date_str}
    _PROCESSED = _processed_dir()

    # Backtest summary (data-publish)
    summary_path = _PROCESSED / "backtest_public_integration_summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
        ms = summary.get("metrics_summary", {})
        snap["precision_at_50"] = ms.get("precision_at_50", {}).get("mean")
        snap["precision_at_50_ci_low"] = ms.get("precision_at_50", {}).get("ci95_low")
        snap["precision_at_50_ci_high"] = ms.get("precision_at_50", {}).get("ci95_high")
        snap["recall_at_200"] = ms.get("recall_at_200", {}).get("mean")
        snap["auroc"] = ms.get("auroc", {}).get("mean") if isinstance(ms.get("auroc"), dict) else ms.get("auroc")
        snap["known_positives"] = summary.get("total_known_cases")
        snap["regions"] = summary.get("regions", [])
        snap["skipped_regions"] = summary.get("skipped_regions", [])

    # Lead time validation
    lead_path = _PROCESSED / "lead_time_report.json"  # noqa: F821 (defined above)
    if lead_path.exists():
        lead = json.loads(lead_path.read_text())
        snap["pre_designation_count"] = lead.get("pre_designation_count")
        snap["mean_lead_days"] = lead.get("mean_lead_days")
        snap["median_lead_days"] = lead.get("median_lead_days")
        snap["unknown_unknown_candidates"] = lead.get("unknown_unknown_candidates")

    # Validation metrics (AIS)
    val_path = _PROCESSED / "validation_metrics.json"
    if val_path.exists():
        val = json.loads(val_path.read_text())
        snap.setdefault("precision_at_50", val.get("precision_at_50"))
        snap.setdefault("recall_at_200", val.get("recall_at_200"))
        snap.setdefault("auroc", val.get("auroc"))

    snap["generated_at_utc"] = datetime.now(UTC).isoformat()
    return snap


# ---------------------------------------------------------------------------
# R2 helpers
# ---------------------------------------------------------------------------

def _make_fs():
    import pyarrow.fs as pafs

    endpoint = _DEFAULT_ENDPOINT
    host = endpoint.split("://", 1)[-1].rstrip("/")
    scheme = "https" if endpoint.startswith("https://") else "http"
    return pafs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_override=host,
        scheme=scheme,
        region=os.getenv("AWS_REGION", "auto"),
    )


def _read_index(fs, bucket: str) -> list[str]:
    try:
        with fs.open_input_stream(f"{bucket}/{_INDEX_KEY}") as f:
            data = json.loads(f.read().decode())
            return data.get("entries", [])
    except Exception:
        return []


def _write_json(fs, bucket: str, key: str, obj: dict) -> None:
    payload = json.dumps(obj, indent=2).encode()
    with fs.open_output_stream(f"{bucket}/{key}") as f:
        f.write(payload)


def _delete_key(fs, bucket: str, key: str) -> None:
    try:
        fs.delete_file(f"{bucket}/{key}")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Push daily metrics snapshot to R2")
    parser.add_argument("--date", default=None, help="Date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--dry-run", action="store_true", help="Print snapshot without uploading")
    args = parser.parse_args()

    date_str = args.date or datetime.now(UTC).strftime("%Y-%m-%d")
    date_key = date_str.replace("-", "")  # YYYYMMDD
    snapshot_key = f"{_METRICS_PREFIX}/{date_key}.json"

    snap = _collect_snapshot(date_str)
    print(f"Snapshot for {date_str}:")
    print(json.dumps(snap, indent=2))

    if args.dry_run:
        print("\n[dry-run] No upload performed.")
        return

    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        if not os.getenv(var):
            print(f"[error] {var} not set — cannot push to R2", file=sys.stderr)
            sys.exit(1)

    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    fs = _make_fs()

    # Write today's snapshot
    _write_json(fs, bucket, snapshot_key, snap)
    print(f"Uploaded: {snapshot_key}")

    # Update index (newest first, max 7 entries)
    entries = _read_index(fs, bucket)
    if date_key not in entries:
        entries = [date_key] + entries
    entries = entries[:_MAX_HISTORY]

    _write_json(fs, bucket, _INDEX_KEY, {"entries": entries, "updated_at_utc": datetime.now(UTC).isoformat()})
    print(f"Updated index: {entries}")

    # Delete entries beyond the 7-day window
    cutoff = datetime.now(UTC) - timedelta(days=_MAX_HISTORY)
    for entry in entries[_MAX_HISTORY:]:
        try:
            entry_date = datetime.strptime(entry, "%Y%m%d").replace(tzinfo=UTC)
            if entry_date < cutoff:
                key = f"{_METRICS_PREFIX}/{entry}.json"
                _delete_key(fs, bucket, key)
                print(f"Deleted old snapshot: {key}")
        except ValueError:
            pass


if __name__ == "__main__":
    main()
