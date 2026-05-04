"""Validate AIS Parquet files uploaded to maridb-public/ais/.

Checks the past VALIDATE_DAYS days (default 3) — catches late-arriving data
and multi-day gaps. Writes per-day JSON reports to R2 and exits non-zero if
the most recent day fails or too many days in the window fail.

Environment variables
---------------------
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION  R2 credentials
S3_BUCKET          Override bucket name (default: maridb-public)
VALIDATE_DAYS      Number of past days to validate (default: 3)
REPORT_LOCAL_PATH  Local path to write combined report JSON
                   (default: data/processed/ais_validation_report.json)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from pipelines.storage.config import _DEFAULT_BUCKET, _DEFAULT_ENDPOINT

_REQUIRED_COLUMNS = {"mmsi", "timestamp", "lat", "lon"}
_ALL_REGIONS = [
    "japansea", "blacksea", "middleeast", "singapore", "europe",
    "gulfofmexico", "hornofafrica", "persiangulf", "gulfofaden", "gulfofguinea",
]
_ACTIVE_REGIONS = [
    "japansea", "singapore", "europe", "blacksea", "middleeast", "gulfofguinea", "hornofafrica",
]
_MIN_ACTIVE_REGIONS = 6


def _build_fs():
    """Build pyarrow S3FileSystem for R2 — same pattern as sync_r2.py _build_r2_fs()."""
    import pyarrow.fs as pafs
    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    host = endpoint.split("://", 1)[-1].rstrip("/")
    scheme = "https" if endpoint.startswith("https://") else "http"
    return pafs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.getenv("AWS_REGION", "auto"),
        endpoint_override=host,
        scheme=scheme,
    )


def _read_parquet_from_r2(fs, bucket: str, key: str) -> pl.DataFrame | None:
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq
    path = f"{bucket}/{key}"
    info = fs.get_file_info(path)
    if info.type == pafs.FileType.NotFound:
        return None
    return pl.from_arrow(pq.read_table(path, filesystem=fs))


def _validate_region(df: pl.DataFrame, region: str, target_date: str, is_recent: bool) -> dict:
    result: dict = {"region": region, "date": target_date, "row_count": df.height, "checks": {}}

    # Schema
    missing_cols = _REQUIRED_COLUMNS - set(df.columns)
    result["checks"]["schema"] = {
        "pass": len(missing_cols) == 0,
        "missing_columns": sorted(missing_cols),
    }

    # Row count — 0 rows OK (sentinel uploaded; no vessels that day)
    result["checks"]["row_count"] = {"pass": True, "rows": df.height}

    if df.height == 0:
        result["pass"] = True
        return result

    # Null rate for mmsi
    mmsi_nulls = df["mmsi"].null_count() if "mmsi" in df.columns else df.height
    result["checks"]["mmsi_null_rate"] = {
        "pass": mmsi_nulls == 0,
        "null_count": mmsi_nulls,
    }

    # Coordinate range
    if "lat" in df.columns and "lon" in df.columns:
        bad_coords = df.filter(
            (pl.col("lat") < -90) | (pl.col("lat") > 90) |
            (pl.col("lon") < -180) | (pl.col("lon") > 180)
        ).height
        lat_nulls = df["lat"].null_count()
        lon_nulls = df["lon"].null_count()
        null_rate = (lat_nulls + lon_nulls) / (2 * df.height)
        result["checks"]["coordinates"] = {
            "pass": bad_coords == 0 and null_rate < 0.05,
            "out_of_range": bad_coords,
            "null_rate": round(null_rate, 4),
        }

    # Timestamp within target date (UTC) — skip for older days; recency only matters for yesterday
    if is_recent and "timestamp" in df.columns:
        try:
            ts_col = df["timestamp"]
            if ts_col.dtype == pl.Utf8:
                ts_col = ts_col.str.to_datetime(time_unit="us", time_zone="UTC")
            elif hasattr(ts_col.dtype, "time_zone") and ts_col.dtype.time_zone:
                ts_col = ts_col.dt.convert_time_zone("UTC")
            cutoff = datetime.now(UTC) - timedelta(hours=48)
            stale = (ts_col < cutoff).sum()
            result["checks"]["timestamp_recency"] = {
                "pass": stale / df.height < 0.5,
                "stale_fraction": round(stale / df.height, 4),
            }
        except Exception as exc:
            result["checks"]["timestamp_recency"] = {"pass": False, "error": str(exc)}

    # Duplicate rate
    if "mmsi" in df.columns and "timestamp" in df.columns:
        total = df.height
        unique = df.select(["mmsi", "timestamp"]).unique().height
        dup_rate = (total - unique) / total
        result["checks"]["duplicate_rate"] = {
            "pass": dup_rate < 0.01,
            "duplicate_rate": round(dup_rate, 4),
        }

    result["pass"] = all(c.get("pass", True) for c in result["checks"].values())
    return result


def _validate_date(fs, bucket: str, target_date: str, is_recent: bool) -> dict:
    """Validate all regions for a single date. Returns a day-level result dict."""
    region_results = []
    regions_passing = []

    print(f"\n[{target_date}]{'  ← most recent' if is_recent else ''}")
    for region in _ALL_REGIONS:
        key = f"ais/region={region}/date={target_date}/positions.parquet"
        try:
            df = _read_parquet_from_r2(fs, bucket, key)
            if df is None:
                region_results.append({
                    "region": region, "date": target_date, "row_count": 0,
                    "checks": {}, "pass": False, "error": "file not found in R2",
                })
                print(f"  {region:20s} MISSING")
                continue
            result = _validate_region(df, region, target_date, is_recent)
            region_results.append(result)
            status = "OK" if result["pass"] else "FAIL"
            print(f"  {region:20s} {status:4s}  rows={result['row_count']:,}")
            if result["pass"]:
                regions_passing.append(region)
        except Exception as exc:
            region_results.append({
                "region": region, "date": target_date, "row_count": 0,
                "checks": {}, "pass": False, "error": str(exc),
            })
            print(f"  {region:20s} ERROR: {exc}")

    active_passing = [r for r in regions_passing if r in _ACTIVE_REGIONS]
    coverage_pass = len(active_passing) >= _MIN_ACTIVE_REGIONS

    day_pass = coverage_pass
    print(f"  → {'PASS' if day_pass else 'FAIL'} ({len(active_passing)}/{len(_ACTIVE_REGIONS)} active regions)")

    return {
        "date": target_date,
        "pass": day_pass,
        "active_regions_passing": active_passing,
        "coverage_check": {
            "pass": coverage_pass,
            "active_passing": len(active_passing),
            "required": _MIN_ACTIVE_REGIONS,
        },
        "region_results": region_results,
    }


def main() -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    validate_days = int(os.getenv("VALIDATE_DAYS", "3"))
    local_report_path = Path(
        os.getenv("REPORT_LOCAL_PATH", "data/processed/ais_validation_report.json")
    )
    local_report_path.parent.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(UTC)
    # Validate from (validate_days) ago up to yesterday (UTC)
    dates = [
        (now_utc - timedelta(days=i)).date().isoformat()
        for i in range(validate_days, 0, -1)
    ]
    most_recent = dates[-1]

    print(f"Validating AIS uploads — past {validate_days} days (UTC): {dates[0]} → {most_recent}")
    print(f"Bucket: {bucket}/ais/")

    fs = _build_fs()
    day_results = []

    for target_date in dates:
        is_recent = (target_date == most_recent)
        day_result = _validate_date(fs, bucket, target_date, is_recent)
        day_results.append(day_result)

        # Upload per-day report to R2
        r2_key = f"validation/ais/{target_date}.json"
        try:
            with fs.open_output_stream(f"{bucket}/{r2_key}") as f:
                f.write(json.dumps(day_result, indent=2).encode())
        except Exception as exc:
            print(f"  Warning: failed to upload day report to R2: {exc}", file=sys.stderr)

    # Overall pass: most recent day passes AND at least (validate_days - 1) days pass
    most_recent_result = day_results[-1]
    days_passing = sum(1 for d in day_results if d["pass"])
    overall_pass = most_recent_result["pass"] and days_passing >= (validate_days - 1)

    print(f"\n{'='*50}")
    for d in day_results:
        icon = "✅" if d["pass"] else "❌"
        print(f"  {icon} {d['date']}  ({len(d['active_regions_passing'])}/{_MIN_ACTIVE_REGIONS} active regions)")
    print(f"\nOverall: {'PASS' if overall_pass else 'FAIL'} "
          f"({days_passing}/{validate_days} days passing)")

    report = {
        "generated_at_utc": now_utc.isoformat(),
        "validate_days": validate_days,
        "most_recent_date": most_recent,
        "days_passing": days_passing,
        "overall_pass": overall_pass,
        "day_results": day_results,
    }

    local_report_path.write_text(json.dumps(report, indent=2))
    print(f"Report written to {local_report_path}")

    # Upload combined report
    try:
        with fs.open_output_stream(f"{bucket}/validation/ais/latest.json") as f:
            f.write(json.dumps(report, indent=2).encode())
        print(f"Combined report uploaded to R2: {bucket}/validation/ais/latest.json")
    except Exception as exc:
        print(f"Warning: failed to upload combined report to R2: {exc}", file=sys.stderr)

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
