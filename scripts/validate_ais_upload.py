"""Daily validation of AIS Parquet files uploaded to maridb-public/ais/.

Reads yesterday's partition for each region from R2, runs sanity checks,
writes a JSON report to R2 at validation/ais/YYYY-MM-DD.json, and exits
non-zero if any region fails a required check.

Environment variables
---------------------
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION  R2 credentials
S3_BUCKET          Override bucket name (default: maridb-public)
VALIDATE_DATE      Override target date ISO string (default: yesterday)
REPORT_LOCAL_PATH  Local path to write report JSON (default: data/processed/ais_validation_report.json)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from pipelines.storage.config import _DEFAULT_BUCKET, _DEFAULT_ENDPOINT

_REQUIRED_COLUMNS = {"mmsi", "timestamp", "lat", "lon"}
_ALL_REGIONS = [
    "japansea", "blacksea", "middleeast", "singapore", "europe",
    "gulfofmexico", "hornofafrica", "persiangulf", "gulfofaden", "gulfofguinea",
]
_ACTIVE_REGIONS = [
    "japansea", "singapore", "europe", "blacksea", "middleeast", "gulfofguinea",
]
_MIN_ACTIVE_REGIONS = 5


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


def _validate_region(df: pl.DataFrame, region: str, target_date: str) -> dict:
    result: dict = {"region": region, "date": target_date, "row_count": df.height, "checks": {}}

    # Schema
    missing_cols = _REQUIRED_COLUMNS - set(df.columns)
    result["checks"]["schema"] = {
        "pass": len(missing_cols) == 0,
        "missing_columns": sorted(missing_cols),
    }

    # Row count — 0 rows is OK (no vessels in region that day); file missing is the real error
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

    # Timestamp recency (within last 48 hours from now)
    if "timestamp" in df.columns:
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


def main() -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    target_date = os.getenv("VALIDATE_DATE") or (date.today() - timedelta(days=1)).isoformat()
    local_report_path = Path(
        os.getenv("REPORT_LOCAL_PATH", "data/processed/ais_validation_report.json")
    )
    local_report_path.parent.mkdir(parents=True, exist_ok=True)

    fs = _build_fs()
    region_results = []
    regions_with_data: list[str] = []

    print(f"Validating AIS uploads for {target_date} in {bucket}/ais/ ...")

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
            result = _validate_region(df, region, target_date)
            region_results.append(result)
            status = "OK" if result["pass"] else "FAIL"
            print(f"  {region:20s} {status:4s}  rows={result['row_count']:,}")
            if result["pass"]:
                regions_with_data.append(region)
        except Exception as exc:
            region_results.append({
                "region": region, "date": target_date, "row_count": 0,
                "checks": {}, "pass": False, "error": str(exc),
            })
            print(f"  {region:20s} ERROR: {exc}")

    # Region coverage check
    active_passing = [r for r in regions_with_data if r in _ACTIVE_REGIONS]
    coverage_pass = len(active_passing) >= _MIN_ACTIVE_REGIONS

    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "target_date": target_date,
        "regions_passing": regions_with_data,
        "active_regions_passing": active_passing,
        "coverage_check": {
            "pass": coverage_pass,
            "active_passing": len(active_passing),
            "required": _MIN_ACTIVE_REGIONS,
        },
        "region_results": region_results,
        "overall_pass": coverage_pass and all(
            r["pass"] for r in region_results if r["region"] in _ACTIVE_REGIONS
        ),
    }

    # Write local report
    local_report_path.write_text(json.dumps(report, indent=2))
    print(f"\nReport written to {local_report_path}")

    # Upload JSON report to R2
    r2_key = f"validation/ais/{target_date}.json"
    try:
        body = json.dumps(report, indent=2).encode()
        with fs.open_output_stream(f"{bucket}/{r2_key}") as f:
            f.write(body)
        print(f"Report uploaded to R2: {bucket}/{r2_key}")
    except Exception as exc:
        print(f"Warning: failed to upload report to R2: {exc}", file=sys.stderr)

    overall = report["overall_pass"]
    print(f"\nOverall: {'PASS' if overall else 'FAIL'}")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
