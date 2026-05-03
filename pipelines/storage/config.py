"""Storage configuration for maridb pipelines.

maridb is the central data layer. Pipelines write processed data to
maridb-public R2, then the distribute step pushes validated outputs to
app-specific buckets (arktrace-public, documaris-public).

Environment variables
---------------------
USE_S3                Set to "1" or "true" to route reads/writes to R2.
                      Default: off (local disk).
MARIDB_DATA_DIR       Override the local data directory (default: ~/.indago/data).
S3_BUCKET             Bucket name (default: maridb-public)
S3_ENDPOINT           Custom endpoint URL for R2 / MinIO
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION            Default: auto
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

_DEFAULT_BUCKET = "maridb-public"
_DEFAULT_ENDPOINT = "https://b8a0b09feb89390fb6e8cf4ef9294f48.r2.cloudflarestorage.com"
_DEFAULT_REGION = "auto"

# App-specific output buckets (written by the distribute step)
ARKTRACE_BUCKET = "arktrace-public"
DOCUMARIS_BUCKET = "documaris-public"


def _data_dir() -> str:
    if explicit := os.getenv("MARIDB_DATA_DIR"):
        return str(Path(explicit).expanduser())
    return str(Path.home() / ".maridb" / "data")


def is_s3() -> bool:
    return os.getenv("USE_S3", "0").lower() in ("1", "true", "yes")


def _bucket() -> str:
    return os.getenv("S3_BUCKET", _DEFAULT_BUCKET)


def polars_storage_options() -> dict[str, str] | None:
    if not is_s3():
        return None
    opts: dict[str, str] = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "aws_region": os.getenv("AWS_REGION", _DEFAULT_REGION),
    }
    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    if endpoint:
        opts["aws_endpoint_url"] = endpoint
        opts["aws_allow_http"] = "true"
    return opts


def lance_storage_options() -> dict[str, str] | None:
    if not is_s3():
        return None
    opts: dict[str, str] = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "aws_region": os.getenv("AWS_REGION", _DEFAULT_REGION),
    }
    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    if endpoint:
        opts["aws_endpoint"] = endpoint
        opts["aws_allow_http"] = "true"
    return opts


def output_uri(filename: str, bucket: str | None = None) -> str:
    """Resolve an output URI.

    When USE_S3=1, returns ``s3://<bucket>/<filename>``.
    Otherwise returns a local path under MARIDB_DATA_DIR.
    """
    if is_s3():
        b = bucket or _bucket()
        return f"s3://{b}/{filename}"
    data_dir = os.getenv("DATA_DIR", _data_dir())
    return os.path.join(data_dir, filename)


def lance_db_uri() -> str:
    if is_s3():
        return f"s3://{_bucket()}/gdelt.lance"
    return os.getenv("GDELT_LANCE_PATH", str(Path(_data_dir()) / "gdelt.lance"))


def graph_uri(db_path: str) -> str:
    stem = Path(db_path).stem
    if is_s3():
        return f"s3://{_bucket()}/{stem}_graph"
    return str(Path(db_path).parent / f"{stem}_graph")


def write_parquet(df: "pl.DataFrame", uri: str) -> None:
    if uri.startswith("s3://"):
        _write_parquet_s3(df, uri)
    else:
        os.makedirs(os.path.dirname(uri) or ".", exist_ok=True)
        df.write_parquet(uri)


def _write_parquet_s3(df: "pl.DataFrame", uri: str) -> None:
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    fs_kwargs: dict[str, str] = {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "region": os.getenv("AWS_REGION", _DEFAULT_REGION),
    }
    if endpoint:
        fs_kwargs["endpoint_override"] = endpoint.split("://", 1)[-1]
        fs_kwargs["scheme"] = "http" if endpoint.startswith("http://") else "https"
    s3 = pafs.S3FileSystem(**fs_kwargs)
    pq.write_table(df.to_arrow(), f"{bucket}/{key}", filesystem=s3)


def read_parquet(uri: str) -> "pl.DataFrame | None":
    import polars as pl

    if uri.startswith("s3://"):
        try:
            return pl.read_parquet(uri, storage_options=polars_storage_options())
        except Exception:
            return None
    if not os.path.exists(uri):
        return None
    return pl.read_parquet(uri)
