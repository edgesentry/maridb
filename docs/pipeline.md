# maridb — Pipeline and Data Flow

**Updated:** 2026-04-25

## Overview

```
[macOS local]                [Cloudflare R2]              [GitHub Actions]        [arktrace PWA]

AIS stream (live)
  singapore.duckdb ──┐
  japansea.duckdb  ──┼── push-ais-parquet ──→ maridb-public/ais/  ──┐
  blacksea.duckdb  ──┘   (hourly, incremental)                       │
                                                                      │ pull-ais-parquet
                                                              data-publish.yml (daily)
                                                                      │
                                                              ingest → features → score
                                                                      │
                                                              Gate 1: validate
                                                                      │
                                                              ┌───────┴────────┐
                                                              ▼                ▼
                                                    maridb-public/      maridb-public/
                                                    features/           score/
                                                              │
                                                      Gate 2: validate + R2-to-R2 copy
                                                              │
                                                    arktrace-public/    documaris-public/
                                                    watchlist/          voyage-evidence/
                                                    features/
                                                              │
                                                              ▼
                                                    DuckDB-WASM reads
                                                    watchlist.parquet
```

---

## Storage format decisions

| Stage | Format | Reason |
|---|---|---|
| Local AIS stream | **DuckDB** (`.duckdb` per region) | Streaming writes; SQL aggregation; concurrent read-write supported |
| R2 intermediate (AIS raw) | **Parquet** (date-partitioned) | Universal; incremental upload by date; no tooling dependency |
| R2 intermediate (features, scores) | **Parquet** | Readable by polars, DuckDB, DuckDB-WASM without extensions |
| arktrace PWA | **Parquet** via DuckDB-WASM | Browser-native; selective download; no DuckLake extension needed |

DuckLake was evaluated and rejected: its incremental-append benefit is matched by date-partitioned Parquet, and DuckLake extensions are not guaranteed in DuckDB-WASM.

---

## Stage 1 — Local AIS stream collection

**What:** macOS LaunchAgents continuously write live AIS positions to local DuckDB files.

**Who writes:** `pipelines/ingest/ais_stream.py` (via LaunchAgent)

**Output:**
```
data/processed/
  singapore.duckdb    ← ais_positions, vessel_meta tables
  japansea.duckdb
  blacksea.duckdb
```

**Manage LaunchAgents:**
```bash
# Load 3 regions + r2sync
bash scripts/install_launchagents.sh singapore japansea blacksea

# Unload all
bash scripts/install_launchagents.sh --unload singapore japansea blacksea

# Check status
launchctl list | grep io.maridb
```

Secrets are in `~/.maridb/env` (git-ignored). Templates with `REPLACE_WITH_*` placeholders are in `config/launchagents/`.

---

## Stage 2 — R2 upload (local → maridb-public)

**What:** Hourly LaunchAgent exports new AIS rows to date-partitioned Parquet and uploads to `maridb-public`.

**Who writes:** `scripts/sync_r2.py push-ais-parquet` (via `io.maridb.r2sync` LaunchAgent)

**Incremental logic:** checks which `date=YYYY-MM-DD` partitions already exist in R2; only uploads new dates.

**Output on R2:**
```
maridb-public/
  ais/
    region=singapore/date=2026-04-25/positions.parquet
    region=japansea/date=2026-04-25/positions.parquet
    region=blacksea/date=2026-04-25/positions.parquet
```

**Schema:**

| Column | Type | Notes |
|---|---|---|
| `mmsi` | VARCHAR | vessel identifier |
| `timestamp` | TIMESTAMPTZ | UTC position fix time |
| `lat` | DOUBLE | latitude |
| `lon` | DOUBLE | longitude |
| `sog` | FLOAT | speed over ground (knots) |
| `cog` | FLOAT | course over ground (degrees) |
| `nav_status` | TINYINT | AIS navigational status |
| `ship_type` | TINYINT | AIS ship type code |

**Manual run:**
```bash
source ~/.maridb/env
uv run python scripts/sync_r2.py push-ais-parquet \
  --regions singapore,japansea,blacksea \
  --data-dir data/processed
```

---

## Stage 3 — CI pipeline (data-publish.yml)

**Trigger:** daily 01:00 UTC; manual dispatch.

**Steps:**

| Step | Script | Input | Output |
|---|---|---|---|
| Pull AIS Parquet | `sync_r2.py pull-ais-parquet` | `maridb-public/ais/` | local `data/processed/ais/` |
| Ingest (schema, sanctions, vessel registry) | `pipelines/ingest/` | local `.duckdb` + Parquet | local `.duckdb` |
| Features | `pipelines/features/build_matrix.py` | local `.duckdb` | `vessel_features.parquet` |
| Score | `pipelines/score/composite.py`, `watchlist.py` | local `.duckdb` | `{region}_watchlist.parquet` |
| Gate 1 validate + push to maridb-public | `sync_r2.py push` | local Parquet | `maridb-public/features/`, `maridb-public/score/` |
| Gate 2 distribute | `pipelines/distribute/push.py` | `maridb-public/` | `arktrace-public/`, `documaris-public/` |
| Notify | `scripts/notify_metrics.py` | pipeline results | email |

---

## Stage 4 — App reads

### arktrace PWA

Reads from `arktrace-public` via DuckDB-WASM in the browser.

```
arktrace-public/
  watchlist/singapore_watchlist.parquet
  watchlist/japansea_watchlist.parquet
  features/vessel_features.parquet
  ais-summaries/latest.parquet
```

Files are downloaded on first load, cached by Service Worker, and queried offline via DuckDB-WASM.

### documaris app

Reads vessel/voyage/cargo from `maridb-public` directly (no copy needed).
Reads voyage evidence and regulatory KB from `documaris-public`.

```
maridb-public/vessels/snapshot/
maridb-public/voyages/snapshot/
maridb-public/cargo/snapshot/

documaris-public/voyage-evidence/
documaris-public/regulatory-kb/
```

---

## Validation gates

### Gate 1 — before writing to maridb-public

Implemented in `pipelines/storage/validate.py`. Raises `PipelineValidationError` on failure; upload is aborted.

- Schema check: required columns present with correct types
- Null check: no nulls in required fields
- Row count ≥ minimum threshold

### Gate 2 — before R2-to-R2 copy to app buckets

Implemented in `pipelines/distribute/push.py`. On failure, the previous version in the app bucket remains live.

- Watchlist row count within expected range
- AIS coverage ≥ threshold for the region
- Regulatory KB: no silent key deletion

---

## Quick reference

```bash
# Check LaunchAgents
launchctl list | grep io.maridb

# Manual AIS → R2 upload
source ~/.maridb/env
uv run python scripts/sync_r2.py push-ais-parquet --regions singapore,japansea,blacksea --data-dir data/processed

# Manual full pipeline
uv run python scripts/run_pipeline.py --region singapore --non-interactive

# Trigger CI manually
gh workflow run data-publish.yml --repo edgesentry/maridb

# Check CI logs
gh run list --repo edgesentry/maridb --limit 5
```
