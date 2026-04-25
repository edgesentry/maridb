# maridb — Pipeline and Data Flow

**Updated:** 2026-04-25

## Overview

```
[macOS — always running]       [Cloudflare R2]        [GitHub Actions — daily]   [apps]

LaunchAgent: aisstream
  raw/ais/singapore.duckdb ─┐
  raw/ais/japansea.duckdb  ─┤
  raw/ais/blacksea.duckdb  ─┘
           │
           │ LaunchAgent: r2sync (hourly)
           │ push-ais-parquet
           ▼
  staging/ais/region=*/date=*/
           │
           │ upload
           ▼
     maridb-public/ais/               ←──── pull-ais-parquet (data-publish.yml)
                                                    │
                                          downloads/ais/region=*/date=*/
                                                    │
                                          _load_ais_from_parquet()
                                                    │
                                          processed/ais/{region}.duckdb
                                                    │
                                          ingest → features → score
                                                    │
                                          Gate 1: validate
                                                    │
                                          push to maridb-public/score/
                                                    │
                                          Gate 2: validate + distribute
                                          ┌─────────┴─────────┐
                                          ▼                   ▼
                                  arktrace-public/    documaris-public/
                                  watchlist/          voyage-evidence/
                                  features/
                                          │
                                          ▼
                                  DuckDB-WASM (browser)
```

---

## Local directory layout

All local data lives under `~/.maridb/data/` (override with `MARIDB_DATA_DIR` env var).

```
~/.maridb/
├── data/
│   ├── raw/
│   │   └── ais/
│   │       ├── singapore.duckdb     ← LaunchAgent writes live AIS positions here
│   │       ├── japansea.duckdb
│   │       └── blacksea.duckdb      (one file per active region)
│   │
│   ├── staging/
│   │   └── ais/
│   │       └── region={r}/
│   │           └── date=YYYY-MM-DD/
│   │               └── positions.parquet  ← push-ais-parquet writes here before upload
│   │
│   ├── downloads/
│   │   └── ais/
│   │       └── region={r}/
│   │           └── date=YYYY-MM-DD/
│   │               └── positions.parquet  ← pull-ais-parquet downloads from R2 here
│   │
│   └── processed/
│       └── ais/
│           ├── singapore.duckdb     ← run_pipeline.py working DB (rebuilt from downloads/)
│           └── japansea.duckdb
│
├── env                              ← secrets (git-ignored); sourced by LaunchAgents
├── singapore.log / singapore.err    ← aisstream LaunchAgent stdout/stderr
└── r2sync.log / r2sync.err          ← r2sync LaunchAgent stdout/stderr
```

---

## Stage 1 — AIS stream collection (local, always running)

**What:** macOS LaunchAgents stream live AIS positions via WebSocket and append rows to DuckDB.

**Who writes:** `pipelines/ingest/ais_stream.py` via `io.maridb.aisstream.<region>` LaunchAgent

**Writes to:** `~/.maridb/data/raw/ais/{region}.duckdb`

**Tables written:** `ais_positions`, `vessel_meta`

**Manage LaunchAgents:**
```bash
# Install (copies and loads all plists for listed regions)
bash scripts/install_launchagents.sh singapore japansea blacksea

# Check status
launchctl list | grep io.maridb

# Tail live AIS log
tail -f ~/.maridb/singapore.log

# Unload all
bash scripts/install_launchagents.sh --unload singapore japansea blacksea
```

Plist templates with `REPLACE_WITH_*` placeholders live in `config/launchagents/`. Filled-in
`*.local.plist` files are git-ignored. Secrets come from `~/.maridb/env`.

---

## Stage 2 — R2 upload (local → maridb-public, hourly)

**What:** Exports new AIS rows from raw DuckDB to date-partitioned Parquet, stages locally, then uploads to `maridb-public`.

**Who writes:** `scripts/sync_r2.py push-ais-parquet` via `io.maridb.r2sync` LaunchAgent

**Reads from:** `~/.maridb/data/raw/ais/{region}.duckdb`

**Stages to:** `~/.maridb/data/staging/ais/region={r}/date=YYYY-MM-DD/positions.parquet`

**Uploads to:** `maridb-public/ais/region={r}/date=YYYY-MM-DD/positions.parquet`

**Incremental logic:** checks which `date=YYYY-MM-DD` partitions already exist in R2; only uploads new dates.

**Parquet schema:**

| Column | Type | Notes |
|---|---|---|
| `mmsi` | VARCHAR | vessel identifier |
| `timestamp` | TIMESTAMPTZ | UTC position fix time |
| `lat` | DOUBLE | latitude |
| `lon` | DOUBLE | longitude |
| `sog` | FLOAT | speed over ground (knots) |
| `cog` | FLOAT | course over ground (degrees) |
| `nav_status` | TINYINT | AIS navigational status code |
| `ship_type` | TINYINT | AIS ship type code |

**Manual run:**
```bash
source ~/.maridb/env
uv run python scripts/sync_r2.py push-ais-parquet \
  --regions singapore,japansea,blacksea \
  --data-dir ~/.maridb/data/raw/ais \
  --staging-dir ~/.maridb/data/staging/ais
```

---

## Stage 3 — CI pipeline (data-publish.yml, daily at 01:00 UTC)

**Trigger:** daily 01:00 UTC; or manually via `gh workflow run data-publish.yml`.

**All steps run on the CI runner. No local machine needed.**

### Step 3a — Download AIS Parquet

```bash
uv run python scripts/sync_r2.py pull-ais-parquet \
  --days 60 \
  --regions singapore,japansea,blacksea
```

Downloads last 60 days of AIS partitions from `maridb-public/ais/` into the runner's
`~/.maridb/data/downloads/ais/` (path from `MARIDB_DATA_DIR`).

### Step 3b — Ingest (run_pipeline.py step 1)

`_load_ais_from_parquet()` loads downloaded Parquet into `processed/ais/{region}.duckdb`,
then the following modules run against that DB:

| Module | What it ingests |
|---|---|
| `pipelines/ingest/schema.py` | Creates/migrates DB schema |
| `pipelines/ingest/sanctions.py` | OFAC SDN + UN Consolidated list |
| `pipelines/ingest/vessel_registry.py` | IMO/MMSI registry |
| `pipelines/ingest/gdelt.py` | GDELT news events (last N days) |

**DB written:** `~/.maridb/data/processed/ais/{region}.duckdb`

### Step 3c — Features (run_pipeline.py step 2)

| Module | What it computes |
|---|---|
| `pipelines/features/ais_behavior.py` | Gap counts, loitering hours, position jumps |
| `pipelines/features/identity.py` | Flag/name/owner change counts |
| `pipelines/features/trade_mismatch.py` | Route vs cargo type mismatch |
| `pipelines/features/build_matrix.py` | Assembles `vessel_features` table |

**DB written:** `processed/ais/{region}.duckdb` (`vessel_features` table)

### Step 3d — Score (run_pipeline.py step 3)

| Module | What it outputs |
|---|---|
| `pipelines/score/mpol_baseline.py` | MPOL ownership risk baseline |
| `pipelines/score/anomaly.py` | Isolation Forest anomaly scores |
| `pipelines/score/composite.py` | Final `confidence` + `composite_score` |
| `pipelines/score/watchlist.py` | `{region}_watchlist.parquet` |

**Files written:**
```
data/processed/
  score/
    singapore_watchlist.parquet
    composite_scores.parquet
    causal_effects.parquet
```

### Step 3e — Gate 1: validate + push to maridb-public

Validation in `pipelines/storage/validate.py`. Upload aborts on failure.

```bash
uv run python scripts/sync_r2.py push-watchlists --data-dir data/processed
```

**Writes to R2:**
```
maridb-public/
  score/
    singapore_watchlist.parquet
    composite_scores.parquet
```

### Step 3f — Gate 2: distribute to app buckets

`pipelines/distribute/push.py` validates then copies to app buckets.

**Writes to R2:**
```
arktrace-public/
  score/
    singapore_watchlist.parquet
    composite_scores.parquet
    causal_effects.parquet
  ducklake_manifest.json      ← index for DuckDB-WASM browser sync
```

### Step 3g — Notify

`scripts/notify_metrics.py` emails pipeline metrics (AUROC, Precision@50, known-case coverage).

---

## Stage 4 — App reads

### arktrace PWA

Reads watchlists + scores from `arktrace-public` via DuckDB-WASM in the browser.
`ducklake_manifest.json` tells the Service Worker which files to cache.

```
arktrace-public/
  score/
    singapore_watchlist.parquet
    japansea_watchlist.parquet
    composite_scores.parquet
    causal_effects.parquet
  ducklake_manifest.json
```

### documaris app

Reads vessel/voyage/cargo directly from `maridb-public` (no copy needed).
Reads voyage evidence from `documaris-public`.

---

## Validation gates

### Gate 1 — before writing to maridb-public

Implemented in `pipelines/storage/validate.py`. Raises `PipelineValidationError` on failure; upload is aborted.

| Check | Failure action |
|---|---|
| Required columns present with correct types | Abort upload |
| No nulls in required fields | Abort upload |
| Row count ≥ minimum threshold | Abort upload |

### Gate 2 — before R2-to-R2 copy to app buckets

Implemented in `pipelines/distribute/push.py`. Previous version in the app bucket remains live on failure.

| Check | Failure action |
|---|---|
| Watchlist row count ≥ 1 | Abort copy |
| `composite_score` column present | Abort copy |
| Vessel features / AIS summaries present | Warn only (not yet enforced) |

---

## Quick reference

```bash
# Check LaunchAgents
launchctl list | grep io.maridb

# Tail live logs
tail -f ~/.maridb/singapore.log
tail -f ~/.maridb/r2sync.log

# Manual: upload today's AIS to R2
source ~/.maridb/env
uv run python scripts/sync_r2.py push-ais-parquet \
  --regions singapore,japansea,blacksea \
  --data-dir ~/.maridb/data/raw/ais \
  --staging-dir ~/.maridb/data/staging/ais

# Manual: download 60 days of AIS from R2 (runs before pipeline locally)
source ~/.maridb/env
uv run python scripts/sync_r2.py pull-ais-parquet \
  --regions singapore \
  --days 60

# Manual: run full pipeline locally for one region
MARIDB_DATA_DIR=~/.maridb/data \
  uv run python scripts/run_pipeline.py --region singapore --non-interactive

# Trigger CI manually
gh workflow run data-publish.yml --repo edgesentry/maridb

# Check recent CI runs
gh run list --repo edgesentry/maridb --limit 5
```
