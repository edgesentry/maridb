# maridb — Pipeline and Data Flow

**Updated:** 2026-04-25

## Overview

```
[macOS — always running]       [Cloudflare R2]              [GitHub Actions]           [apps]

LaunchAgent: aisstream
  raw/ais/{region}.duckdb ─┐
           │                │
           │ LaunchAgent: r2sync (hourly)
           │ push-ais-parquet
           ▼
  staging/ais/region=*/date=*/
           │
           │ upload (region=<stem> e.g. region=japansea)
           ▼
  maridb-public/ais/              ←── pull-ais-parquet (data-publish.yml, daily)
  maridb-public/gdelt.lance.zip   ←── push-gdelt       (gdelt-ingest.yml, weekly)

                                            data-publish.yml (daily, 01:00 UTC)
                                            ├── pull-ais-parquet → downloads/ais/
                                            ├── pull-gdelt       → processed/gdelt.lance
                                            ├── pull-watchlists  → processed/
                                            ├── pull-gfw-eo      → processed/
                                            │
                                            ├── run_pipeline.py ×5 (parallel)
                                            │     _load_ais_from_parquet()
                                            │     → processed/ais/{region}.duckdb
                                            │     ingest (schema, sanctions, vessel_registry)
                                            │     features → score
                                            │     → processed/score/{region}_watchlist.parquet
                                            │
                                            ├── push to maridb-public/score/
                                            │
                                            ├── Gate 2: distribute
                                            │   ┌────────────┴────────────┐
                                            ▼   ▼                         ▼
                                    arktrace-public/            documaris-public/
                                    score/watchlist             voyage-evidence/
                                            │
                                            ▼
                                    DuckDB-WASM (browser)

                                            public-backtest-integration.yml (on push to main)
                                            └── run_public_backtest_batch.py
                                                  run_pipeline.py --seed-dummy ×4 regions
                                                  → backtest metrics + artifacts
```

---

## Local directory layout

All local data lives under `~/.maridb/data/` (override with `MARIDB_DATA_DIR` env var).

```
~/.maridb/
├── data/
│   ├── raw/
│   │   └── ais/
│   │       └── {region}.duckdb      ← LaunchAgent (aisstream) writes live AIS here
│   │
│   ├── staging/
│   │   └── ais/
│   │       └── region={r}/date=YYYY-MM-DD/positions.parquet
│   │                                ← push-ais-parquet stages here before R2 upload
│   │
│   ├── downloads/
│   │   └── ais/
│   │       └── region={r}/date=YYYY-MM-DD/positions.parquet
│   │                                ← pull-ais-parquet downloads from R2 here
│   │
│   └── processed/
│       ├── ais/
│       │   └── {region}.duckdb      ← run_pipeline.py working DB
│       ├── gdelt.lance              ← pulled from R2 by pull-gdelt
│       └── score/
│           └── {region}_watchlist.parquet  ← written by step_score
│
├── env                              ← secrets (git-ignored)
├── {region}.log / {region}.err      ← aisstream LaunchAgent logs
└── r2sync.log / r2sync.err          ← r2sync LaunchAgent logs
```

---

## Stage 1 — AIS stream collection (local, always running)

**Who writes:** `pipelines/ingest/ais_stream.py` via `io.maridb.aisstream.<region>` LaunchAgent

**Writes to:** `~/.maridb/data/raw/ais/{region}.duckdb`

**Active regions:** whichever have a `*.local.plist` installed under `~/Library/LaunchAgents/`

```bash
launchctl list | grep io.maridb          # check status
tail -f ~/.maridb/singapore.log          # tail live AIS log
```

---

## Stage 2 — R2 upload (local → maridb-public, hourly)

**Who writes:** `scripts/sync_r2.py push-ais-parquet` via `io.maridb.r2sync` LaunchAgent

**Reads from:** `~/.maridb/data/raw/ais/{region}.duckdb`  
**Stages to:** `~/.maridb/data/staging/ais/region={stem}/date=YYYY-MM-DD/positions.parquet`  
**Uploads to:** `maridb-public/ais/region={stem}/date=YYYY-MM-DD/positions.parquet`

The region name in R2 is the **file stem** (e.g. `japansea`, not `japan`).

**Manual run:**
```bash
source ~/.maridb/env
uv run python scripts/sync_r2.py push-ais-parquet \
  --regions singapore,japansea,blacksea \
  --data-dir ~/.maridb/data/raw/ais \
  --staging-dir ~/.maridb/data/staging/ais
```

---

## Stage 3 — GDELT ingest (CI, weekly Sunday 23:00 UTC)

**Workflow:** `gdelt-ingest.yml`

**What:** Downloads last 7 days of GDELT global news events, indexes into `gdelt.lance`, pushes `gdelt.lance.zip` to R2.

**Why separate:** `gdelt.lance` is a shared LanceDB dataset. Running it inside the 5 parallel pipeline jobs causes concurrent write lock errors. Weekly cadence is sufficient — GDELT news context changes slowly.

**Manual trigger:**
```bash
gh workflow run gdelt-ingest.yml --repo edgesentry/maridb
```

---

## Stage 4 — CI pipeline (data-publish.yml, daily 01:00 UTC)

**Trigger:** daily 01:00 UTC; manual dispatch; automatically after public-backtest-integration succeeds.

### Step 4a — Pull data from R2 (parallel)

| Command | Downloads to |
|---|---|
| `pull-ais-parquet --data-dir data/downloads --days 60` | `data/downloads/ais/region=*/date=*/` |
| `pull-gdelt` | `data/processed/gdelt.lance` |
| `pull-watchlists` | `data/processed/` |
| `pull-gfw-eo` | `data/processed/` |

### Step 4b — Run pipelines (5 regions in parallel)

Each region's pipeline is isolated — no shared mutable state.

| Sub-step | Module | Output |
|---|---|---|
| Load AIS | `_load_ais_from_parquet()` | `processed/ais/{region}.duckdb` (ais_positions table) |
| Schema | `pipelines/ingest/schema.py` | DB tables created/migrated |
| Sanctions | `pipelines/ingest/sanctions.py` | sanctions_entities table |
| Vessel registry | `pipelines/ingest/vessel_registry.py` | vessel_meta table |
| AIS behavior | `pipelines/features/ais_behavior.py` | gap/loitering/jump features |
| Identity | `pipelines/features/identity.py` | flag/name/owner change counts |
| Trade mismatch | `pipelines/features/trade_mismatch.py` | route vs cargo mismatch |
| Build matrix | `pipelines/features/build_matrix.py` | vessel_features table |
| Score | `pipelines/score/composite.py` | confidence scores |
| Watchlist | `pipelines/score/watchlist.py` | `processed/score/{region}_watchlist.parquet` |

### Step 4c — Push to R2 + distribute

- Gate 1: validate outputs → push to `maridb-public/score/`
- Gate 2: validate → distribute to `arktrace-public/` + `documaris-public/`
- `push-watchlists` → `maridb-public/watchlists.zip`
- `push-demo` → `maridb-public/demo.zip`

---

## Stage 5 — Public backtest integration (on push to main)

**Workflow:** `public-backtest-integration.yml`

Runs `run_public_backtest_batch.py` with `--seed-dummy` for 4 regions. Seeds 10 known OFAC vessels per region, runs the full pipeline, evaluates against OpenSanctions. No real AIS data — plumbing test only.

**Artifacts uploaded:** `backtest_report_public_integration.json`, eval labels CSV per region.

---

## Validation gates

### Gate 1 — before writing to maridb-public

| Check | Action on failure |
|---|---|
| Required columns + types | Abort upload |
| No nulls in required fields | Abort upload |
| Row count ≥ minimum | Abort upload |

### Gate 2 — before distributing to app buckets

| Check | Action on failure |
|---|---|
| Watchlist row count ≥ 1 | Abort copy |
| `composite_score` column present | Abort copy |
| Vessel features / AIS summaries | Warn only |

Previous version in app bucket remains live on Gate 2 failure.

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

# Manual: download 60 days of AIS from R2
source ~/.maridb/env
uv run python scripts/sync_r2.py pull-ais-parquet \
  --regions singapore --days 60

# Manual: run backtest locally
source ~/.maridb/env
uv run python scripts/sync_r2.py pull-ais-parquet --regions singapore --days 60
uv run python scripts/run_public_backtest_batch.py \
  --regions singapore --stream-duration 0 --seed-dummy --min-known-cases 5

# Trigger CI manually
gh workflow run data-publish.yml --repo edgesentry/maridb
gh workflow run gdelt-ingest.yml --repo edgesentry/maridb

# Check recent CI runs
gh run list --repo edgesentry/maridb --limit 5
```
