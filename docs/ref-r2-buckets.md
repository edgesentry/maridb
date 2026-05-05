# indago — R2 Bucket Architecture

**Updated:** 2026-04-25

## Buckets

| Bucket | Public URL | Access | Writer | Readers |
|---|---|---|---|---|
| `maridb-public` | `https://pub-e088008b61ee432b906ef710d52af28c.r2.dev` | Unauthenticated GET | maridb pipelines | maridb, arktrace, documaris, any app |
| `arktrace-public` | existing | Unauthenticated GET | maridb distribute step (R2-to-R2 copy) | arktrace PWA |
| `documaris-public` | `https://pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev` | Unauthenticated GET | maridb distribute step + documaris CI | documaris app |

## Data flow

`maridb-public` is the **single source of truth**. All pipeline output lands here first. The distribute step copies the necessary subset to each app bucket via R2-to-R2 copy after validation.

```
[Raw sources]
  AIS feeds / vessel registries / sanctions / cargo manifests / port circulars
        │
        ▼
  maridb ingest + feature + score pipelines
        │
        ▼
  maridb-public/        ← ALL pipeline output (master copy)
        │
        ├── validate() → R2-to-R2 copy → arktrace-public/
        │                                  watchlist/, features/, ais-summaries/
        │
        └── validate() → R2-to-R2 copy → documaris-public/
                                           voyage-evidence/, regulatory-kb/
```

Apps read directly from their own bucket. documaris also reads vessel/voyage/cargo
directly from `maridb-public` — no copy needed for those.

## indago-public partition layout

```
maridb-public/
  ais/
    region=singapore/date=YYYY-MM-DD/positions.parquet
    region=malacca/date=YYYY-MM-DD/positions.parquet
  vessels/
    snapshot/vessel_id=IMO{7digits}/registry.parquet
  voyages/
    snapshot/voyage_id={id}/voyage.parquet
  cargo/
    snapshot/voyage_id={id}/manifest.parquet
  sanctions/
    ofac_sdn.parquet
    un_consolidated.parquet
  ownership/
    graph.parquet
  gdelt/
    events/date=YYYY-MM-DD/events.parquet
  features/
    vessel_features.parquet
  score/
    singapore_watchlist.parquet
    japansea_watchlist.parquet
  ais-summaries/
    vessel_id=IMO{7digits}/summary.parquet
  voyage-evidence/
    vessel_id=IMO{7digits}/voyage_id={id}/evidence.parquet
```

## arktrace-public partition layout

Populated by R2-to-R2 copy from `maridb-public` after validation.

```
arktrace-public/
  watchlist/
    singapore_watchlist.parquet
    japansea_watchlist.parquet
  features/
    vessel_features.parquet
  ais-summaries/
    vessel_id=IMO{7digits}/summary.parquet
```

## documaris-public partition layout

Voyage evidence and regulatory KB populated by R2-to-R2 copy from `maridb-public`.
Form templates and WASM assets written by documaris CI/CD at release time.
Audit log written by documaris app at runtime (append-only).

```
documaris-public/
  voyage-evidence/
    vessel_id=IMO{7digits}/voyage_id={id}/evidence.parquet
  regulatory-kb/
    singapore/
      port-marine-circulars/YYYY-MM-DD.json
      ica-requirements/current.json
      tradenet-field-map/current.json
    imo/
      fal-convention/form1-fields.json
      fal-convention/form5-fields.json
  form-templates/
    fal-form-1/template.html.j2
    fal-form-5/template.html.j2
    singapore-port-entry/template.html.j2
  audit-log/
    YYYY/MM/DD/{vessel_imo}_{timestamp}.jsonl
  wasm-assets/
    documaris-wasm-{version}.wasm
    documaris-wasm-{version}.js
```

## Validation gates

Two mandatory gates. Either failure halts the pipeline; previous versions remain live.

### Gate 1 — before writing to maridb-public

Runs immediately after each pipeline step, before the output is uploaded to `maridb-public`.

| Check | Failure action |
|---|---|
| Schema conformance — expected columns, types present | Abort upload; alert |
| No nulls in required fields | Abort upload; alert |
| Row count ≥ minimum threshold for the data type | Abort upload; alert |
| Output is newer than the current version in maridb-public | Abort upload; alert |

### Gate 2 — before R2-to-R2 copy to app buckets

Runs after maridb-public is updated, before copying to `arktrace-public` or `documaris-public`.

| Check | Target bucket | Failure action |
|---|---|---|
| Watchlist row count within expected range | arktrace-public | Abort copy |
| AIS coverage ≥ threshold for the region | arktrace-public | Abort copy |
| Regulatory KB diff — no silent key deletion | documaris-public | Abort copy |
| Voyage evidence coverage ≥ threshold | documaris-public | Abort copy |

Previous version in the app bucket remains live on Gate 2 failure.
