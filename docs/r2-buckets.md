# maridb — R2 Bucket Architecture

**Updated:** 2026-04-25

## Buckets

| Bucket | Public URL | Access | Writer | Readers |
|---|---|---|---|---|
| `arktrace-public` | TBD (existing) | Unauthenticated GET | maridb pipelines | arktrace app |
| `maridb-public` | `https://pub-e088008b61ee432b906ef710d52af28c.r2.dev` | Unauthenticated GET | maridb pipelines | arktrace, documaris, any app |
| `documaris-public` | `https://pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev` | Unauthenticated GET | maridb pipelines + documaris CI | documaris app |

## Data flow

```
[Raw sources]
  AIS feeds / vessel registries / sanctions / cargo manifests / port circulars
        │
        ▼
  maridb ingest pipelines
        │
        ▼
  maridb-public/           ← intermediate Parquet lake (raw + cleaned data)
        │
        ▼
  maridb aggregation + transformation pipelines
        │
        ├── validate() ──→ arktrace-public/    (detection-ready features, AIS summaries)
        │
        └── validate() ──→ documaris-public/   (regulatory KB, voyage evidence Parquet,
                                                 form pre-fill data)
```

maridb owns all pipeline execution. Apps only read from their respective public buckets.

## maridb-public partition layout

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
```

## arktrace-public partition layout

```
arktrace-public/
  watchlist/
    snapshot.parquet                    # scored vessel watchlist
  signals/
    shap/date=YYYY-MM-DD/signals.parquet
  ais-summaries/
    vessel_id=IMO{7digits}/summary.parquet
```

## documaris-public partition layout

```
documaris-public/
  regulatory-kb/
    singapore/
      port-marine-circulars/YYYY-MM-DD.json
      ica-requirements/current.json
      tradenet-field-map/current.json
    imo/
      fal-convention/form1-fields.json
      fal-convention/form5-fields.json
  voyage-evidence/
    vessel_id=IMO{7digits}/voyage_id={id}/evidence.parquet
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

## Validation gate

Before maridb uploads aggregated output to an app bucket:

1. **Schema check** — expected columns, types, no nulls in required fields
2. **Completeness check** — row count vs. expected source volume
3. **arktrace-public** — AIS event coverage ≥ threshold before pushing detection-ready features
4. **documaris-public** — regulatory KB diff reviewed; silent deletion blocks upload

Validation failures halt the upload; previous version in bucket remains live.
