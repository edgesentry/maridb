# indago

*indago* — Latin: to investigate, to track, to follow a trail. Root of "investigation."

**Multi-domain OSINT data layer — ingestion, transformation, and distribution of open-source intelligence across maritime, corporate, sanctions, and trade domains.**

indago is the shared data foundation for the edgesentry product stack. It collects and transforms raw signals from multiple open-source intelligence domains into structured Parquet datasets distributed via Cloudflare R2.

### Domains

| Domain | Sources | Output |
|---|---|---|
| **Maritime** | AIS (NMEA 0183, AIS stream), port call records | Vessel tracks, voyage events, CPA/DCPA features |
| **Corporate** | Beneficial ownership registries, company filings | Ownership depth, UBO chains, corporate graph |
| **Sanctions & compliance** | OFAC, UN, EU, MAS lists | Sanctions distance, watchlist membership, cluster ratios |
| **Trade & cargo** | Cargo manifests, HS codes, port declarations | Cargo risk profiles, trade route anomalies |

Each domain produces Parquet partitions in R2, consumed by downstream products without further pipeline dependency.

## Product stack

| Product | Role |
|---|---|
| **indago** | OSINT data layer — ingest, transform, validate, distribute to R2 buckets |
| **arktrace** | Shadow fleet detection — reads maritime + sanctions features from R2 |
| **documaris** | Port call document generation — reads voyage and cargo data from R2 |
| **clarus** | Physical safety layer — reads vessel behavioral features from R2 |

## R2 buckets

| Bucket | URL | Contents |
|---|---|---|
| `maridb-public` | `https://pub-e088008b61ee432b906ef710d52af28c.r2.dev` | AIS, vessels, voyages, cargo, sanctions, ownership |
| `arktrace-public` | existing | Detection-ready features, watchlist, SHAP signals |
| `documaris-public` | `https://pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev` | Regulatory KB, form templates, voyage evidence |

All buckets: unauthenticated public read.

See [`docs/ref-r2-buckets.md`](docs/ref-r2-buckets.md) for full partition layout and data flow.

## Repository layout

```
indago/
  .agents/skills/      # agent skills (npx skills add edgesentry/indago)
  docs/                # reference material (ref-*.md)
  pipelines/           # ingest and transformation pipeline implementations
  scripts/             # one-off upload and maintenance scripts
  config/              # pipeline configuration (sources, schedules, thresholds)
  AGENTS.md            # agent-facing guide (directory map, R2 buckets, skills)
  CONTRIBUTING.md      # scope, layering, documentation rules
  wrangler.toml        # R2 bucket bindings
```

## Related issues

- [#1 — Implement data pipelines](https://github.com/edgesentry/indago/issues/1)
- [#2 — R2 bucket architecture](https://github.com/edgesentry/indago/issues/2)
- [arktrace#517 — Decouple arktrace from data pipelines](https://github.com/edgesentry/arktrace/issues/517)
- [documaris#8 — documaris-public R2 bucket schema](https://github.com/edgesentry/documaris/issues/8)
