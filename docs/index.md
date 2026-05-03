# indago

*indago* — Latin: to investigate, to track, to follow a trail. Root of "investigation."

**Multi-domain OSINT data layer — ingestion, transformation, and distribution of open-source intelligence across maritime, corporate, sanctions, and trade domains.**

indago is the shared data foundation for the edgesentry product stack. It collects and transforms raw signals from multiple open-source intelligence domains into structured Parquet datasets distributed via Cloudflare R2.

## Domains

| Domain | Sources | Output |
|---|---|---|
| **Maritime** | AIS (NMEA 0183, AIS stream), port call records | Vessel tracks, voyage events, CPA/DCPA features |
| **Corporate** | Beneficial ownership registries, company filings | Ownership depth, UBO chains, corporate graph |
| **Sanctions & compliance** | OFAC, UN, EU, MAS lists | Sanctions distance, watchlist membership, cluster ratios |
| **Trade & cargo** | Cargo manifests, HS codes, port declarations | Cargo risk profiles, trade route anomalies |

## Quick links

- [R2 Buckets](r2-buckets.md) — bucket architecture, partition layout, data flow

## Product stack

| Product | Role |
|---|---|
| **indago** | OSINT data layer — ingest, transform, validate, distribute to R2 buckets |
| **arktrace** | Shadow fleet detection — reads maritime + sanctions features from R2. [github.com/edgesentry/arktrace](https://github.com/edgesentry/arktrace) |
| **documaris** | Port call document generation — reads voyage and cargo data from R2. [github.com/edgesentry/documaris](https://github.com/edgesentry/documaris) |
| **clarus** | Physical safety layer — reads vessel behavioral features from R2. [github.com/edgesentry/clarus](https://github.com/edgesentry/clarus) |

## R2 buckets

| Bucket | URL | Contents |
|---|---|---|
| `maridb-public` | [pub-e088008b61ee432b906ef710d52af28c.r2.dev](https://pub-e088008b61ee432b906ef710d52af28c.r2.dev) | AIS, vessels, voyages, cargo, sanctions, ownership |
| `arktrace-public` | existing | Detection-ready features, watchlist, SHAP signals |
| `documaris-public` | [pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev](https://pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev) | Regulatory KB, form templates, voyage evidence |

All buckets: unauthenticated public read.

## Source

- GitHub: [github.com/edgesentry/indago](https://github.com/edgesentry/indago)
