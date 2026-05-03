# indago

**Maritime data layer — vessel, voyage, cargo, and AIS ingestion and transformation pipelines.**

maridb is the shared data foundation for the edgesentry product stack. It collects and transforms raw vessel, voyage, cargo, and AIS data into structured Parquet datasets distributed via Cloudflare R2.

## Quick links

- [R2 Buckets](r2-buckets.md) — bucket architecture, partition layout, data flow

## Product stack

| Product | Role |
|---|---|
| **maridb** | Data layer — ingest, transform, validate, distribute to public R2 buckets |
| **arktrace** | Shadow fleet detection — reads from `arktrace-public` R2. [github.com/edgesentry/arktrace](https://github.com/edgesentry/arktrace) |
| **documaris** | Port call document generation — reads from `documaris-public` and `maridb-public` R2. [github.com/edgesentry/documaris](https://github.com/edgesentry/documaris) |
| **edgesentry** | Physical layer — robotic inspection, sensor deployment |

## R2 buckets

| Bucket | URL | Contents |
|---|---|---|
| `maridb-public` | [pub-e088008b61ee432b906ef710d52af28c.r2.dev](https://pub-e088008b61ee432b906ef710d52af28c.r2.dev) | AIS, vessels, voyages, cargo, sanctions, ownership |
| `arktrace-public` | existing | Detection-ready features, watchlist, SHAP signals |
| `documaris-public` | [pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev](https://pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev) | Regulatory KB, form templates, voyage evidence |

All buckets: unauthenticated public read.

## Source

- GitHub: [github.com/edgesentry/indago](https://github.com/edgesentry/indago)
