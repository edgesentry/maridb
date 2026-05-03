# indago

**Maritime data layer — vessel, voyage, cargo, and AIS ingestion and transformation pipelines.**

maridb is the shared data foundation for the edgesentry product stack. It collects and transforms raw vessel, voyage, cargo, and AIS data into structured Parquet datasets distributed via Cloudflare R2.

## Product stack

| Product | Role |
|---|---|
| **maridb** | Data layer — ingest, transform, validate, distribute to public R2 buckets |
| **arktrace** | Shadow fleet detection — reads from `arktrace-public` R2 |
| **documaris** | Port call document generation — reads from `documaris-public` and `maridb-public` R2 |
| **edgesentry** | Physical layer — robotic inspection, sensor deployment |

## R2 buckets

| Bucket | URL | Contents |
|---|---|---|
| `maridb-public` | `https://pub-e088008b61ee432b906ef710d52af28c.r2.dev` | AIS, vessels, voyages, cargo, sanctions, ownership |
| `arktrace-public` | existing | Detection-ready features, watchlist, SHAP signals |
| `documaris-public` | `https://pub-2892b1afd7b94cc6a381e3dcec4edc30.r2.dev` | Regulatory KB, form templates, voyage evidence |

All buckets: unauthenticated public read.

See [`docs/r2-buckets.md`](docs/r2-buckets.md) for full partition layout and data flow.

## Repository layout

```
maridb/
  docs/
    r2-buckets.md      # bucket architecture, partition layout, data flow
  pipelines/           # ingest and transformation pipeline implementations
  scripts/             # one-off upload and maintenance scripts
  config/              # pipeline configuration (sources, schedules, thresholds)
  wrangler.toml        # R2 bucket bindings
```

## Related issues

- [#1 — Implement data pipelines](https://github.com/edgesentry/indago/issues/1)
- [#2 — R2 bucket architecture](https://github.com/edgesentry/indago/issues/2)
- [arktrace#517 — Decouple arktrace from data pipelines](https://github.com/edgesentry/arktrace/issues/517)
- [documaris#8 — documaris-public R2 bucket schema](https://github.com/edgesentry/documaris/issues/8)
