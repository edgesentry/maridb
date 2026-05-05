---
name: indago-sync-r2
description: Sync processed data to Cloudflare R2 buckets. Use when publishing new Parquet partitions, updating watchlists, or preparing demo bundles.
license: Apache-2.0
compatibility: Requires wrangler CLI and Cloudflare R2 credentials
metadata:
  repo: indago
---

## Full sync (AIS + GDELT + watchlists + demo bundles)

```bash
uv run python scripts/sync_r2.py
```

## Validate after upload

```bash
uv run python scripts/validate_ais_upload.py
```

## Buckets

| Bucket | Contents |
|--------|---------|
| `maridb-public` | AIS, vessels, voyages, cargo, sanctions, ownership Parquet |
| `arktrace-public` | Detection-ready features, watchlist, SHAP signals |
| `documaris-public` | Regulatory KB, form templates, voyage evidence |

R2 bindings are defined in `wrangler.toml`. See [docs/ref-r2-buckets.md](../../docs/ref-r2-buckets.md) for full partition layout.
