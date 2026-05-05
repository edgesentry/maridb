---
name: indago-run-pipeline
description: Run the indago 11-step data pipeline. Use when ingesting new AIS data, refreshing features, or running a full regional screening.
license: Apache-2.0
compatibility: Requires Python >=3.12 and uv; Cloudflare R2 credentials for upload steps
metadata:
  repo: indago
---

```bash
uv run python scripts/run_pipeline.py --region singapore
```

Region presets: `singapore` `japan_sea` `middle_east` `europe_baltic` `us_gulf`

To skip upload to R2:
```bash
uv run python scripts/run_pipeline.py --region singapore --no-upload
```

See [references/pipeline-operations.md](references/pipeline-operations.md) for all 11 steps, CLI flags, Docker Compose, and recovery procedures.
