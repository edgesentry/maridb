# AGENTS

Multi-domain OSINT data layer. indago ingests raw signals from maritime, corporate, sanctions, and trade domains, transforms them into structured Parquet datasets, and distributes them via Cloudflare R2 to downstream products.

## Related repos

| Repo | Role | Relationship |
|------|------|-------------|
| [arktrace](https://github.com/edgesentry/arktrace) | Shadow fleet detection | Reads maritime + sanctions features from `arktrace-public` R2 |
| [documaris](https://github.com/edgesentry/documaris) | Port call documents | Reads voyage and cargo data from `documaris-public` R2 |
| [clarus](https://github.com/edgesentry/clarus) | Physical port safety monitoring | Reads vessel behavioral features from `maridb-public` R2 |
| [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) | Audit chain | Audit record format (BLAKE3 + Ed25519) used for signed data outputs |

## Directory map

| Path | Purpose |
|------|---------|
| `pipelines/ingest/` | Data ingestion from AIS, sanctions, vessel registry, GDELT, EO/SAR, custom feeds |
| `pipelines/features/` | Feature engineering — AIS behavior, identity volatility, ownership graph, trade flow |
| `pipelines/score/` | Scoring engines — HDBSCAN baseline, Isolation Forest, composite + causal calibration |
| `pipelines/analysis/` | Causal reasoning, drift monitoring, backtracking, label propagation |
| `pipelines/storage/` | DuckDB init, Lance Graph management |
| `pipelines/distribute/` | R2 distribution logic |
| `scripts/run_pipeline.py` | Main 11-step pipeline orchestrator |
| `scripts/sync_r2.py` | R2 bucket sync (AIS, GDELT, watchlists, demo bundles) |
| `scripts/run_backtracking.py` | Delayed-label intelligence loop |
| `scripts/generate_osint_report.py` | OSINT intelligence report generation |
| `scripts/check_score_regression.py` | Model regression validation |
| `config/sanction_regimes.yaml` | Configurable sanction regime definitions |
| `config/geopolitical_events.json` | Geopolitical filter zones |

## R2 buckets

| Bucket | Written by | Read by | Note |
|--------|-----------|---------|------|
| `maridb-public` | indago | clarus, arktrace (AIS/vessel features) | Legacy name — predates the maridb → indago rename; bucket name is fixed in Cloudflare |
| `arktrace-public` | indago | arktrace (detection-ready features, watchlist) | |
| `documaris-public` | indago | documaris (voyage evidence, regulatory KB) | |

All buckets: unauthenticated public read. See [docs/ref-r2-buckets.md](docs/ref-r2-buckets.md) for partition layout.

## External dependency map

| Symptom | Owner | Where |
|---------|-------|-------|
| Empty arktrace dashboard | indago pipeline not run / R2 not synced | `scripts/sync_r2.py` |
| documaris vessel selector empty | `documaris-public` bucket not populated | `scripts/sync_r2.py` |
| Score regression after model change | Check `check_score_regression.py` output | `scripts/check_score_regression.py` |
| AIS validation failure | AIS stream or rotation issue | `scripts/validate_ais_upload.py` |

## Key design decisions

| Decision | Detail |
|----------|--------|
| DuckDB + Polars | Local-first, no external query server — edge deployment without infra |
| Lance Graph | Sub-second ownership chain traversal for corporate domain |
| R2 as distribution layer | Each downstream product pulls its own partition — no cross-app R2 access |
| Causal calibration (C3) | Composite score calibrated by DiD + HC3 robust OLS, not raw ML score |
| Pre-label holdout | Analyst labels withheld from training until governance thresholds met |

## Coding conventions

- Python 3.12+, `uv run` for all commands
- Ruff linter + formatter (`line-length = 100`)
- pytest with `integration` marker for opt-in external data tests

## Commit convention

Conventional Commits (`fix:`, `feat:`, `feat!:`)

## Docs

- Analytics overview: `docs/ref-analytics-overview.md`
- Feature engineering: `docs/ref-feature-engineering.md`
- Scoring model: `docs/ref-scoring-model.md`
- R2 buckets: `docs/ref-r2-buckets.md`
- Pipeline catalog: `docs/ref-pipeline-catalog.md`

## Agent Skills

```bash
npx skills add edgesentry/indago
```

| Skill | Trigger |
|-------|---------|
| `/indago-run-pipeline` | Running a regional screening; ingesting new AIS data |
| `/indago-run-tests` | Changing pipeline logic, scoring models, or schema contracts |
| `/indago-sync-r2` | Publishing new Parquet partitions; preparing demo bundles |
| `/indago-run-osint` | Investigating a flagged vessel; preparing weekly analyst report |
| `/indago-run-backtrack` | New analyst labels available; rewinding causal reasoning |
