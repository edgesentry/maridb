# indago

*indago* — Latin: to investigate, to track, to follow a trail.

**Multi-domain OSINT data layer — ingestion, transformation, and distribution of open-source intelligence across maritime, corporate, sanctions, and trade domains.**

indago is the shared data foundation for the edgesentry product stack. It collects and transforms raw signals from multiple OSINT domains into structured Parquet datasets distributed via Cloudflare R2.

## Design

- [ref-analytics-overview.md](ref-analytics-overview.md) — end-to-end pipeline data flow
- [ref-feature-engineering.md](ref-feature-engineering.md) — 27 features across 6 families
- [ref-scoring-model.md](ref-scoring-model.md) — HDBSCAN baseline, Isolation Forest, composite scoring
- [ref-causal-analysis.md](ref-causal-analysis.md) — causal inference, DiD + HC3 robust OLS
- [ref-field-investigation.md](ref-field-investigation.md) — physical measurement tiers, VDES
- [ref-triage-governance.md](ref-triage-governance.md) — human-in-the-loop review workflow

## Operations

- [ref-pipeline.md](ref-pipeline.md) — pipeline overview and data flow
- [ref-pipeline-catalog.md](ref-pipeline-catalog.md) — 8 pipeline types and when to use each
- [ref-regional-playbooks.md](ref-regional-playbooks.md) — per-region operational notes
- [ref-r2-buckets.md](ref-r2-buckets.md) — bucket architecture and data flow
- [ref-r2-data-layout.md](ref-r2-data-layout.md) — partition layout, data classification

## Validation

- [ref-backtesting.md](ref-backtesting.md) — historical evaluation workflow
- [ref-evaluation-metrics.md](ref-evaluation-metrics.md) — Precision@K, AUROC, acceptance thresholds
- [ref-precision-plan.md](ref-precision-plan.md) — P@50 improvement strategy
- [ref-prelabel-governance.md](ref-prelabel-governance.md) — analyst pre-label holdout policy

## Reference

- [ref-osint-workflow.md](ref-osint-workflow.md) — local LLM vs GitHub Actions OSINT workflows
- [ref-study-guide.md](ref-study-guide.md) — 21-day curriculum

## Product stack

| Product | Role |
|---------|------|
| **indago** | OSINT data layer — ingest, transform, validate, distribute to R2 |
| [arktrace](https://github.com/edgesentry/arktrace) | Shadow fleet detection — reads maritime + sanctions features from R2 |
| [documaris](https://github.com/edgesentry/documaris) | Port call documents — reads voyage and cargo data from R2 |
| [clarus](https://github.com/edgesentry/clarus) | Vessel risk intelligence — reads vessel behavioral features from R2 |
