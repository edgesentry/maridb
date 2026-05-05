# Roadmap

## Current state (PoC)

The 11-step pipeline runs daily via GitHub Actions across 5 regions (Singapore, Japan Sea, Middle East, Europe/Baltic, US Gulf). Data is distributed to Cloudflare R2 and consumed by arktrace, documaris, and clarus.

| Phase | Status | Scope |
|-------|--------|-------|
| **1 — Core pipeline** | ✅ Live | AIS + sanctions + vessel registry → HDBSCAN + Isolation Forest + composite score → R2 |
| **2 — Causal calibration** | ✅ Live | C3 causal model (DiD + HC3 OLS) calibrates composite score; backtracking loop for delayed labels |
| **3 — EO/SAR fusion** | 🔲 Planned | Satellite optical + SAR detections fused into feature matrix via `pipelines/features/` |
| **4 — Expanded domains** | 🔲 Planned | Engine/sensor logs (IMO shipboard OT); port call sequencing from documaris data |
| **5 — indago-public bucket** | 🔲 Planned | Rename `maridb-public` → `indago-public`; requires coordinated migration with arktrace, documaris, clarus |

## Phase 3 — EO/SAR fusion

- Ingest Sentinel-1 SAR detections and optical EO imagery via existing `pipelines/ingest/` extension point
- Add `eo_detection_count`, `sar_dark_vessel_flag`, `optical_id_confidence` to feature matrix
- Gate: confidence < 0.5 → suppress, log `CoverageGap` event to audit chain
- Accuracy targets: night detection ~78% (vs 45% generic), rain/obstruction ~71% (vs 38%)

## Phase 4 — Expanded domains

- Engine/sensor logs from edgesentry physical inspection layer → shipboard OT anomaly features
- Port call sequence analysis using documaris FAL Form data → route deviation scoring
- Japan NACCS port entry data → Asia-Pacific voyage coverage expansion

## Phase 5 — Bucket migration

Migration from `maridb-public` to `indago-public` requires coordinated changes across all downstream consumers. Trigger: before any production deployment to avoid split naming in live systems.

Migration checklist:
- [ ] Create `indago-public` bucket in Cloudflare R2
- [ ] Update `wrangler.toml` bindings
- [ ] Update `scripts/sync_r2.py` upload targets
- [ ] Coordinate with arktrace, documaris, clarus to update their R2 read paths
- [ ] Deprecate `maridb-public` after all consumers migrated
