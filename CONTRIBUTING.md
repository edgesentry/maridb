# Contributing

## Scope

indago is the **OSINT data layer** of the edgesentry platform. It ingests, transforms, and distributes raw intelligence signals. Do not add detection logic, document generation, or UI components here — those belong in [arktrace](https://github.com/edgesentry/arktrace), [documaris](https://github.com/edgesentry/documaris), and [clarus](https://github.com/edgesentry/clarus) respectively.

## Layering

| Layer | Where | What belongs there |
|-------|-------|-------------------|
| OSINT ingestion + transformation | this repo | AIS, sanctions, corporate, trade pipelines → R2 |
| Shadow fleet detection | arktrace | Causal inference scoring, watchlist, analyst dashboard |
| Port call documents | documaris | Form generation, compliance checking, audit trail |
| Vessel risk intelligence | clarus | Physical safety scoring, GUI |

## Language

English is the single source of truth for all documentation.

## Documentation rules

1. **README.md** — human-facing, high-level only
2. **AGENTS.md** — agent-facing: directory map, R2 buckets, external dep map, skills
3. **Agent Skills** — step-by-step procedures (`npx skills add edgesentry/indago`)
4. **`docs/`** — reference material only (design decisions, data contracts, model specs)
5. **No duplication** — each fact lives in exactly one place
6. **No cargo doc territory** — don't duplicate types, fields, or method signatures from code

### File naming

All files under `docs/` use `kebab-case.md` with role prefixes:

| Prefix | Use for |
|--------|---------|
| `ref-` | Design references, data layout, model specs, governance |
| `roadmap/` | Roadmap documents (subdirectory) |

### Skill-first policy

Before adding a procedure to `docs/`, create a Skill instead. Detailed CLI references and runbooks belong in `.agents/skills/<skill>/references/`. Only reference material (facts, schemas, design decisions) goes in `docs/`.

## Agent Skills

Skills use the `indago-` prefix, follow the [agentskills.io](https://agentskills.io/specification) spec, and live in `.agents/skills/`.

## Issues

Add every new issue to the relevant [project board](https://github.com/orgs/edgesentry/projects) with a priority set.

| Label | Meaning |
|-------|---------|
| `priority:P0` | Blocks a release or core functionality |
| `priority:P1` | High value, scheduled near-term |
| `priority:P2` | Valuable but deferrable |
