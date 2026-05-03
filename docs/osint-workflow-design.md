# OSINT Investigation Workflow Design

Two complementary workflows for cross-referencing the arktrace watchlist against open-source intelligence. They serve different purposes and different users — use both.

---

## Overview

| | Case A — Local LLM | Case B — GitHub Actions |
|---|---|---|
| **Who** | Individual analyst, pre-pitch | Automated, team-wide |
| **Trigger** | Manual (analyst decides when) | Weekly schedule + on-demand |
| **Speed** | 10–20 min interactive session | ~3 min, runs unattended |
| **Output** | Narrative briefing, slide draft | Structured GitHub Issue |
| **MarineTraffic / news** | ✅ LLM browses and reads | ❌ Links only (no scraping) |
| **Synthesis / "why now"** | ✅ LLM reasons across sources | ❌ Structured data only |
| **Audit trail** | On human approval → GH Issue | Automatic every run |
| **Local setup required** | llama.cpp / Ollama | None (GitHub-hosted runner) |

Both cases share the same **deterministic core scripts** for data preparation.

---

## Case A — Local LLM-assisted analyst investigation

**Issue:** [arktrace#551](https://github.com/edgesentry/arktrace/issues/551)

### When to use

- Before a pitch or demo (need narrative, not just data)
- When a new OSINT story breaks and you want to check the watchlist immediately
- When you need to explain *why* a vessel matters, not just that it scores high

### Workflow

```
Step 1 — Data preparation (deterministic, ~30 sec)
  uv run python scripts/osint_watchlist_check.py --top 50 --output /tmp/watchlist.json

Step 2 — LLM investigation session (interactive, 10-20 min)
  # In Claude Code or local LLM session:
  # "Read /tmp/watchlist.json. For sanctions_distance=0 vessels,
  #  search MarineTraffic and current news. Tell me which are
  #  most relevant to the Iran/Russia shadow fleet today."

Step 3 — Human review and redirect
  # "Focus on PIONEER 92. What is its connection to Singapore EOPL?"
  # "Draft a 3-sentence pitch slide about this vessel."
  # "Is there a newer OFAC designation this week that affects any of these?"

Step 4 — Approve and record
  # Human confirms findings
  # LLM posts approved summary to GitHub Issue (audit trail)
```

### What the LLM can do that scripts cannot

- Fetch and read MarineTraffic / VesselFinder vessel detail pages
- Search and summarise current Reuters / Al Jazeera / Lloyd's List articles
- Synthesise across multiple sources: "vessel X appears in 3 OSINT sources this week"
- Explain unallocated MMSI patterns in plain language
- Draft pitch narrative in the correct framing for DSTA / MPA / insurance audiences
- Respond to follow-up questions ("Is this vessel currently in Hormuz?")

### Local LLM requirements

- **llama.cpp** or **Ollama** running locally (CPU sufficient — no GPU required for reasoning tasks)
- Model: Llama 3.1 8B or equivalent — small enough for Raspberry Pi 5
- Tool use: web fetch, file read (standard Claude Code / Ollama tool use)

---

## Case B — Automated GitHub Actions workflow

**Issue:** [arktrace#550](https://github.com/edgesentry/arktrace/issues/550)

### When to use

- Weekly routine check — team stays current without manual effort
- Trigger after a major OFAC/UN sanctions action to see watchlist impact
- CI gate: "did this week's pipeline run produce any new sanctions hits?"

### Workflow

```
Trigger: schedule (weekly) OR workflow_dispatch (manual)

Step 1 — Pull top-50 from maridb-public R2
Step 2 — Detect stateless / unallocated MMSIs
Step 3 — Cross-reference OpenSanctions DB (local)
Step 4 — Generate MarineTraffic / OFAC links for each vessel
Step 5 — Post GitHub Issue with structured findings table
```

### Output format

GitHub Issue titled `[OSINT Check] Watchlist — YYYY-MM-DD`:

```markdown
## sanctions_distance=0 (6 vessels)

| Rank | MMSI | IMO | Name | Flag | Conf | Links |
|---|---|---|---|---|---|---|
| 5 | 312171000 | 9354521 | ANHONA | BZ | 0.566 | [MT](…) [OFAC](…) |
| 7 | 457133000 | 9340934 | PIONEER 92 | MN | 0.489 | [MT](…) [OFAC](…) |

## Stateless MMSIs (unallocated MID)

| Rank | MMSI | Conf | MID | Note |
|---|---|---|---|---|
| 2 | 400789012 | 0.606 | 400 | ⚠ ITU unallocated |
```

Human clicks links to verify on MarineTraffic / Tanker Trackers.

### What GitHub Actions cannot do

- Read MarineTraffic pages (scraping prohibited)
- Interpret news articles
- Explain why a vessel is relevant *this week* specifically
- Generate pitch narrative

---

## Shared deterministic core

Both cases use the same script for data preparation:

```bash
# scripts/osint_watchlist_check.py
# Input:  maridb-public/score/candidate_watchlist.parquet (R2)
# Output: structured JSON with:
#   - top-N vessels sorted by confidence
#   - sanctions_distance=0 flag
#   - stateless MMSI detection (ITU MID lookup)
#   - pre-built MarineTraffic / VesselFinder / OFAC search URLs
```

The script is deterministic and fast. The LLM (Case A) or GitHub Actions (Case B) consumes its output.

---

## Combined architecture

```
maridb-public R2
      │
      ▼
osint_watchlist_check.py  ←── deterministic, shared
      │
      ├──► Case A: LLM session ──► human review ──► GH Issue (narrative)
      │
      └──► Case B: GHA workflow ──► GH Issue (structured, automatic)
```

Run both. Case B catches things while you sleep. Case A goes deep before you present.

---

## CAP Vista pitch relevance

> "An analyst preparing for an operational briefing triggers a 10-minute investigation session. The system pulls the current watchlist, the LLM cross-references live OSINT and drafts a briefing note. The LLM runs on llama.cpp — on the same Raspberry Pi 5 as the AIS receiver. No cloud dependency, no latency, no data leaving the device. That is the edge deployment story."

This directly addresses Cap Vista's requirements for:
- Low-compute edge deployment
- Analyst decision support
- Operational continuity under network-denied conditions

---

## References

- [Shadow Fleet OSINT Playbook](shadow-fleet-osint-playbook.md) — manual procedure this automates
- [arktrace#550](https://github.com/edgesentry/arktrace/issues/550) — GitHub Actions implementation
- [arktrace#551](https://github.com/edgesentry/arktrace/issues/551) — Local LLM implementation
