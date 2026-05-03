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
  # Pull watchlist from R2 and query sanctions DB — see indago#86 for
  # the planned replacement of the removed osint_watchlist_check.py script.
  # Interim: query data/processed/public_eval.duckdb + candidate_watchlist.parquet
  # directly (see shadow-fleet-osint-playbook.md Step 1–2 for queries).

Step 2 — LLM investigation session (interactive, 10-20 min)
  # In arktrace app: open the 5-step investigation panel on any watchlist candidate.
  # For news-triggered / sanctions-only vessels not in the watchlist:
  # run the sanctions DB gap check manually (see coverage gap pattern section below).

Step 3 — Human review and redirect
  # Focus on a specific vessel using the OSINT links generated in Step 2 of the panel.

Step 4 — Approve and record
  # Analyst approves brief → "Copy as Markdown" → paste into GitHub Issue (audit trail)
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

## Case B — Automated scheduled workflow

**Issue:** [arktrace#550](https://github.com/edgesentry/arktrace/issues/550)

**Workflow engine is not prescribed.** The core is a Python script; the scheduler is interchangeable — cron, Airflow, Prefect, GitHub Actions, or whatever the deployment environment provides.

### When to use

- Weekly routine check — team stays current without manual effort
- Trigger after a major OFAC/UN sanctions action to see watchlist impact
- Any environment where a scheduler is available

### Workflow

```
Trigger: any scheduler (cron / Airflow / GHA / manual)

Step 1 — Pull top-50 from R2 (or local pipeline output)
Step 2 — Detect stateless / unallocated MMSIs
Step 3 — Cross-reference OpenSanctions DB (local)
Step 4 — Generate MarineTraffic / OFAC links for each vessel
Step 5 — Write structured report (file / issue / notification)
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

Both cases require the same structured data:

- Top-N vessels sorted by confidence
- `sanctions_distance=0` flag
- Stateless MMSI detection (ITU MID lookup)
- Pre-built MarineTraffic / VesselFinder / OFAC search URLs

`scripts/osint_watchlist_check.py` was removed because its output was designed to be fed into a Claude Code session (Case A). The replacement approach — surfacing this data natively in arktrace's investigation panel and as a scheduled indago pipeline output — is tracked in [indago#86](https://github.com/edgesentry/indago/issues/86).

---

## Combined architecture

```
maridb-public R2
      │
      ▼
candidate_watchlist.parquet + sanctions_entities  ←── deterministic, shared
      │
      ├──► Case A: arktrace investigation panel ──► human review ──► GH Issue
      │
      └──► Case B: scheduled workflow ──► structured report (automatic)
```

Run both. Case B catches things while you sleep. Case A goes deep before you present.

---

## Coverage gap pattern — vessel in sanctions DB but absent from watchlist

indago's scoring pipeline requires AIS positional data to compute behavioural
features. If a vessel is not broadcasting AIS (or is broadcasting under a different
MMSI after a flag change), it accumulates no behavioural score and does not appear
in `candidate_watchlist.parquet` — even if it is directly sanctioned.

The `sanctions_entities` table is sourced from OpenSanctions (OFAC, EU, UN, MAS)
and is independent of AIS coverage. A vessel can therefore be present in
`sanctions_entities` while completely absent from the watchlist. This gap is not a
bug: it means the vessel has evaded our AIS observation window.

**How to detect the gap:**

```python
import duckdb, polars as pl
from pathlib import Path

db = duckdb.connect("data/processed/public_eval.duckdb")
wl = pl.read_parquet(Path.home() / ".maridb/data/candidate_watchlist.parquet")

# Query sanctions DB by vessel name or known MMSI/IMO
hits = db.execute(
    "SELECT name, mmsi, imo, flag, lists FROM sanctions_entities "
    "WHERE UPPER(name) LIKE '%VOYAGER%'"
).df()
print(hits)

# Cross-reference: is the MMSI also in the watchlist?
for _, row in hits.iterrows():
    in_wl = wl.filter(pl.col("mmsi") == row["mmsi"]).height
    print(f"{row['name']}  MMSI={row['mmsi']}  in_watchlist={in_wl > 0}")
```

**When you find a gap, trigger Case A.** The sanctions DB entry provides the
anchor (MMSI, IMO, flag at time of designation); the LLM investigates the current
AIS state, flag changes, and news coverage to reconstruct the vessel's recent
activity.

### Worked example — VOYAGER (May 2026)

Starting point: Al Jazeera investigative report 2026-04-30,
[*Tracking the shadow fleet: How Iran evaded the US naval blockade in Hormuz*](https://www.aljazeera.com/economy/2026/4/30/tracking-the-shadow-fleet-how-iran-evaded-the-us-naval-blockade-in-hormuz).
The article named a vessel "Pola" in the Hormuz/Iran shadow fleet context. A name
search in `sanctions_entities` found no direct match — but returned 20 POLA-series
Sovcomflot tankers (OFAC SDN, EO 14024, all linked to Sakhalin-2 LNG exports from
Russia). Following the Sakhalin-2 thread, a Sankei Shimbun report (2026-05-01)
identified VOYAGER — an Oman-flagged crude tanker carrying Sakhalin-2 Russian crude
inbound to Kikuma port, Ehime (Taiyo Oil refinery) after transiting the Osumi
Strait. The vessel is OFAC-sanctioned.

Running the gap check:

```
sanctions_entities result:
  name=Voyager  MMSI=314906000  IMO=9843560  flag=km (Comoros at designation)
  lists: us_ofac_sdn, eu_journal_sanctions, ca_dfatd_sema_sanctions, gb_fcdo_sanctions, ch_seco_sanctions

candidate_watchlist result:
  MMSI 314906000 → 0 rows   (not in watchlist)
  IMO  9843560   → 0 rows   (not in watchlist)
```

**Interpretation:**

| Finding | Implication |
|---|---|
| In `sanctions_entities` (`us_ofac_sdn`) | Vessel is OFAC-designated — public fact, confirmed |
| Flag changed Comoros → Oman | MMSI likely changed after re-registration; old MMSI no longer broadcasting |
| Not in `candidate_watchlist` | No AIS coverage under the known MMSI → behavioural score cannot be computed |
| Not in `ais_positions` | Vessel is either AIS-dark or has adopted a new MMSI |

**Why it matters for the pitch:** indago's sanctions layer caught the vessel; the
AIS behavioural layer could not. This is the exact gap arktrace's causal inference
fills — connecting ownership network signals to vessels that have gone dark.

**Next step:** Hand off to arktrace Case A investigation. See
[arktrace investigation test cases](https://edgesentry.github.io/arktrace/investigation-test-cases/)
for the full analyst workflow applied to this vessel.

---

## Edge deployment note

> "An analyst preparing for an operational briefing triggers a 10-minute investigation session. The system pulls the current watchlist, the LLM cross-references live OSINT and drafts a briefing note. The LLM runs on llama.cpp — on the same Raspberry Pi 5 as the AIS receiver. No cloud dependency, no latency, no data leaving the device."

This workflow is designed for:
- Low-compute edge deployment (Raspberry Pi 5 / laptop — no GPU required)
- Analyst decision support in forward-deployed or network-constrained environments
- Operational continuity when internet connectivity is unreliable or unavailable

---

## References

- [Shadow Fleet OSINT Playbook](shadow-fleet-osint-playbook.md) — manual procedure this automates
- [arktrace investigation test cases](https://edgesentry.github.io/arktrace/investigation-test-cases/) — analyst LLM workflow including news-triggered cases
- [arktrace#550](https://github.com/edgesentry/arktrace/issues/550) — GitHub Actions implementation
- [arktrace#551](https://github.com/edgesentry/arktrace/issues/551) — Local LLM implementation
