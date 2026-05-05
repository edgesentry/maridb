---
name: indago-run-osint
description: Generate an OSINT intelligence report for a vessel or watchlist batch. Use when investigating a flagged vessel or preparing a weekly analyst report.
license: Apache-2.0
compatibility: Requires Python >=3.12 and uv; optional local LLM for analyst workflow
metadata:
  repo: indago
---

## Automated report (GitHub Actions style)

```bash
uv run python scripts/generate_osint_report.py --region singapore
```

## Manual analyst workflow (local LLM)

```bash
# 1. Pull watchlist
uv run python scripts/generate_osint_report.py --vessel <MMSI> --local-llm

# 2. Triage by sanctions_distance
# 3. Cross-reference: IMO registry, OpenSanctions, MarineTraffic
```

See [references/osint-playbook.md](references/osint-playbook.md) for the full 5-step manual OSINT procedure and VOYAGER case study.
