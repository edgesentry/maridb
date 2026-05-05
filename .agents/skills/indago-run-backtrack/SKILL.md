---
name: indago-run-backtrack
description: Run the backtracking delayed-label intelligence loop. Use when new analyst labels are available or to rewind causal reasoning on past detections.
license: Apache-2.0
compatibility: Requires Python >=3.12 and uv; scoring model must have run first
metadata:
  repo: indago
---

```bash
uv run python scripts/run_backtracking.py --days 30
```

Checks for new analyst labels, propagates them backward through the causal model, and flags precursor signals that should have fired.

## Regression check after backtracking

```bash
uv run python scripts/check_score_regression.py
```

See [references/backtracking-runbook.md](references/backtracking-runbook.md) for full delayed-label loop operations, evidence types, and demo scenario.
