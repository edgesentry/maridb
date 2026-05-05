---
name: indago-run-tests
description: Run the indago test suite. Use when changing pipeline logic, scoring models, or schema contracts.
license: Apache-2.0
compatibility: Requires Python >=3.12 and uv
metadata:
  repo: indago
---

```bash
uv run pytest tests/ -v
```

Integration tests (require external/public data, opt-in):
```bash
uv run pytest tests/ -v -m integration
```

Score regression check:
```bash
uv run python scripts/check_score_regression.py
```
