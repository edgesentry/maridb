---
name: indago-launchagents
description: Install, unload, and check status of the indago macOS LaunchAgents (AIS stream + R2 sync). Use when setting up a new machine, after a plist change, or to verify streams are alive and uploading.
license: Apache-2.0
compatibility: macOS only — requires launchctl. Credentials must be pre-filled in config/launchagents/*.local.plist before installing.
metadata:
  repo: indago
---

## LaunchAgents

| Label | Purpose | Schedule |
|-------|---------|----------|
| `io.indago.aisstream` | Streams live AIS positions into `~/.indago/data/raw/ais/*.duckdb` | Continuous (KeepAlive) |
| `io.indago.r2sync` | Exports DuckDB → Parquet and uploads to `maridb-public` R2 | Hourly |

Plist files live in `config/launchagents/`. The `*.local.plist` files contain real credentials and are gitignored — edit them before installing on a new machine.

---

## Install (new machine setup)

```bash
# 1. Fill credentials into the local plist files first:
#    config/launchagents/io.indago.aisstream.local.plist  → AISSTREAM_API_KEY
#    config/launchagents/io.indago.r2sync.local.plist     → AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET

# 2. Copy to LaunchAgents and load
cp config/launchagents/io.indago.aisstream.local.plist ~/Library/LaunchAgents/io.indago.aisstream.plist
cp config/launchagents/io.indago.r2sync.local.plist    ~/Library/LaunchAgents/io.indago.r2sync.plist

launchctl load ~/Library/LaunchAgents/io.indago.aisstream.plist
launchctl load ~/Library/LaunchAgents/io.indago.r2sync.plist
```

## Reload after plist change

```bash
launchctl unload ~/Library/LaunchAgents/io.indago.aisstream.plist
launchctl unload ~/Library/LaunchAgents/io.indago.r2sync.plist

cp config/launchagents/io.indago.aisstream.local.plist ~/Library/LaunchAgents/io.indago.aisstream.plist
cp config/launchagents/io.indago.r2sync.local.plist    ~/Library/LaunchAgents/io.indago.r2sync.plist

launchctl load ~/Library/LaunchAgents/io.indago.aisstream.plist
launchctl load ~/Library/LaunchAgents/io.indago.r2sync.plist
```

## Unload (stop)

```bash
launchctl unload ~/Library/LaunchAgents/io.indago.aisstream.plist
launchctl unload ~/Library/LaunchAgents/io.indago.r2sync.plist
```

---

## Check status

### Are agents running?

```bash
launchctl list | grep indago
```

Expected output:
```
<PID>   0   io.indago.aisstream   ← PID present = running; exit code 0 = healthy
-       0   io.indago.r2sync      ← no PID = idle between hourly runs; exit code 0 = healthy
```

A non-zero exit code means the last run failed — check the logs.

### AIS stream — live?

```bash
tail -20 ~/.indago/logs/aisstream.log
```

Look for recent `Flushed N records → N inserted (total N)` lines. If the log is stale (no new lines for >10 min), the stream may have dropped.

### R2 sync — what was uploaded recently?

```bash
grep -v "already in R2\|vessel_meta\|Reading\|rows across\|Done\.\|skipping" \
  ~/.indago/logs/r2sync.log | grep -v "^$" | tail -30
```

Look for lines like:
```
2026-05-06 (76,209 rows) -> staging + maridb-public/ais/region=europe/date=2026-05-06/positions.parquet
```

### Errors?

```bash
tail -20 ~/.indago/logs/aisstream.err
tail -20 ~/.indago/logs/r2sync.err
```

Common non-fatal errors:
- `[skip] cannot open DB ... Conflicting lock` — AIS stream holds the DuckDB write lock while r2sync tries to read; harmless, r2sync skips and retries next hour.
- `ACCESS_DENIED ... indago-public` — stale error from before the bucket was renamed to `maridb-public`; ignore if `r2sync.log` shows successful uploads to `maridb-public`.

### Record counts per region (DuckDB)

```bash
uv run python - <<'EOF'
import duckdb, os
from pathlib import Path
data_dir = Path.home() / ".indago/data/raw/ais"
for db in sorted(data_dir.glob("*.duckdb")):
    try:
        con = duckdb.connect(str(db), read_only=True)
        total = con.execute("SELECT COUNT(*) FROM ais_positions").fetchone()[0]
        last  = con.execute("SELECT MAX(timestamp) FROM ais_positions").fetchone()[0]
        con.close()
        print(f"{db.stem:20} total={total:>9,}  latest={last}")
    except Exception as e:
        print(f"{db.stem:20} error: {e}")
EOF
```
