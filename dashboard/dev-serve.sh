#!/usr/bin/env bash
# Serve the metrics dashboard locally with mock data.
# Visit: http://localhost:8080/?local
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
METRICS_DIR="$SCRIPT_DIR/metrics"

mkdir -p "$METRICS_DIR"

# Generate mock snapshots for the last 7 days
python3 - <<'EOF'
import json, os, random
from datetime import date, timedelta

metrics_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metrics")
os.makedirs(metrics_dir, exist_ok=True)

base_p50      = 0.356
base_recall   = 1.0
base_lead     = 29
base_uu       = 50
base_positives = 89
regions = ["singapore", "japan", "europe", "blacksea", "middleeast"]

entries = []
for i in range(7):
    d = date.today() - timedelta(days=6 - i)
    key = d.strftime("%Y%m%d")
    entries.append(key)

    noise = random.uniform(-0.02, 0.02)
    snap = {
        "date": d.isoformat(),
        "precision_at_50": round(base_p50 + noise, 4),
        "precision_at_50_ci_low": round(base_p50 + noise - 0.07, 4),
        "precision_at_50_ci_high": round(base_p50 + noise + 0.07, 4),
        "recall_at_200": base_recall,
        "auroc": round(0.87 + random.uniform(-0.03, 0.03), 4),
        "known_positives": base_positives + random.randint(-2, 2),
        "regions": regions,
        "skipped_regions": [],
        "pre_designation_count": 6 + random.randint(-1, 1),
        "mean_lead_days": base_lead + random.randint(-3, 3),
        "median_lead_days": base_lead + random.randint(-2, 2),
        "unknown_unknown_candidates": base_uu + random.randint(-5, 5),
        "generated_at_utc": f"{d.isoformat()}T01:00:00+00:00",
    }
    path = os.path.join(metrics_dir, f"{key}.json")
    with open(path, "w") as f:
        json.dump(snap, f, indent=2)
    print(f"  wrote {key}.json  P@50={snap['precision_at_50']}")

index = {"entries": list(reversed(entries)), "updated_at_utc": entries[-1] + "T01:00:00+00:00"}
with open(os.path.join(metrics_dir, "index.json"), "w") as f:
    json.dump(index, f, indent=2)
print(f"  wrote index.json  entries={index['entries']}")
EOF

echo ""
echo "Mock data written to dashboard/metrics/"
echo ""
echo "Starting server at http://localhost:8080"
echo "Open: http://localhost:8080/?local"
echo ""
cd "$SCRIPT_DIR"
python3 -m http.server 8080
