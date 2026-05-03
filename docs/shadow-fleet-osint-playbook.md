# Shadow Fleet OSINT Cross-Reference Playbook

**Purpose:** Repeatable methodology to find shadow fleet vessels in the arktrace watchlist and match them against current OSINT reporting.

---

## The core problem

Shadow fleet vessels change names frequently — sometimes weekly. Searching by name ("Flora", "Genoa") will usually fail. The reliable identifiers are **MMSI** and **IMO number**, which change less often (IMO is permanent by definition; MMSI changes require re-registration).

---

## Step 1 — Pull the current top-50

```python
import polars as pl
import pyarrow.fs as pafs
import os

endpoint = os.getenv("S3_ENDPOINT", "https://b8a0b09feb89390fb6e8cf4ef9294f48.r2.cloudflarestorage.com")
fs = pafs.S3FileSystem(
    access_key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_override=endpoint,
    region="auto",
)

with fs.open_input_file("maridb-public/score/candidate_watchlist.parquet") as f:
    wl = pl.read_parquet(f)

top50 = wl.sort("confidence", descending=True).head(50)
print(top50.select(["mmsi", "imo", "vessel_name", "flag", "confidence", "sanctions_distance"]))
```

Or use the CLI:

```bash
uv run python scripts/sync_r2.py pull --data-dir /tmp/demo
python3 -c "
import polars as pl
wl = pl.read_parquet('/tmp/demo/candidate_watchlist.parquet')
print(wl.sort('confidence', descending=True).head(50).select(['mmsi','imo','vessel_name','flag','confidence','sanctions_distance']))
"
```

---

## Step 2 — Triage by sanctions_distance

| sanctions_distance | Meaning | Action |
|---|---|---|
| 0 | Vessel or operator is directly on OFAC/UN/EU/MAS SDN list | Cross-reference immediately |
| 1 | One ownership hop from a sanctioned entity | Check operator name |
| 2 | Two hops | Monitor |
| 99 | No graph connection found | Behavioural anomaly only |

**Focus on `sanctions_distance=0` first** — these are confirmed by public lists, not just model inference.

---

## Step 3 — Detect spoofed / stateless MMSIs

Check the MMSI prefix (first 3 digits = MID, Maritime Identification Digit):

```python
UNALLOCATED_MIDS = {
    "400", "401", "402", "403", "404", "405", "406", "407", "408", "409",
    "420", "600", "601",  # examples — check ITU allocations
}

for row in top50.iter_rows(named=True):
    mid = str(row["mmsi"])[:3]
    if mid in UNALLOCATED_MIDS:
        print(f"⚠ Unallocated MID: {row['mmsi']}")
```

Vessels broadcasting unallocated MIDs are deliberately stateless — a documented shadow fleet evasion tactic (Lloyd's List, 2025). No commercial AIS platform surfaces these; arktrace does.

---

## Step 4 — Cross-reference against OSINT sources

For each `sanctions_distance=0` vessel with a valid MMSI or IMO:

### 4a. MarineTraffic
```
https://www.marinetraffic.com/en/ais/details/ships/mmsi:<MMSI>
https://www.marinetraffic.com/en/ais/details/ships/imo:<IMO>
```
Retrieves: current name, flag, last position, AIS history.

### 4b. VesselFinder
```
https://www.vesselfinder.com/?mmsi=<MMSI>
```
Alternative, sometimes faster for Russian/Iranian fleet vessels.

### 4c. OFAC SDN lookup
```
https://sanctionssearch.ofac.treas.gov/
```
Search by vessel name or IMO. Returns the specific EO and designation date.

### 4d. OpenSanctions (programmatic)
```python
# Already in our pipeline via scripts/prepare_public_sanctions_db.py
# Query vessel_meta joined with sanctions_entities for IMO cross-reference
```

### 4e. Tanker Trackers / Twitter/X
Search: `MMSI site:tankertrackers.com` or `"<vessel_name>" shadow fleet`

---

## Step 5 — Record the lead time

If a vessel appears in OSINT reporting after arktrace flagged it, calculate lead time:

```
lead_time_days = date_of_public_reporting - date_arktrace_first_scored_vessel
```

This is the key metric for the pitch: **arktrace flags vessels N days before they appear in public reporting.**

To find when arktrace first scored a vessel, check the causal effects history or the DuckLake score history table if available.

---

## What to look for in OSINT

| Signal | Where | What it means |
|---|---|---|
| OFAC/UN/EU SDN designation | OFAC press releases, OpenSanctions | Direct confirmation |
| Fake flag (Mongolia, Belize, Comoros, Gabon) | MarineTraffic flag field | High-risk registry bypass |
| Recent name change | MarineTraffic history tab | Identity reset |
| AIS dark periods near chokepoints | MarineTraffic track, Tanker Trackers | Deliberate evasion |
| STS operation near Singapore EOPL / Hormuz | Tanker Trackers, Bloomberg/Reuters | Active transfer event |
| Named in journalist investigation | Al Jazeera, Bloomberg, Reuters | Highest-value OSINT match |

---

## Recommended frequency

Run this playbook **weekly** to maintain a live OSINT-matched watchlist.
