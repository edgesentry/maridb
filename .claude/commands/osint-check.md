# /osint-check — Analyst OSINT Investigation Session

Run a structured OSINT investigation session on the arktrace watchlist. This command drives a human-in-the-loop analyst workflow: deterministic data preparation first, then LLM-assisted investigation with the analyst confirming and redirecting at each step.

## Step 1 — Prepare the watchlist

Run the deterministic script to pull the top-50 vessels, detect stateless MMSIs, and generate OSINT links:

```bash
uv run python scripts/osint_watchlist_check.py --top 50 --output /tmp/watchlist.json
```

Read the output file:

```
Read /tmp/watchlist.json
```

## Step 2 — Initial triage

Summarise the findings in 3–5 sentences:
- How many vessels have `sanctions_distance=0`?
- How many have stateless (ITU-unallocated) MMSIs?
- Which vessels are highest priority for investigation right now?

## Step 3 — Deep investigation

For each priority vessel (start with `sanctions_distance=0`, then stateless MMSIs):

1. Fetch the MarineTraffic page using the `links.marinetraffic_mmsi` URL from the JSON.
2. Search for current news: vessel name, MMSI, IMO, operator name.
3. Summarise: flag, type, last known position, any recent news or OFAC designation.
4. State why this vessel is (or is not) relevant **right now**.

Present findings vessel by vessel. Wait for the analyst to redirect before proceeding to the next.

## Step 4 — Analyst redirect

The analyst may ask:
- "Focus on [VESSEL]. Go deeper on its Singapore connections."
- "Is there a newer OFAC designation this week that affects any of these?"
- "Draft a 3-sentence briefing note for DSTA audience."
- "Is PIONEER 92 currently in Hormuz?"

Follow the redirect. Do not continue down the list until the analyst confirms or redirects.

## Step 5 — Record findings (on analyst approval)

When the analyst says "approved" or "record this":

1. Format a summary as a GitHub Issue comment:
   - Date and watchlist snapshot timestamp
   - Top findings with MMSI, name, flag, confidence score
   - Key OSINT sources referenced
   - Analyst conclusion (1–2 sentences)

2. Post to the relevant GitHub issue (arktrace#549 for live demo screenshots, or create a new dated issue).

---

## Notes for the LLM

- This is a **local session** — no cloud dependency. Run on llama.cpp / Ollama locally.
- Always cross-reference by MMSI or IMO, not vessel name (names change frequently).
- `sanctions_distance=0` means the vessel's operator network has direct ownership overlap with a sanctioned entity — not necessarily that the vessel itself is designated.
- Stateless MMSI (ITU-unallocated MID) is a high-confidence evasion signal. No legitimate vessel can hold a 400-prefix MMSI.
- The analyst has final say on all findings. Do not post to GitHub without explicit approval.
- If MarineTraffic returns a 403 or blocks scraping, note it and try VesselFinder instead.

---

## Quick start

```
/osint-check
```

Or with a specific watchlist:

```
uv run python scripts/osint_watchlist_check.py --watchlist data/processed/singapore_watchlist.parquet --output /tmp/sg_watchlist.json
```

Then start the session with: "Read /tmp/sg_watchlist.json and begin the OSINT investigation."
