"""Deterministic data preparation for OSINT watchlist investigation.

Pulls the top-N vessels from the candidate watchlist, detects stateless
(ITU-unallocated) MMSIs, flags sanctions_distance=0 vessels, and generates
pre-built OSINT lookup URLs for each candidate.

Output is a structured JSON file consumed by the analyst's LLM session
(Case A) or an automated workflow (Case B).

Usage:
    uv run python scripts/osint_watchlist_check.py
    uv run python scripts/osint_watchlist_check.py --top 100
    uv run python scripts/osint_watchlist_check.py --output /tmp/watchlist.json
    uv run python scripts/osint_watchlist_check.py --watchlist data/processed/singapore_watchlist.parquet
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]

# ITU-allocated MIDs (first 3 digits of MMSI) — derived from composite.py
# An MMSI whose MID prefix is absent from this set is unallocated / stateless.
_ALLOCATED_MIDS: frozenset[str] = frozenset(
    [
        # North Asia
        "412", "413", "414", "431", "432", "433", "440", "441", "445", "477",
        # South-East Asia
        "525", "533", "563", "564", "565", "566", "567", "574", "576",
        # South Asia / Middle East
        "403", "416", "419", "422", "447", "466", "470", "471",
        # Russia / CIS
        "272", "273",
        # Europe
        "205", "209", "210", "212", "219", "232", "233", "234", "235",
        "237", "239", "240", "241", "248", "249", "253", "256", "257",
        "258", "259", "271",
        # Americas
        "303", "305", "338", "366", "367", "368", "369", "710", "725",
        "730", "734",
        # Flag-of-convenience / open registries
        "308", "309", "310", "311", "312", "351", "352", "353", "354",
        "355", "356", "357", "370", "371", "372", "373", "374", "518",
        "538", "548", "636", "667",
        # Africa
        "613",
    ]
)


def _default_watchlist_path() -> Path:
    candidates = [
        Path.home() / ".maridb" / "data" / "candidate_watchlist.parquet",
        REPO_ROOT / "data" / "processed" / "candidate_watchlist.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def _is_stateless(mmsi: str) -> bool:
    s = str(mmsi).strip()
    if len(s) < 3:
        return True
    return s[:3] not in _ALLOCATED_MIDS


def _mt_mmsi_url(mmsi: str) -> str:
    return f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}"


def _mt_imo_url(imo: str) -> str:
    return f"https://www.marinetraffic.com/en/ais/details/ships/imo:{imo}"


def _vf_url(mmsi: str) -> str:
    return f"https://www.vesselfinder.com/?mmsi={mmsi}"


def _ofac_url(name: str, mmsi: str) -> str:
    query = name.strip() if name and name.strip() and name != mmsi else f"MMSI {mmsi}"
    import urllib.parse
    return f"https://sanctionssearch.ofac.treas.gov/?searchText={urllib.parse.quote(query)}"


def _parse_signals(raw: object) -> list:
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return []
    return []


def _vessel_record(row: dict) -> dict:
    mmsi = str(row.get("mmsi", ""))
    imo = str(row.get("imo", "")) if row.get("imo") else ""
    name = str(row.get("vessel_name", "")) if row.get("vessel_name") else ""

    links: dict[str, str] = {"marinetraffic_mmsi": _mt_mmsi_url(mmsi)}
    if imo and imo not in ("", "0", "None"):
        links["marinetraffic_imo"] = _mt_imo_url(imo)
    links["vesselfinder"] = _vf_url(mmsi)
    links["ofac_search"] = _ofac_url(name, mmsi)

    return {
        "rank": row.get("_rank"),
        "mmsi": mmsi,
        "imo": imo,
        "vessel_name": name or mmsi,
        "flag": str(row.get("flag", "")) or "",
        "vessel_type": str(row.get("vessel_type", "")) or "",
        "confidence": round(float(row.get("confidence", 0)), 4),
        "sanctions_distance": int(row["sanctions_distance"] if row.get("sanctions_distance") is not None else 99),
        "stateless_mmsi": _is_stateless(mmsi),
        "mmsi_mid": str(mmsi)[:3] if len(str(mmsi)) >= 3 else "",
        "ais_gap_count_30d": int(row.get("ais_gap_count_30d", 0) or 0),
        "ais_gap_max_hours": float(row.get("ais_gap_max_hours", 0) or 0),
        "top_signals": _parse_signals(row.get("top_signals")),
        "last_seen": str(row.get("last_seen", "")) if row.get("last_seen") else "",
        "links": links,
    }


def run(
    watchlist_path: Path,
    top_n: int,
    output_path: Path,
) -> dict:
    if not watchlist_path.exists():
        print(
            f"Error: watchlist not found at {watchlist_path}\n"
            "Run: uv run python scripts/sync_r2.py pull-watchlists",
            file=sys.stderr,
        )
        raise SystemExit(1)

    df = pl.read_parquet(watchlist_path)
    ranked = df.sort("confidence", descending=True).head(top_n)

    rows = ranked.to_dicts()
    vessels = []
    for i, row in enumerate(rows, start=1):
        row["_rank"] = i
        vessels.append(_vessel_record(row))

    sanctioned = [v for v in vessels if v["sanctions_distance"] == 0]
    stateless = [v for v in vessels if v["stateless_mmsi"]]
    high_gap = [v for v in vessels if v["ais_gap_count_30d"] >= 3]

    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "watchlist_path": str(watchlist_path),
        "total_vessels_in_watchlist": df.height,
        "top_n": top_n,
        "summary": {
            "sanctions_distance_0_count": len(sanctioned),
            "stateless_mmsi_count": len(stateless),
            "high_ais_gap_count": len(high_gap),
        },
        "vessels": vessels,
        "sanctions_distance_0": sanctioned,
        "stateless_mmsis": stateless,
        "high_ais_gap": high_gap,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, default=str))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--watchlist",
        type=Path,
        default=_default_watchlist_path(),
        help="Path to candidate_watchlist.parquet",
    )
    parser.add_argument("--top", type=int, default=50, help="Number of top vessels (default: 50)")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/watchlist.json"),
        help="Output JSON path (default: /tmp/watchlist.json)",
    )
    args = parser.parse_args()

    result = run(args.watchlist, args.top, args.output)

    s = result["summary"]
    print(f"Output: {args.output}")
    print(f"Top {args.top} of {result['total_vessels_in_watchlist']:,} vessels")
    print(f"  sanctions_distance=0 : {s['sanctions_distance_0_count']}")
    print(f"  stateless MMSIs      : {s['stateless_mmsi_count']}")
    print(f"  high AIS gap (≥3/30d): {s['high_ais_gap_count']}")

    if result["sanctions_distance_0"]:
        print("\nTop sanctions_distance=0 vessels:")
        for v in result["sanctions_distance_0"][:5]:
            print(f"  [{v['rank']:2}] {v['mmsi']} {v['vessel_name']:<20} conf={v['confidence']:.3f}")

    if result["stateless_mmsis"]:
        print("\nStateless MMSIs (ITU-unallocated MID):")
        for v in result["stateless_mmsis"][:5]:
            print(f"  [{v['rank']:2}] {v['mmsi']} MID={v['mmsi_mid']} conf={v['confidence']:.3f}")


if __name__ == "__main__":
    main()
