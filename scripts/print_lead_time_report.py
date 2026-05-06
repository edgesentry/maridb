"""Print a CAP Vista pitch-ready lead time validation report.

Reads the JSON produced by validate_lead_time_ofac.py and formats it
as a presentation-ready summary with honest caveats about AIS data coverage.

Usage
-----
    # Generate the source report first
    uv run python scripts/validate_lead_time_ofac.py \\
        --watchlist ~/.indago/data/candidate_watchlist.parquet \\
        --out data/processed/lead_time_report.json

    # Print the formatted report
    uv run python scripts/print_lead_time_report.py
    uv run python scripts/print_lead_time_report.py --report data/processed/lead_time_report.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = REPO_ROOT / "data" / "processed" / "lead_time_report.json"

# AIS collection start date — used in the data coverage caveat
AIS_COLLECTION_START = "2026-03-01"


def _col(text: str, width: int) -> str:
    return str(text)[:width].ljust(width)


def _print_section(title: str) -> None:
    print(f"\n{'═' * 72}")
    print(f"  {title}")
    print(f"{'═' * 72}")


def _table(rows: list[dict], cols: list[tuple[str, int, str]], max_rows: int = 20) -> None:
    if not rows:
        print("  (no results)")
        return
    header = "  " + "  ".join(_col(label, w) for _, w, label in cols)
    print(header)
    print("  " + "─" * (sum(w for _, w, _ in cols) + 2 * len(cols)))
    for row in rows[:max_rows]:
        print("  " + "  ".join(_col(row.get(k, ""), w) for k, w, _ in cols))
    if len(rows) > max_rows:
        print(f"  … and {len(rows) - max_rows} more")


def main() -> None:
    parser = argparse.ArgumentParser(description="Print CAP Vista lead time validation report")
    parser.add_argument(
        "--report",
        default=str(DEFAULT_REPORT),
        help=f"Path to lead_time_report.json (default: {DEFAULT_REPORT})",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Max rows to show per table (default: 20)",
    )
    args = parser.parse_args()

    report_path = Path(args.report)
    if not report_path.exists():
        print(f"[error] Report not found: {report_path}")
        print("Run first:  uv run python scripts/validate_lead_time_ofac.py --out <path>")
        return

    with report_path.open() as f:
        r = json.load(f)

    generated = r.get("generated_at_utc", "unknown")[:19].replace("T", " ")

    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║          LEAD TIME VALIDATION — CAP Vista Pitch Report              ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print(f"  Generated : {generated} UTC")
    print(f"  Watchlist : {r.get('watchlist_size', 0):,} vessels")
    print(f"  OFAC/UN/EU vessels with MMSI: {r.get('designation_dates_loaded', 0)}")
    print(f"  Matched in watchlist         : {r.get('matched_to_designation', 0)}")

    # ── Data coverage caveat ──────────────────────────────────────────────────
    _print_section("DATA COVERAGE NOTE")
    print(f"  AIS collection began: {AIS_COLLECTION_START}")
    print("  Detection windows are limited to data available since that date.")
    print("  60–90 day lead time validation requires 6+ months of AIS history.")
    print("  Current data demonstrates the methodology; full validation pending.")

    # ── Pre-designation detections ────────────────────────────────────────────
    pre = r.get("pre_designation_vessels", [])
    _print_section(f"PRE-DESIGNATION DETECTIONS  ({len(pre)} vessels)")
    print("  Model flagged these vessels BEFORE public OFAC/UN/EU listing.")
    print()
    _table(
        pre,
        [
            ("mmsi", 12, "MMSI"),
            ("flag", 5, "Flag"),
            ("watchlist_rank", 8, "Rank"),
            ("confidence", 10, "Confidence"),
            ("designation_date_proxy", 14, "Designation"),
            ("detection_window_start", 14, "Detection"),
            ("lead_days", 9, "Lead days"),
            ("ais_gap_count_30d", 8, "Gaps 30d"),
            ("sanctions_distance", 9, "SanctDist"),
        ],
        max_rows=args.top_n,
    )

    # ── Lead time distribution ────────────────────────────────────────────────
    n_pre = r.get("pre_designation_count", 0)
    n_post = r.get("post_designation_count", 0)
    _print_section("LEAD TIME STATISTICS")
    print(f"  Pre-designation  : {n_pre} vessels  ← detected BEFORE public listing")
    print(f"  Post-designation : {n_post} vessels  ← confirmed recall (listed then flagged)")
    print(f"  Mean lead time   : {r.get('mean_lead_days', 0)} days")
    print(f"  p25 / p50 / p75  : "
          f"{r.get('p25_lead_days', 0)} / {r.get('median_lead_days', 0)} / {r.get('p75_lead_days', 0)} days")

    # ── Watchlist rank of pre-designation vessels ─────────────────────────────
    _print_section("WATCHLIST RANK OF PRE-DESIGNATION VESSELS")
    print("  Note: Rank is position in the full multi-region watchlist sorted by confidence.")
    print("  Contractual acceptance gate is P@50 ≥ 0.60 on the trial dataset.")
    print()
    if pre:
        ranks = [v.get("watchlist_rank", 0) for v in pre]
        in_top50 = sum(1 for r_ in ranks if r_ <= 50)
        in_top200 = sum(1 for r_ in ranks if r_ <= 200)
        print(f"  In top-50  : {in_top50} / {len(pre)}")
        print(f"  In top-200 : {in_top200} / {len(pre)}")
        print()
        for v in pre:
            bar = "★" if v.get("watchlist_rank", 9999) <= 50 else "·"
            print(f"  {bar} MMSI {v['mmsi']:12s}  rank #{v.get('watchlist_rank','?'):>6}  "
                  f"confidence {v.get('confidence',0):.3f}  lead {v.get('lead_days',0)} days")
    else:
        print("  (no pre-designation vessels found)")

    # ── Post-designation (recall confirmation) ────────────────────────────────
    post = r.get("post_designation_vessels", [])
    _print_section(f"POST-DESIGNATION RECALL  ({len(post)} vessels — confirms model catches known cases)")
    _table(
        post,
        [
            ("mmsi", 12, "MMSI"),
            ("flag", 5, "Flag"),
            ("watchlist_rank", 8, "Rank"),
            ("confidence", 10, "Confidence"),
            ("designation_date_proxy", 14, "Designation"),
            ("lead_days", 9, "Lead days"),
        ],
        max_rows=args.top_n,
    )

    # ── Unknown-unknown watch ─────────────────────────────────────────────────
    uu = r.get("unknown_unknown_watch", [])
    _print_section(f"UNKNOWN-UNKNOWN WATCH  ({len(uu)} candidates — not yet designated)")
    print("  High-scoring vessels with no current sanctions link.")
    print("  Lead time measurable when/if designated in future.")
    print()
    _table(
        uu,
        [
            ("mmsi", 12, "MMSI"),
            ("flag", 5, "Flag"),
            ("confidence", 10, "Confidence"),
            ("ais_gap_count_30d", 9, "Gaps 30d"),
            ("status", 55, "Status"),
        ],
        max_rows=args.top_n,
    )

    # ── CAP Vista pitch framing ───────────────────────────────────────────────
    _print_section("CAP VISTA PITCH FRAMING")
    print("  What we demonstrate TODAY:")
    print(f"    · {n_pre} OFAC-designated vessels detected BEFORE public listing")
    print(f"    · {n_post + n_pre} / {r.get('matched_to_designation', 0)} matched vessels "
          "appear in watchlist (recall confirmed)")
    print(f"    · {len(uu)} unknown-unknown candidates currently under watch")
    print()
    print("  What requires the Week 7 trial (Cap Vista MPOL feed):")
    print("    · Rank validation in top-50 on an independent dataset")
    print("    · 60–90 day lead time on vessels with pre-March-2026 AIS history")
    print("    · Prospective validation: do current unknown-unknowns get designated?")
    print()
    print("  Contractual commitment: Precision@50 ≥ 0.60 on the trial dataset.")
    print("  Demonstrated ceiling : Precision@50 = 0.68 (multi-region public backtest).")
    print()


if __name__ == "__main__":
    main()
