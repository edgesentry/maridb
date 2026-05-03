"""Generate a structured OSINT watchlist report from candidate_watchlist.parquet.

Replaces the removed osint_watchlist_check.py with a pure data-transformation
step that has no LLM dependency. Intended to be consumed by a GitHub Actions
weekly cron (Case B in docs/osint-workflow-design.md).

What this script does
---------------------
- Reads candidate_watchlist.parquet (local or from a provided path)
- Selects top-N vessels by confidence score
- Flags vessels where sanctions_distance == 0 (directly sanctioned)
- Detects ITU-unallocated MMSIs (stateless vessels)
- Generates MarineTraffic / VesselFinder / OFAC search URLs per vessel
- Writes a structured Markdown report and optionally emails it

Usage
-----
    uv run python scripts/generate_osint_report.py
    uv run python scripts/generate_osint_report.py --top 50 --out report.md
    uv run python scripts/generate_osint_report.py --email

Environment variables
---------------------
MARIDB_DATA_DIR  Directory containing candidate_watchlist.parquet
                 (default: ~/.maridb/data or data/processed if that exists)
NOTIFY_EMAIL     Recipient address (required for --email)
SMTP_HOST        SMTP server hostname  (default: smtp.gmail.com)
SMTP_PORT        SMTP server port      (default: 587)
SMTP_USER        Sender address / login
SMTP_PASSWORD    SMTP password / app-password
GITHUB_RUN_ID    Injected automatically by GitHub Actions
GITHUB_REPOSITORY Injected automatically by GitHub Actions
"""

from __future__ import annotations

import argparse
import os
import smtplib
import sys
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import polars as pl

# ---------------------------------------------------------------------------
# ITU Maritime Identification Digits (MID) — complete allocation table.
# Any 9-digit MMSI whose first 3 digits are NOT in this set is considered
# unallocated / stateless.  Source: ITU List of Ship Stations and Maritime
# Mobile Service Identity Assignments (Table of MIDs).
# ---------------------------------------------------------------------------
_ALLOCATED_MIDS: frozenset[str] = frozenset(
    {
        # Europe (200–299)
        "201", "202", "203", "204", "205", "206", "207", "208",
        "209", "210", "211", "212", "213", "214", "215", "216",
        "218", "219", "220", "224", "225", "226", "227", "228",
        "229", "230", "231", "232", "233", "234", "235", "236",
        "237", "238", "239", "240", "241", "242", "243", "244",
        "245", "246", "247", "248", "249", "250", "251", "252",
        "253", "254", "255", "256", "257", "258", "259", "261",
        "262", "263", "264", "265", "266", "267", "268", "269",
        "270", "271", "272", "273", "274", "275", "276", "277",
        "278", "279", "281",
        # Americas (300–399)
        "301", "303", "304", "305", "306", "307", "308", "309",
        "310", "311", "312", "316", "319", "321", "323", "325",
        "327", "329", "330", "331", "332", "333", "334", "336",
        "338", "339", "341", "343", "345", "347", "348", "350",
        "351", "352", "353", "354", "355", "356", "357", "358",
        "359", "361", "362", "364", "366", "367", "368", "369",
        "370", "371", "372", "373", "374", "375", "376", "377",
        "378", "379",
        # Asia / Middle East (400–499)
        "401", "403", "405", "408", "410", "412", "413", "414",
        "416", "417", "419", "422", "423", "425", "428", "431",
        "432", "433", "434", "436", "437", "438", "440", "441",
        "443", "445", "447", "450", "451", "453", "455", "457",
        "459", "461", "463", "466", "468", "470", "471", "472",
        "473", "477",
        # Asia-Pacific / Oceania (500–599)
        "501", "503", "506", "508", "510", "511", "512", "514",
        "515", "516", "518", "520", "523", "525", "529", "531",
        "533", "536", "538", "540", "542", "544", "546", "548",
        "553", "555", "557", "559", "561", "563", "564", "565",
        "566", "567", "570", "572", "574", "576", "577", "578",
        # Africa (600–699)
        "601", "603", "605", "607", "608", "609", "611", "613",
        "616", "618", "619", "621", "622", "624", "625", "626",
        "627", "629", "630", "631", "632", "633", "634", "635",
        "636", "637", "638", "642", "644", "645", "647", "649",
        "650", "654", "655", "656", "657", "659", "660", "661",
        "662", "663", "664", "665", "666", "667", "668", "669",
        "670", "671", "672", "674", "675", "676", "677", "678",
        "679",
        # Americas South (700–799)
        "701", "710", "720", "725", "730", "735", "740", "745",
        "750", "755", "760", "765", "770", "775",
    }
)


def _is_stateless(mmsi: str) -> bool:
    """Return True if the MMSI's MID prefix is not in the ITU allocation table."""
    if not mmsi or len(mmsi) != 9 or not mmsi.isdigit():
        return False
    return mmsi[:3] not in _ALLOCATED_MIDS


def _mt_url(mmsi: str) -> str:
    return f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}"


def _vf_url(mmsi: str) -> str:
    return f"https://www.vesselfinder.com/vessels/details/{mmsi}"


def _ofac_url(name: str) -> str:
    q = name.replace(" ", "+") if name and name != "Unknown" else "unknown"
    return f"https://sanctionssearch.ofac.treas.gov/?q={q}"


def _resolve_watchlist_path(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path).resolve()
    data_dir_env = os.getenv("MARIDB_DATA_DIR")
    if data_dir_env:
        p = Path(data_dir_env) / "candidate_watchlist.parquet"
        if p.exists():
            return p
    local = Path("data/processed/candidate_watchlist.parquet")
    if local.exists():
        return local.resolve()
    return Path.home() / ".maridb" / "data" / "candidate_watchlist.parquet"


def generate_report(df: pl.DataFrame, top_n: int) -> str:
    """Return a Markdown OSINT report string for the top-N watchlist candidates."""
    today = date.today().isoformat()
    top = df.sort("confidence", descending=True).head(top_n)

    rows = top.to_dicts()
    for i, row in enumerate(rows):
        row["_rank"] = i + 1
        mmsi = str(row.get("mmsi", "") or "")
        name = str(row.get("vessel_name", "") or "Unknown")
        row["_stateless"] = _is_stateless(mmsi)
        row["_mid"] = mmsi[:3] if mmsi else "—"
        row["_mt"] = f"[MT]({_mt_url(mmsi)})" if mmsi else "—"
        row["_vf"] = f"[VF]({_vf_url(mmsi)})" if mmsi else "—"
        row["_ofac"] = f"[OFAC]({_ofac_url(name)})"

    sanctioned = [r for r in rows if r.get("sanctions_distance") == 0]
    stateless = [r for r in rows if r["_stateless"]]

    lines: list[str] = [
        f"## sanctions_distance=0 ({len(sanctioned)} vessels)",
        "",
        "| Rank | MMSI | IMO | Name | Flag | Conf | Links |",
        "|------|------|-----|------|------|------|-------|",
    ]
    for r in sanctioned:
        conf = f"{r.get('confidence', 0):.3f}"
        lines.append(
            f"| {r['_rank']} | {r.get('mmsi', '—')} | {r.get('imo', '—') or '—'} "
            f"| {r.get('vessel_name', 'Unknown') or 'Unknown'} "
            f"| {r.get('flag', '—') or '—'} | {conf} "
            f"| {r['_mt']} {r['_vf']} {r['_ofac']} |"
        )
    if not sanctioned:
        lines.append("_No directly sanctioned vessels in top-N._")

    lines += [
        "",
        f"## Stateless MMSIs — ITU unallocated ({len(stateless)} vessels)",
        "",
        "| Rank | MMSI | Name | Conf | MID | Note |",
        "|------|------|------|------|-----|------|",
    ]
    for r in stateless:
        conf = f"{r.get('confidence', 0):.3f}"
        lines.append(
            f"| {r['_rank']} | {r.get('mmsi', '—')} "
            f"| {r.get('vessel_name', 'Unknown') or 'Unknown'} "
            f"| {conf} | {r['_mid']} | ⚠ ITU unallocated |"
        )
    if not stateless:
        lines.append("_No stateless MMSIs detected in top-N._")

    lines += [
        "",
        f"## Top-{top_n} watchlist ({today})",
        "",
        "| Rank | MMSI | IMO | Name | Flag | Conf | SD | Links |",
        "|------|------|-----|------|------|------|-----|-------|",
    ]
    for r in rows:
        conf = f"{r.get('confidence', 0):.3f}"
        sd = r.get("sanctions_distance", "—")
        sd_str = str(int(sd)) if sd is not None and str(sd) != "nan" else "—"
        stateless_marker = " ⚠" if r["_stateless"] else ""
        lines.append(
            f"| {r['_rank']} | {r.get('mmsi', '—')}{stateless_marker} "
            f"| {r.get('imo', '—') or '—'} "
            f"| {r.get('vessel_name', 'Unknown') or 'Unknown'} "
            f"| {r.get('flag', '—') or '—'} | {conf} | {sd_str} "
            f"| {r['_mt']} {r['_vf']} |"
        )

    header = [
        f"# [OSINT Check] Watchlist — {today}",
        "",
        f"Top-{top_n} candidates from `candidate_watchlist.parquet`.",
        "Analysts: click links to verify on MarineTraffic / VesselFinder / OFAC.",
        f"Stateless MMSIs (⚠) have unallocated ITU MID prefixes — likely spoofed identity.",
        "",
    ]
    return "\n".join(header + lines) + "\n"


def _send_email(subject: str, markdown_body: str) -> bool:
    """Send the report as a plain-text email. Returns True on success."""
    recipient = os.getenv("NOTIFY_EMAIL")
    if not recipient:
        print("NOTIFY_EMAIL not set — skipping email.", file=sys.stderr)
        return False

    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")

    if not smtp_user or not smtp_password:
        print("SMTP_USER / SMTP_PASSWORD not set — skipping email.", file=sys.stderr)
        return False

    run_id = os.getenv("GITHUB_RUN_ID", "")
    repo = os.getenv("GITHUB_REPOSITORY", "edgesentry/indago")
    run_url = (
        f"https://github.com/{repo}/actions/runs/{run_id}"
        if run_id
        else f"https://github.com/{repo}/actions"
    )

    html_body = (
        "<html><body><pre style='font-family:monospace;font-size:13px'>"
        + markdown_body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        + "</pre>"
        + f"<p><a href='{run_url}'>View CI run →</a></p>"
        + "</body></html>"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg.attach(MIMEText(markdown_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    print(f"Sending email to {recipient} via {smtp_host}:{smtp_port} …")
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient, msg.as_string())
    print(f"Email sent: {subject}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate OSINT watchlist report")
    parser.add_argument("--watchlist", help="Path to candidate_watchlist.parquet")
    parser.add_argument("--top", type=int, default=50, help="Top-N vessels (default: 50)")
    parser.add_argument("--out", help="Output file path (default: stdout)")
    parser.add_argument("--email", action="store_true", help="Send report by email (reads SMTP env vars)")
    args = parser.parse_args()

    watchlist_path = _resolve_watchlist_path(args.watchlist)
    if not watchlist_path.exists():
        print(f"[error] Watchlist not found: {watchlist_path}", file=sys.stderr)
        print(
            "  Pull from R2 first:  uv run python scripts/sync_r2.py pull-watchlists",
            file=sys.stderr,
        )
        return 1

    df = pl.read_parquet(watchlist_path)
    if df.height == 0:
        print("[warn] Watchlist is empty — no candidates to report.", file=sys.stderr)
        return 0

    required = {"mmsi", "confidence", "sanctions_distance"}
    missing = required - set(df.columns)
    if missing:
        print(f"[error] Watchlist missing columns: {missing}", file=sys.stderr)
        return 1

    report = generate_report(df, top_n=args.top)

    if args.out:
        Path(args.out).write_text(report)
        print(f"Report written to {args.out}")
    else:
        sys.stdout.write(report)

    if args.email:
        today = date.today().isoformat()
        _send_email(f"[OSINT Check] Watchlist — {today}", report)

    return 0


if __name__ == "__main__":
    sys.exit(main())
