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
- Derives flag from MMSI MID prefix when registry data is absent
- Generates MarineTraffic / VesselFinder / OFAC search URLs per vessel
- Writes a structured Markdown report and sends an HTML email

Usage
-----
    uv run python scripts/generate_osint_report.py
    uv run python scripts/generate_osint_report.py --top 50 --out report.md
    uv run python scripts/generate_osint_report.py --email

Environment variables
---------------------
MARIDB_DATA_DIR   Directory containing candidate_watchlist.parquet
                  (default: ~/.maridb/data or data/processed if that exists)
NOTIFY_EMAIL      Recipient address (required for --email)
SMTP_HOST         SMTP server hostname  (default: smtp.gmail.com)
SMTP_PORT         SMTP server port      (default: 587)
SMTP_USER         Sender address / login
SMTP_PASSWORD     SMTP password / app-password
GITHUB_RUN_ID     Injected automatically by GitHub Actions
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

# MID prefix → ISO 3166-1 alpha-2 flag code. Used to fill in flag when
# vessel_meta has no registry entry (vessel_name falls back to MMSI).
_MID_TO_FLAG: dict[str, str] = {
    "201": "AL", "202": "AD", "203": "AT", "204": "PT", "205": "BE",
    "206": "BY", "207": "BG", "208": "PT", "209": "CY", "210": "CY", "211": "DE",
    "212": "CY", "213": "GE", "214": "MD", "215": "MT", "216": "NL",
    "218": "DE", "219": "DK", "220": "DK", "224": "ES", "225": "ES",
    "226": "FR", "227": "FR", "228": "FR", "229": "MT", "230": "FI",
    "231": "FO", "232": "GB", "233": "GB", "234": "GB", "235": "GB",
    "236": "GI", "237": "GR", "238": "HR", "239": "GR", "240": "GR",
    "241": "GR", "242": "MA", "243": "HU", "244": "NL", "245": "NL",
    "246": "NL", "247": "IT", "248": "MT", "249": "MT", "250": "IE",
    "251": "IS", "252": "LI", "253": "LU", "254": "MC", "255": "PT",
    "256": "MT", "257": "NO", "258": "NO", "259": "NO", "261": "PL",
    "262": "ME", "263": "PT", "264": "RO", "265": "SE", "266": "SE",
    "267": "SK", "268": "SM", "269": "CH", "270": "CZ", "271": "TR",
    "272": "UA", "273": "RU", "274": "MK", "275": "LV", "276": "EE",
    "277": "LT", "278": "SI", "279": "RS", "281": "BA",
    "301": "AI", "303": "US", "304": "AG", "305": "AG", "306": "AN",
    "307": "AW", "308": "BS", "309": "BS", "310": "BM", "311": "BS",
    "312": "BZ", "316": "CA", "319": "KY", "321": "CU", "323": "DO",
    "325": "SV", "327": "GT", "329": "GP", "330": "GD", "331": "GL",
    "332": "HN", "333": "JM", "334": "MQ", "336": "MX", "338": "US",
    "339": "MX", "341": "NI", "343": "PA", "345": "PR", "347": "KN",
    "348": "VI", "350": "PA", "351": "PA", "352": "PA", "353": "PA",
    "354": "PA", "355": "PA", "356": "PA", "357": "PA", "358": "PR",
    "359": "SH", "361": "PM", "362": "TT", "364": "TC", "366": "US",
    "367": "US", "368": "US", "369": "US", "370": "PA", "371": "PA",
    "372": "PA", "373": "PA", "374": "PA", "375": "VC", "376": "VC",
    "377": "VC", "378": "VG", "379": "VI",
    "401": "AF", "403": "SA", "405": "BD", "408": "BH", "410": "BT",
    "412": "CN", "413": "CN", "414": "CN", "416": "TW", "417": "LK",
    "419": "IN", "422": "IR", "423": "AZ", "425": "IQ", "428": "IL",
    "431": "JP", "432": "JP", "433": "JP", "434": "TM", "436": "KZ",
    "437": "UZ", "438": "JO", "440": "KR", "441": "KR", "443": "PS",
    "445": "KP", "447": "KW", "450": "LB", "451": "KG", "453": "MO",
    "455": "MV", "457": "MN", "459": "NP", "461": "OM", "463": "PK",
    "466": "QA", "468": "SY", "470": "AE", "471": "AE", "472": "TJ",
    "473": "YE", "477": "HK",
    "501": "AQ", "503": "AU", "506": "MM", "508": "BN", "510": "FM",
    "511": "PW", "512": "NZ", "514": "KH", "515": "KH", "516": "CX",
    "518": "CK", "520": "FJ", "523": "CC", "525": "ID", "529": "KI",
    "531": "LA", "533": "MY", "536": "MP", "538": "MH", "540": "NC",
    "542": "NU", "544": "NR", "546": "PF", "548": "PH", "553": "PG",
    "555": "PN", "557": "SB", "559": "WS", "561": "WS", "563": "SG",
    "564": "SG", "565": "SG", "566": "SG", "567": "SG", "570": "TH",
    "572": "TO", "574": "VN", "576": "VN", "577": "VU", "578": "WF",
    "601": "ZA", "603": "AO", "605": "DZ", "607": "TF", "608": "AC",
    "609": "BI", "611": "BJ", "613": "CM", "616": "CD", "618": "CG",
    "619": "KM", "621": "DJ", "622": "EG", "624": "ET", "625": "ER",
    "626": "GA", "627": "GH", "629": "GM", "630": "GW", "631": "GQ",
    "632": "GN", "633": "BF", "634": "KE", "635": "TF", "636": "LR",
    "637": "LR", "638": "SS", "642": "LY", "644": "LS", "645": "MU",
    "647": "MG", "649": "ML", "650": "MZ", "654": "MR", "655": "MW",
    "656": "NE", "657": "NG", "659": "NA", "660": "RE", "661": "RW",
    "662": "SD", "663": "SN", "664": "SC", "665": "SH", "666": "SO",
    "667": "SL", "668": "ST", "669": "SZ", "670": "TD", "671": "TG",
    "672": "TN", "674": "TZ", "675": "UG", "676": "CD", "677": "TZ",
    "678": "ZM", "679": "ZW",
    "701": "AR", "710": "BR", "720": "BO", "725": "CL", "730": "CO",
    "735": "EC", "740": "FK", "745": "GF", "750": "GY", "755": "PY",
    "760": "PE", "765": "SR", "770": "UY", "775": "VE",
}


def _is_stateless(mmsi: str) -> bool:
    """Return True if the MMSI's MID prefix is not in the ITU allocation table."""
    if not mmsi or len(mmsi) != 9 or not mmsi.isdigit():
        return False
    return mmsi[:3] not in _ALLOCATED_MIDS


def _flag_from_mid(mmsi: str) -> str:
    """Derive ISO flag code from the MMSI MID prefix, or '' if unknown."""
    if not mmsi or len(mmsi) < 3:
        return ""
    return _MID_TO_FLAG.get(mmsi[:3], "")


def _has_real_name(vessel_name: str, mmsi: str) -> bool:
    """Return False when vessel_name is just the MMSI fallback (no registry entry)."""
    return bool(vessel_name) and vessel_name != mmsi and not vessel_name.isdigit()


def _mt_url(mmsi: str) -> str:
    return f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}"


def _vf_url(mmsi: str) -> str:
    return f"https://www.vesselfinder.com/vessels/details/{mmsi}"


def _ofac_url(name: str) -> str:
    q = name.replace(" ", "+")
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


def _enrich_rows(df: pl.DataFrame, top_n: int) -> list[dict]:
    """Sort, slice, and add computed display fields to each row dict."""
    top = df.sort("confidence", descending=True).head(top_n)
    rows = top.to_dicts()
    for i, row in enumerate(rows):
        mmsi = str(row.get("mmsi", "") or "")
        raw_name = str(row.get("vessel_name", "") or "")
        raw_flag = str(row.get("flag", "") or "")

        real_name = _has_real_name(raw_name, mmsi)
        # Fall back to MID-derived flag when registry has no entry
        flag = raw_flag if raw_flag else _flag_from_mid(mmsi)

        row["_rank"] = i + 1
        row["_mmsi"] = mmsi
        row["_name"] = raw_name if real_name else "—"
        row["_flag"] = flag or "—"
        row["_real_name"] = real_name
        row["_stateless"] = _is_stateless(mmsi)
        row["_mid"] = mmsi[:3] if mmsi else "—"
        row["_mt_url"] = _mt_url(mmsi) if mmsi else ""
        row["_vf_url"] = _vf_url(mmsi) if mmsi else ""
        # OFAC link only when we have a real vessel name to search
        row["_ofac_url"] = _ofac_url(raw_name) if real_name else ""
    return rows


def generate_report(df: pl.DataFrame, top_n: int) -> str:
    """Return a Markdown OSINT report for the top-N watchlist candidates."""
    today = date.today().isoformat()
    rows = _enrich_rows(df, top_n)

    sanctioned = [r for r in rows if r.get("sanctions_distance") == 0]
    stateless = [r for r in rows if r["_stateless"]]

    def _md_links(r: dict) -> str:
        parts = [f"[MT]({r['_mt_url']})", f"[VF]({r['_vf_url']})"]
        if r["_ofac_url"]:
            parts.append(f"[OFAC]({r['_ofac_url']})")
        return " ".join(parts)

    lines: list[str] = [
        f"## sanctions_distance=0 ({len(sanctioned)} vessels)",
        "",
        "| Rank | MMSI | IMO | Name | Flag | Conf | Links |",
        "|------|------|-----|------|------|------|-------|",
    ]
    for r in sanctioned:
        lines.append(
            f"| {r['_rank']} | {r['_mmsi']} | {r.get('imo') or '—'} "
            f"| {r['_name']} | {r['_flag']} | {r.get('confidence', 0):.3f} "
            f"| {_md_links(r)} |"
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
        lines.append(
            f"| {r['_rank']} | {r['_mmsi']} | {r['_name']} "
            f"| {r.get('confidence', 0):.3f} | {r['_mid']} | ⚠ ITU unallocated |"
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
        sd = r.get("sanctions_distance")
        sd_str = str(int(sd)) if sd is not None and str(sd) != "nan" else "—"
        marker = " ⚠" if r["_stateless"] else ""
        links = f"[MT]({r['_mt_url']}) [VF]({r['_vf_url']})"
        lines.append(
            f"| {r['_rank']} | {r['_mmsi']}{marker} | {r.get('imo') or '—'} "
            f"| {r['_name']} | {r['_flag']} | {r.get('confidence', 0):.3f} "
            f"| {sd_str} | {links} |"
        )

    header = [
        f"# [OSINT Check] Watchlist — {today}",
        "",
        f"Top-{top_n} candidates from `candidate_watchlist.parquet`.",
        "Analysts: click links to verify on MarineTraffic / VesselFinder / OFAC.",
        "Stateless MMSIs (⚠) have unallocated ITU MID prefixes — likely spoofed identity.",
        "",
    ]
    return "\n".join(header + lines) + "\n"


# ---------------------------------------------------------------------------
# HTML email rendering
# ---------------------------------------------------------------------------

_CSS = """
body { font-family: -apple-system, Arial, sans-serif; font-size: 14px; color: #1a1a1a; margin: 0; padding: 20px; background: #f5f5f5; }
.card { background: #fff; border-radius: 6px; padding: 20px 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
h1 { font-size: 18px; margin: 0 0 4px; }
h2 { font-size: 15px; margin: 0 0 12px; border-bottom: 1px solid #e5e5e5; padding-bottom: 6px; }
.meta { color: #666; font-size: 12px; margin-bottom: 20px; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th { background: #f0f0f0; text-align: left; padding: 7px 10px; white-space: nowrap; }
td { padding: 6px 10px; border-bottom: 1px solid #eee; vertical-align: middle; }
tr:hover td { background: #fafafa; }
a { color: #0066cc; text-decoration: none; }
a:hover { text-decoration: underline; }
.badge-sanctioned { background: #fde8e8; color: #b91c1c; border-radius: 3px; padding: 1px 5px; font-size: 11px; font-weight: 600; }
.badge-stateless { background: #fff3cd; color: #92400e; border-radius: 3px; padding: 1px 5px; font-size: 11px; font-weight: 600; }
.conf { font-weight: 600; }
.none { color: #aaa; }
.ci-link { color: #666; font-size: 12px; }
"""


def _e(s: str) -> str:
    """Minimal HTML escaping."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _link(url: str, label: str) -> str:
    return f'<a href="{_e(url)}">{_e(label)}</a>'


def _html_links(r: dict) -> str:
    parts = [_link(r["_mt_url"], "MT"), _link(r["_vf_url"], "VF")]
    if r["_ofac_url"]:
        parts.append(_link(r["_ofac_url"], "OFAC"))
    return " · ".join(parts)


def generate_html_email(df: pl.DataFrame, top_n: int, run_url: str) -> str:
    """Return a fully styled HTML email body."""
    today = date.today().isoformat()
    rows = _enrich_rows(df, top_n)
    sanctioned = [r for r in rows if r.get("sanctions_distance") == 0]
    stateless = [r for r in rows if r["_stateless"]]

    def _td(val: str, cls: str = "") -> str:
        attr = f' class="{cls}"' if cls else ""
        return f"<td{attr}>{val}</td>"

    def _sanctioned_rows() -> str:
        if not sanctioned:
            return '<tr><td colspan="7" class="none">No directly sanctioned vessels in top-N.</td></tr>'
        out = []
        for r in sanctioned:
            conf = f"{r.get('confidence', 0):.3f}"
            out.append(
                "<tr>"
                + _td(str(r["_rank"]))
                + _td(_e(r["_mmsi"]))
                + _td(_e(r.get("imo") or "—"))
                + _td(_e(r["_name"]))
                + _td(_e(r["_flag"]))
                + _td(conf, "conf")
                + _td(_html_links(r))
                + "</tr>"
            )
        return "".join(out)

    def _stateless_rows() -> str:
        if not stateless:
            return '<tr><td colspan="5" class="none">No stateless MMSIs detected in top-N.</td></tr>'
        out = []
        for r in stateless:
            conf = f"{r.get('confidence', 0):.3f}"
            badge = f'<span class="badge-stateless">⚠ {_e(r["_mid"])}</span>'
            out.append(
                "<tr>"
                + _td(str(r["_rank"]))
                + _td(_e(r["_mmsi"]))
                + _td(_e(r["_name"]))
                + _td(conf, "conf")
                + _td(badge)
                + "</tr>"
            )
        return "".join(out)

    def _full_table_rows() -> str:
        out = []
        for r in rows:
            sd = r.get("sanctions_distance")
            sd_str = str(int(sd)) if sd is not None and str(sd) != "nan" else "—"
            conf = f"{r.get('confidence', 0):.3f}"
            mmsi_cell = _e(r["_mmsi"])
            if r["_stateless"]:
                mmsi_cell += ' <span class="badge-stateless">⚠</span>'
            if r.get("sanctions_distance") == 0:
                mmsi_cell += ' <span class="badge-sanctioned">SDN</span>'
            nav_links = _link(r["_mt_url"], "MT") + " · " + _link(r["_vf_url"], "VF")
            out.append(
                "<tr>"
                + _td(str(r["_rank"]))
                + f"<td>{mmsi_cell}</td>"
                + _td(_e(r.get("imo") or "—"))
                + _td(_e(r["_name"]))
                + _td(_e(r["_flag"]))
                + _td(conf, "conf")
                + _td(sd_str)
                + _td(nav_links)
                + "</tr>"
            )
        return "".join(out)

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>{_CSS}</style></head>
<body>
<div class="card">
  <h1>[OSINT Check] Watchlist — {_e(today)}</h1>
  <p class="meta">Top-{top_n} candidates · {_e(today)} · <a class="ci-link" href="{_e(run_url)}">View CI run →</a></p>

  <h2>sanctions_distance=0 — {len(sanctioned)} vessel{"s" if len(sanctioned) != 1 else ""}</h2>
  <table>
    <tr><th>#</th><th>MMSI</th><th>IMO</th><th>Name</th><th>Flag</th><th>Conf</th><th>Links</th></tr>
    {_sanctioned_rows()}
  </table>
</div>

<div class="card">
  <h2>Stateless MMSIs — ITU unallocated — {len(stateless)} vessel{"s" if len(stateless) != 1 else ""}</h2>
  <table>
    <tr><th>#</th><th>MMSI</th><th>Name</th><th>Conf</th><th>MID</th></tr>
    {_stateless_rows()}
  </table>
</div>

<div class="card">
  <h2>Full top-{top_n} watchlist</h2>
  <table>
    <tr><th>#</th><th>MMSI</th><th>IMO</th><th>Name</th><th>Flag</th><th>Conf</th><th>SD</th><th>Links</th></tr>
    {_full_table_rows()}
  </table>
</div>
</body></html>"""


def _send_email(subject: str, markdown_body: str, html_body: str) -> bool:
    """Send the report as multipart plain-text + HTML email. Returns True on success."""
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
        subject = f"[OSINT Check] Watchlist — {today}"
        run_id = os.getenv("GITHUB_RUN_ID", "")
        repo = os.getenv("GITHUB_REPOSITORY", "edgesentry/indago")
        run_url = (
            f"https://github.com/{repo}/actions/runs/{run_id}"
            if run_id
            else f"https://github.com/{repo}/actions"
        )
        html_body = generate_html_email(df, top_n=args.top, run_url=run_url)
        _send_email(subject, report, html_body)

    return 0


if __name__ == "__main__":
    sys.exit(main())
