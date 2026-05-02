"""Send email summary after daily AIS upload validation.

Reads data/processed/ais_validation_report.json and sends a formatted HTML email.

Environment variables
---------------------
NOTIFY_EMAIL, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD
VALIDATE_DATE      Target date (injected by GHA)
GITHUB_RUN_ID, GITHUB_REPOSITORY
"""

from __future__ import annotations

import json
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

_REPORT_PATH = Path("data/processed/ais_validation_report.json")


def _row_color(passed: bool) -> str:
    return "#d4edda" if passed else "#f8d7da"


def _check_cell(check: dict) -> str:
    if check.get("pass"):
        return "✅"
    details = " | ".join(
        f"{k}={v}" for k, v in check.items() if k not in ("pass",) and v not in ([], {}, None, "")
    )
    return f"❌ {details}"


def main() -> int:
    recipient = os.getenv("NOTIFY_EMAIL")
    if not recipient:
        print("NOTIFY_EMAIL not set — skipping.")
        return 0

    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    if not smtp_user or not smtp_password:
        print("SMTP credentials not set — skipping.", file=sys.stderr)
        return 0

    if not _REPORT_PATH.exists():
        print(f"Report not found at {_REPORT_PATH} — skipping.", file=sys.stderr)
        return 1

    report = json.loads(_REPORT_PATH.read_text())
    target_date = report.get("most_recent_date", report.get("target_date", "unknown"))
    overall_pass = report.get("overall_pass", False)

    # active_regions_passing, coverage_check, and region_results live inside
    # each day_result, not at the top level — use the most recent day
    day_results = report.get("day_results", [])
    most_recent_day = day_results[-1] if day_results else {}
    coverage = most_recent_day.get("coverage_check", {})
    active_passing = most_recent_day.get("active_regions_passing", [])

    run_id = os.getenv("GITHUB_RUN_ID", "")
    repo = os.getenv("GITHUB_REPOSITORY", "edgesentry/maridb")
    run_url = (
        f"https://github.com/{repo}/actions/runs/{run_id}"
        if run_id else f"https://github.com/{repo}/actions"
    )

    status_icon = "✅" if overall_pass else "❌"
    subject = f"{status_icon} maridb AIS validation — {target_date} ({'PASS' if overall_pass else 'FAIL'})"

    region_rows = ""
    for r in most_recent_day.get("region_results", []):
        color = _row_color(r.get("pass", False))
        checks = r.get("checks", {})
        err = r.get("error", "")
        region_rows += f"""
        <tr style="background:{color}">
          <td>{r['region']}</td>
          <td>{r.get('row_count', 0):,}</td>
          <td>{_check_cell(checks.get('schema', {}))}</td>
          <td>{_check_cell(checks.get('mmsi_null_rate', {}))}</td>
          <td>{_check_cell(checks.get('coordinates', {}))}</td>
          <td>{_check_cell(checks.get('timestamp_recency', {}))}</td>
          <td>{_check_cell(checks.get('duplicate_rate', {}))}</td>
          <td>{'❌ ' + err if err else ('✅' if r.get('pass') else '❌')}</td>
        </tr>"""

    html = f"""<html><body style="font-family:sans-serif;max-width:760px">
<h2>maridb — Daily AIS Upload Validation</h2>
<p><strong>Date:</strong> {target_date}<br>
<strong>Overall:</strong> {status_icon} {'PASS' if overall_pass else 'FAIL'}<br>
<strong>Active regions passing:</strong> {len(active_passing)}/{coverage.get('required', 5)}
({', '.join(active_passing) or 'none'})</p>

<h3>Per-Region Results</h3>
<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;font-size:13px">
  <tr style="background:#343a40;color:white">
    <th>Region</th><th>Rows</th><th>Schema</th>
    <th>MMSI nulls</th><th>Coords</th><th>Recency</th><th>Duplicates</th><th>Status</th>
  </tr>
  {region_rows}
</table>

<p style="margin-top:16px">
  <a href="s3://maridb-public/validation/ais/{target_date}.json">📄 Full report in R2</a> &nbsp;|&nbsp;
  <a href="{run_url}">View CI run →</a>
</p>
</body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html"))

    print(f"Sending to {recipient} …")
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient, msg.as_string())
    print(f"Sent: {subject}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
