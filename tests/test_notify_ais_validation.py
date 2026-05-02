"""Tests for notify_ais_validation.py — subject line date and active region display."""
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


def _load_notify():
    spec = importlib.util.spec_from_file_location(
        "notify_ais_validation",
        Path(__file__).parent.parent / "scripts" / "notify_ais_validation.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_day_result(date="2026-05-02", active_passing=None, required=5, region_results=None):
    if active_passing is None:
        active_passing = ["japansea", "singapore", "europe", "blacksea", "middleeast", "gulfofguinea"]
    return {
        "date": date,
        "pass": True,
        "active_regions_passing": active_passing,
        "coverage_check": {"pass": True, "active_passing": len(active_passing), "required": required},
        "region_results": region_results or [],
    }


def _make_report(**kwargs):
    day = _make_day_result()
    base = {
        "overall_pass": True,
        "most_recent_date": "2026-05-02",
        "validate_days": 3,
        "days_passing": 2,
        "day_results": [day],
    }
    base.update(kwargs)
    return base


def test_subject_uses_most_recent_date(tmp_path, monkeypatch):
    """Subject must show the actual date, not 'unknown'."""
    report_path = tmp_path / "ais_validation_report.json"
    report_path.write_text(json.dumps(_make_report()))

    mod = _load_notify()
    monkeypatch.setattr(mod, "_REPORT_PATH", report_path)

    subjects = []

    class FakeSMTP:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def starttls(self): pass
        def login(self, *a): pass
        def sendmail(self, _from, _to, msg):
            for line in msg.splitlines():
                if line.startswith("Subject:"):
                    subjects.append(line)

    monkeypatch.setenv("NOTIFY_EMAIL", "test@example.com")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "user@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "secret")
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    monkeypatch.setenv("GITHUB_REPOSITORY", "edgesentry/maridb")

    with patch("smtplib.SMTP", FakeSMTP):
        mod.main()

    assert subjects, "no email sent"
    assert "unknown" not in subjects[0], f"date is 'unknown' in: {subjects[0]}"
    assert "2026-05-02" in subjects[0], f"expected date not in: {subjects[0]}"


def test_subject_falls_back_gracefully_when_date_missing(tmp_path, monkeypatch):
    """If both most_recent_date and target_date are absent, subject shows 'unknown'."""
    report = _make_report()
    del report["most_recent_date"]
    report_path = tmp_path / "ais_validation_report.json"
    report_path.write_text(json.dumps(report))

    mod = _load_notify()
    monkeypatch.setattr(mod, "_REPORT_PATH", report_path)

    subjects = []

    class FakeSMTP:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def starttls(self): pass
        def login(self, *a): pass
        def sendmail(self, _from, _to, msg):
            for line in msg.splitlines():
                if line.startswith("Subject:"):
                    subjects.append(line)

    monkeypatch.setenv("NOTIFY_EMAIL", "test@example.com")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "user@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "secret")
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    monkeypatch.setenv("GITHUB_REPOSITORY", "edgesentry/maridb")

    with patch("smtplib.SMTP", FakeSMTP):
        mod.main()

    assert subjects
    assert "unknown" in subjects[0]


def test_subject_uses_target_date_as_fallback(tmp_path, monkeypatch):
    """target_date is used when most_recent_date is absent (backwards compat)."""
    report = _make_report()
    del report["most_recent_date"]
    report["target_date"] = "2026-04-30"
    report_path = tmp_path / "ais_validation_report.json"
    report_path.write_text(json.dumps(report))

    mod = _load_notify()
    monkeypatch.setattr(mod, "_REPORT_PATH", report_path)

    subjects = []

    class FakeSMTP:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def starttls(self): pass
        def login(self, *a): pass
        def sendmail(self, _from, _to, msg):
            for line in msg.splitlines():
                if line.startswith("Subject:"):
                    subjects.append(line)

    monkeypatch.setenv("NOTIFY_EMAIL", "test@example.com")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "user@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "secret")
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    monkeypatch.setenv("GITHUB_REPOSITORY", "edgesentry/maridb")

    with patch("smtplib.SMTP", FakeSMTP):
        mod.main()

    assert subjects
    assert "2026-04-30" in subjects[0]
    assert "unknown" not in subjects[0]


def _decode_body(raw_msg: str) -> str:
    """Extract and decode the HTML body from a raw MIME message string."""
    import email
    msg = email.message_from_string(raw_msg)
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            payload = part.get_payload(decode=True)
            return payload.decode("utf-8") if payload else ""
    return raw_msg


def _fake_smtp_capture(bodies):
    class FakeSMTP:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def starttls(self): pass
        def login(self, *a): pass
        def sendmail(self, _from, _to, msg):
            bodies.append(_decode_body(msg))
    return FakeSMTP


def _set_smtp_env(monkeypatch):
    monkeypatch.setenv("NOTIFY_EMAIL", "test@example.com")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "user@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "secret")
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    monkeypatch.setenv("GITHUB_REPOSITORY", "edgesentry/maridb")


def test_active_regions_read_from_day_results(tmp_path, monkeypatch):
    """Active regions must come from day_results[-1], not the top level."""
    day = _make_day_result(
        active_passing=["japansea", "singapore", "europe", "blacksea", "middleeast", "gulfofguinea"],
        required=5,
    )
    report = _make_report(day_results=[day])
    report_path = tmp_path / "ais_validation_report.json"
    report_path.write_text(json.dumps(report))

    mod = _load_notify()
    monkeypatch.setattr(mod, "_REPORT_PATH", report_path)
    _set_smtp_env(monkeypatch)

    bodies = []
    with patch("smtplib.SMTP", _fake_smtp_capture(bodies)):
        mod.main()

    assert bodies
    body = bodies[0]
    assert "6/5" in body, f"expected 6/5 in email body, got:\n{body[:500]}"
    assert "none" not in body.lower().split("active regions passing")[1][:50]


def test_active_regions_not_none_when_populated(tmp_path, monkeypatch):
    """Email must not show '(none)' when active regions are present."""
    day = _make_day_result(active_passing=["japansea", "singapore", "europe"])
    report = _make_report(day_results=[day])
    report_path = tmp_path / "ais_validation_report.json"
    report_path.write_text(json.dumps(report))

    mod = _load_notify()
    monkeypatch.setattr(mod, "_REPORT_PATH", report_path)
    _set_smtp_env(monkeypatch)

    bodies = []
    with patch("smtplib.SMTP", _fake_smtp_capture(bodies)):
        mod.main()

    assert bodies
    assert "japansea" in bodies[0]
    assert "singapore" in bodies[0]
    assert "(none)" not in bodies[0]


def test_active_regions_shows_none_when_empty(tmp_path, monkeypatch):
    """Email must show '(none)' when no active regions pass."""
    day = _make_day_result(active_passing=[])
    report = _make_report(day_results=[day], overall_pass=False)
    report_path = tmp_path / "ais_validation_report.json"
    report_path.write_text(json.dumps(report))

    mod = _load_notify()
    monkeypatch.setattr(mod, "_REPORT_PATH", report_path)
    _set_smtp_env(monkeypatch)

    bodies = []
    with patch("smtplib.SMTP", _fake_smtp_capture(bodies)):
        mod.main()

    assert bodies
    assert "none" in bodies[0].lower()
