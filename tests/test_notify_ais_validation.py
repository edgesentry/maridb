"""Tests for notify_ais_validation.py — subject line date extraction."""
import importlib.util
import json
import sys
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


def _make_report(**kwargs):
    base = {
        "overall_pass": True,
        "most_recent_date": "2026-05-02",
        "validate_days": 3,
        "days_passing": 2,
        "active_regions_passing": ["japansea", "singapore"],
        "coverage_check": {"pass": True, "active_passing": 2, "required": 5},
        "region_results": [],
        "day_results": [],
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
