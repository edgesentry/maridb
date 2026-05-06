"""Tests for push_metrics_snapshot.py."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from scripts.push_metrics_snapshot import (
    _collect_snapshot,
    _delete_key,
    _make_client,
    _read_index,
    _write_json,
)


def test_collect_snapshot_minimal(tmp_path, monkeypatch):
    """_collect_snapshot returns a dict with at least date and generated_at."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    snap = _collect_snapshot("2026-05-06")
    assert snap["date"] == "2026-05-06"
    assert "generated_at_utc" in snap


def test_collect_snapshot_reads_backtest_summary(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    summary = {
        "metrics_summary": {
            "precision_at_50": {"mean": 0.356, "ci95_low": 0.289, "ci95_high": 0.423},
            "recall_at_200": {"mean": 1.0},
            "auroc": {"mean": 0.87},
        },
        "total_known_cases": 89,
        "regions": ["singapore", "japan"],
        "skipped_regions": [],
    }
    (processed / "backtest_public_integration_summary.json").write_text(json.dumps(summary))

    snap = _collect_snapshot("2026-05-06")
    assert snap["precision_at_50"] == pytest.approx(0.356)
    assert snap["precision_at_50_ci_low"] == pytest.approx(0.289)
    assert snap["recall_at_200"] == pytest.approx(1.0)
    assert snap["known_positives"] == 89
    assert snap["regions"] == ["singapore", "japan"]


def test_collect_snapshot_reads_lead_time(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    lead = {
        "pre_designation_count": 6,
        "mean_lead_days": 26,
        "median_lead_days": 29,
        "unknown_unknown_candidates": 50,
    }
    (processed / "lead_time_report.json").write_text(json.dumps(lead))

    snap = _collect_snapshot("2026-05-06")
    assert snap["pre_designation_count"] == 6
    assert snap["median_lead_days"] == 29
    assert snap["unknown_unknown_candidates"] == 50


def test_collect_snapshot_partial_files(tmp_path, monkeypatch):
    """Only lead time report available — no backtest summary."""
    monkeypatch.chdir(tmp_path)
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    lead = {"pre_designation_count": 3, "mean_lead_days": 22, "median_lead_days": 22, "unknown_unknown_candidates": 10}
    (processed / "lead_time_report.json").write_text(json.dumps(lead))

    snap = _collect_snapshot("2026-05-06")
    assert snap["pre_designation_count"] == 3
    assert snap.get("precision_at_50") is None


def test_dry_run_cli(tmp_path, monkeypatch):
    """--dry-run prints snapshot without uploading (no credentials needed)."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "push_metrics_snapshot.py"),
         "--dry-run", "--date", "2026-05-06"],
        capture_output=True, text=True,
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "2026-05-06" in result.stdout
    assert "dry-run" in result.stdout


def test_dry_run_includes_all_keys(tmp_path, monkeypatch):
    """--dry-run snapshot contains expected top-level keys."""
    monkeypatch.chdir(tmp_path)
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    summary = {
        "metrics_summary": {
            "precision_at_50": {"mean": 0.40},
            "recall_at_200": {"mean": 0.95},
            "auroc": {"mean": 0.80},
        },
        "total_known_cases": 50,
        "regions": ["singapore"],
        "skipped_regions": [],
    }
    lead = {"pre_designation_count": 4, "mean_lead_days": 28, "median_lead_days": 28, "unknown_unknown_candidates": 30}
    (processed / "backtest_public_integration_summary.json").write_text(json.dumps(summary))
    (processed / "lead_time_report.json").write_text(json.dumps(lead))

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "push_metrics_snapshot.py"),
         "--dry-run", "--date", "2026-05-06"],
        capture_output=True, text=True,
        cwd=tmp_path,
    )
    assert result.returncode == 0
    output = result.stdout
    for key in ("precision_at_50", "recall_at_200", "pre_designation_count", "median_lead_days"):
        assert key in output, f"missing key in output: {key}"


# ---------------------------------------------------------------------------
# boto3 upload helpers
# ---------------------------------------------------------------------------

def _make_mock_client(bucket: str = "maridb-public"):
    """Return a (mock_client, bucket) tuple with a pre-configured boto3 mock."""
    mock = MagicMock()
    return (mock, bucket), mock


def test_write_json_calls_put_object():
    """_write_json uses boto3 put_object (not multipart)."""
    cb, mock = _make_mock_client()
    _write_json(cb, "maridb-public", "metrics/20260506.json", {"date": "2026-05-06"})
    mock.put_object.assert_called_once()
    kwargs = mock.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "maridb-public"
    assert kwargs["Key"] == "metrics/20260506.json"
    assert kwargs["ContentType"] == "application/json"
    assert b"2026-05-06" in kwargs["Body"]


def test_write_json_serialises_correctly():
    """_write_json Body is valid JSON with expected content."""
    cb, mock = _make_mock_client()
    obj = {"date": "2026-05-06", "precision_at_50": 0.356}
    _write_json(cb, "maridb-public", "metrics/20260506.json", obj)
    body = mock.put_object.call_args.kwargs["Body"]
    parsed = json.loads(body.decode())
    assert parsed["precision_at_50"] == pytest.approx(0.356)


def test_read_index_returns_entries():
    cb, mock = _make_mock_client()
    mock.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({"entries": ["20260506", "20260505"]}).encode())
    }
    entries = _read_index(cb, "maridb-public")
    assert entries == ["20260506", "20260505"]
    mock.get_object.assert_called_once_with(Bucket="maridb-public", Key="metrics/index.json")


def test_read_index_returns_empty_on_error():
    """Missing index returns [] without raising."""
    cb, mock = _make_mock_client()
    mock.get_object.side_effect = Exception("NoSuchKey")
    entries = _read_index(cb, "maridb-public")
    assert entries == []


def test_delete_key_calls_delete_object():
    cb, mock = _make_mock_client()
    _delete_key(cb, "maridb-public", "metrics/20260430.json")
    mock.delete_object.assert_called_once_with(Bucket="maridb-public", Key="metrics/20260430.json")


def test_delete_key_ignores_errors(capsys):
    """Deletion errors are logged as warnings, not raised."""
    cb, mock = _make_mock_client()
    mock.delete_object.side_effect = Exception("AccessDenied")
    _delete_key(cb, "maridb-public", "metrics/20260430.json")  # must not raise
    captured = capsys.readouterr()
    assert "warning" in captured.err


def test_make_client_uses_env_credentials(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("AWS_REGION", "auto")
    with patch("boto3.client") as mock_boto3:
        mock_boto3.return_value = MagicMock()
        cb, bucket = _make_client("maridb-public")
        mock_boto3.assert_called_once()
        kwargs = mock_boto3.call_args.kwargs
        assert kwargs["aws_access_key_id"] == "test-key"
        assert kwargs["aws_secret_access_key"] == "test-secret"
        assert bucket == "maridb-public"
