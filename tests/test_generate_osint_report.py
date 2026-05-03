import polars as pl
import pytest

from scripts.generate_osint_report import (
    _ALLOCATED_MIDS,
    _MID_TO_FLAG,
    _flag_from_mid,
    _has_real_name,
    _is_stateless,
    _mt_url,
    _ofac_url,
    _vf_url,
    generate_html_email,
    generate_report,
)


# ---------------------------------------------------------------------------
# ITU MID helpers
# ---------------------------------------------------------------------------


def test_allocated_mids_not_empty():
    assert len(_ALLOCATED_MIDS) > 100


def test_mid_to_flag_coverage():
    # Every MID in _ALLOCATED_MIDS must have a _MID_TO_FLAG entry
    missing = _ALLOCATED_MIDS - set(_MID_TO_FLAG.keys())
    assert not missing, f"MIDs in _ALLOCATED_MIDS but missing from _MID_TO_FLAG: {missing}"


@pytest.mark.parametrize(
    "mmsi,expected",
    [
        ("412171000", False),  # China
        ("563123456", False),  # Singapore
        ("351234567", False),  # Panama
        ("273456789", False),  # Russia
        ("636001234", False),  # Liberia
        ("400789012", True),   # 400 — unallocated
        ("800000001", True),   # 800 — unallocated
        ("100000000", True),   # 100 — unallocated
        ("000000000", True),   # all-zeros — prefix "000" is unallocated
        ("", False),           # empty
        ("12345678", False),   # only 8 digits
        ("ABCDEFGHI", False),  # non-numeric
    ],
)
def test_is_stateless(mmsi, expected):
    assert _is_stateless(mmsi) == expected


@pytest.mark.parametrize(
    "mmsi,expected_flag",
    [
        ("273449240", "RU"),   # Russia
        ("244374235", "NL"),   # Netherlands
        ("563069100", "SG"),   # Singapore
        ("457133000", "MN"),   # Mongolia
        ("400789012", ""),     # unallocated — no flag
        ("", ""),
    ],
)
def test_flag_from_mid(mmsi, expected_flag):
    assert _flag_from_mid(mmsi) == expected_flag


# ---------------------------------------------------------------------------
# Vessel-name fallback detection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "vessel_name,mmsi,expected",
    [
        ("ANHONA", "312171000", True),         # real name
        ("312171000", "312171000", False),     # MMSI used as name fallback
        ("273449240", "273449240", False),     # MMSI used as name fallback
        ("", "563000001", False),              # empty name
        ("123456789", "999999999", False),     # any digit-only string
        ("PIONEER 92", "457133000", True),     # real name with space
    ],
)
def test_has_real_name(vessel_name, mmsi, expected):
    assert _has_real_name(vessel_name, mmsi) == expected


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------


def test_mt_url_contains_mmsi():
    assert "123456789" in _mt_url("123456789")


def test_vf_url_contains_mmsi():
    assert "123456789" in _vf_url("123456789")


def test_ofac_url_encodes_spaces():
    assert "SHADOW+TANKER" in _ofac_url("SHADOW TANKER")


# ---------------------------------------------------------------------------
# generate_report — Markdown
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_watchlist():
    return pl.DataFrame(
        {
            "mmsi":          ["312171000", "400789012", "563456789", "273449240"],
            "imo":           ["9354521",   "",          "9123456",   ""],
            "vessel_name":   ["ANHONA",    "400789012", "REAL VESSEL", "273449240"],
            "vessel_type":   ["Tanker",    "Unknown",   "Cargo",      "Tanker"],
            "flag":          ["BZ",        "",          "SG",         "RU"],
            "confidence":    [0.566,       0.700,       0.489,        0.674],
            "sanctions_distance": [0,      99,          2,            0],
        }
    )


def test_report_is_string(sample_watchlist):
    assert isinstance(generate_report(sample_watchlist, top_n=50), str)


def test_report_header(sample_watchlist):
    assert "[OSINT Check] Watchlist" in generate_report(sample_watchlist, top_n=50)


def test_sanctioned_section_count(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    assert "sanctions_distance=0 (2 vessels)" in report
    assert "ANHONA" in report
    # 273449240 has no real name → shown as "—" not as the MMSI
    assert "| — |" in report or "| —" in report


def test_stateless_section(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    assert "ITU unallocated (1 vessels)" in report
    assert "400789012" in report
    assert "400" in report


def test_stateless_marker_in_full_table(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    lines = [l for l in report.splitlines() if "400789012" in l]
    assert any("⚠" in l for l in lines)


def test_allocated_vessel_has_no_marker(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    lines = [l for l in report.splitlines() if "563456789" in l]
    assert all("⚠" not in l for l in lines)


def test_mmsi_fallback_name_shown_as_dash(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    # 273449240 has vessel_name == mmsi — should display "—", not the MMSI as a name
    lines = [l for l in report.splitlines() if "273449240" in l]
    assert lines, "Row for 273449240 missing from report"
    # The name column should be "—", not "273449240"
    for line in lines:
        cols = [c.strip() for c in line.split("|")]
        name_cols = [c for c in cols if c == "—"]
        assert name_cols, f"Expected '—' name in: {line}"


def test_flag_derived_from_mid_when_blank():
    df = pl.DataFrame(
        {
            "mmsi": ["457133000"],
            "imo": [""],
            "vessel_name": ["PIONEER 92"],
            "vessel_type": ["Cargo"],
            "flag": [""],          # blank in registry
            "confidence": [0.5],
            "sanctions_distance": [0],
        }
    )
    report = generate_report(df, top_n=50)
    # Flag should be derived as MN (Mongolia MID 457)
    assert "MN" in report


def test_ofac_link_absent_when_no_real_name(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    # 273449240 has no real name — its sanctioned-section row must not have an OFAC link
    lines = [l for l in report.splitlines() if "273449240" in l and "sanctions_distance" not in l]
    for line in lines:
        if "| 1 |" in line or "| 4 |" in line:  # the sanctioned rows
            assert "OFAC" not in line


def test_ofac_link_present_for_named_vessel(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    lines = [l for l in report.splitlines() if "ANHONA" in l]
    assert any("OFAC" in l for l in lines)


def test_top_n_limits_rows(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=2)
    assert "400789012" in report   # rank 1
    assert "273449240" in report   # rank 2
    assert "ANHONA" not in report  # rank 3 — excluded
    assert "REAL VESSEL" not in report


def test_links_present(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    assert "marinetraffic.com" in report
    assert "vesselfinder.com" in report


def test_empty_sanctions_section():
    df = pl.DataFrame({
        "mmsi": ["563456789"], "imo": ["9123456"], "vessel_name": ["CLEAN"],
        "vessel_type": ["Cargo"], "flag": ["SG"], "confidence": [0.4],
        "sanctions_distance": [5],
    })
    assert "sanctions_distance=0 (0 vessels)" in generate_report(df, top_n=50)
    assert "No directly sanctioned" in generate_report(df, top_n=50)


def test_empty_stateless_section():
    df = pl.DataFrame({
        "mmsi": ["563456789"], "imo": ["9123456"], "vessel_name": ["CLEAN"],
        "vessel_type": ["Cargo"], "flag": ["SG"], "confidence": [0.4],
        "sanctions_distance": [5],
    })
    assert "ITU unallocated (0 vessels)" in generate_report(df, top_n=50)
    assert "No stateless MMSIs" in generate_report(df, top_n=50)


# ---------------------------------------------------------------------------
# generate_html_email
# ---------------------------------------------------------------------------


def test_html_email_is_html(sample_watchlist):
    html = generate_html_email(sample_watchlist, top_n=50, run_url="https://example.com")
    assert html.strip().startswith("<!DOCTYPE html>")
    assert "<table>" in html
    assert "<td>" in html


def test_html_email_contains_vessel_data(sample_watchlist):
    html = generate_html_email(sample_watchlist, top_n=50, run_url="https://example.com")
    assert "ANHONA" in html
    assert "312171000" in html
    assert "marinetraffic.com" in html


def test_html_email_sdn_badge(sample_watchlist):
    html = generate_html_email(sample_watchlist, top_n=50, run_url="https://example.com")
    assert "SDN" in html


def test_html_email_stateless_badge(sample_watchlist):
    html = generate_html_email(sample_watchlist, top_n=50, run_url="https://example.com")
    assert "badge-stateless" in html


def test_html_email_ci_link(sample_watchlist):
    html = generate_html_email(sample_watchlist, top_n=50, run_url="https://ci.example.com/run/42")
    assert "https://ci.example.com/run/42" in html


def test_html_email_no_ofac_for_unnamed_vessel(sample_watchlist):
    html = generate_html_email(sample_watchlist, top_n=50, run_url="https://example.com")
    # 273449240 has no real name — its OFAC link must not appear
    import re
    ofac_links = re.findall(r'href="(https://sanctionssearch\.ofac[^"]*)"', html)
    for url in ofac_links:
        assert "273449240" not in url, f"OFAC URL should not search for MMSI: {url}"


def test_html_email_flag_derived_from_mid():
    df = pl.DataFrame({
        "mmsi": ["457133000"], "imo": [""], "vessel_name": ["PIONEER 92"],
        "vessel_type": ["Cargo"], "flag": [""],
        "confidence": [0.5], "sanctions_distance": [0],
    })
    html = generate_html_email(df, top_n=50, run_url="https://example.com")
    assert "MN" in html
