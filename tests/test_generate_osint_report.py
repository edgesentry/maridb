import polars as pl
import pytest

from scripts.generate_osint_report import (
    _ALLOCATED_MIDS,
    _is_stateless,
    _mt_url,
    _ofac_url,
    _vf_url,
    generate_report,
)


# ---------------------------------------------------------------------------
# ITU MID helpers
# ---------------------------------------------------------------------------


def test_allocated_mids_not_empty():
    assert len(_ALLOCATED_MIDS) > 100


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


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------


def test_mt_url_contains_mmsi():
    assert "123456789" in _mt_url("123456789")


def test_vf_url_contains_mmsi():
    assert "123456789" in _vf_url("123456789")


def test_ofac_url_encodes_spaces():
    url = _ofac_url("SHADOW TANKER")
    assert "SHADOW+TANKER" in url


def test_ofac_url_unknown_vessel():
    url = _ofac_url("Unknown")
    assert "unknown" in url.lower()


# ---------------------------------------------------------------------------
# generate_report
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_watchlist():
    return pl.DataFrame(
        {
            "mmsi": ["312171000", "400789012", "563456789", "273111222"],
            "imo": ["9354521", "", "9123456", "9876543"],
            "vessel_name": ["ANHONA", "GHOST SHIP", "REAL VESSEL", "PETROVSKY"],
            "vessel_type": ["Tanker", "Unknown", "Cargo", "Tanker"],
            "flag": ["BZ", "", "SG", "RU"],
            "confidence": [0.566, 0.700, 0.489, 0.610],
            "sanctions_distance": [0, 99, 2, 0],
        }
    )


def test_report_is_string(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    assert isinstance(report, str)
    assert len(report) > 0


def test_report_header(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    assert "[OSINT Check] Watchlist" in report


def test_sanctioned_section_count(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    # ANHONA (SD=0) and PETROVSKY (SD=0) — 2 sanctioned vessels
    assert "sanctions_distance=0 (2 vessels)" in report
    assert "ANHONA" in report
    assert "PETROVSKY" in report


def test_stateless_section_count(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    # GHOST SHIP has MID 400 (unallocated)
    assert "ITU unallocated (1 vessels)" in report
    assert "GHOST SHIP" in report
    assert "400" in report


def test_stateless_marker_in_full_table(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    # Stateless vessels are marked with ⚠ in the full table
    lines = [l for l in report.splitlines() if "400789012" in l]
    assert any("⚠" in l for l in lines)


def test_allocated_vessels_have_no_marker(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    lines = [l for l in report.splitlines() if "563456789" in l]
    # ⚠ must not appear on the allocated-MMSI row
    assert all("⚠" not in l for l in lines)


def test_links_present(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=50)
    assert "marinetraffic.com" in report
    assert "vesselfinder.com" in report
    assert "sanctionssearch.ofac.treas.gov" in report


def test_top_n_limits_rows(sample_watchlist):
    report = generate_report(sample_watchlist, top_n=2)
    # Only the top-2 by confidence should appear in the full table
    # GHOST SHIP (0.700) and PETROVSKY (0.610) are top-2
    assert "GHOST SHIP" in report
    assert "PETROVSKY" in report
    assert "ANHONA" not in report
    assert "REAL VESSEL" not in report


def test_empty_sanctions_section():
    df = pl.DataFrame(
        {
            "mmsi": ["563456789"],
            "imo": ["9123456"],
            "vessel_name": ["CLEAN VESSEL"],
            "vessel_type": ["Cargo"],
            "flag": ["SG"],
            "confidence": [0.4],
            "sanctions_distance": [5],
        }
    )
    report = generate_report(df, top_n=50)
    assert "sanctions_distance=0 (0 vessels)" in report
    assert "No directly sanctioned vessels" in report


def test_empty_stateless_section():
    df = pl.DataFrame(
        {
            "mmsi": ["563456789"],
            "imo": ["9123456"],
            "vessel_name": ["CLEAN VESSEL"],
            "vessel_type": ["Cargo"],
            "flag": ["SG"],
            "confidence": [0.4],
            "sanctions_distance": [5],
        }
    )
    report = generate_report(df, top_n=50)
    assert "ITU unallocated (0 vessels)" in report
    assert "No stateless MMSIs" in report
