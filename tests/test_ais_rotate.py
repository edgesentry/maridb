import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.ais_rotate import REGIONS, rotate


# ---------------------------------------------------------------------------
# REGIONS data
# ---------------------------------------------------------------------------

def test_regions_count():
    assert len(REGIONS) == 10


def test_regions_unique_names():
    names = [n for n, _ in REGIONS]
    assert len(names) == len(set(names))


@pytest.mark.parametrize("name,bbox", REGIONS)
def test_region_bbox_shape(name, bbox):
    assert len(bbox) == 4, f"{name}: expected 4 values, got {len(bbox)}"


@pytest.mark.parametrize("name,bbox", REGIONS)
def test_region_bbox_lat_lon_order(name, bbox):
    lat_min, lon_min, lat_max, lon_max = bbox
    assert -90 <= lat_min < lat_max <= 90, f"{name}: invalid lat range [{lat_min}, {lat_max}]"
    assert -180 <= lon_min <= 180, f"{name}: lon_min {lon_min} out of range"
    assert -180 <= lon_max <= 180, f"{name}: lon_max {lon_max} out of range"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_run_slot(calls: list[str], stop_after: int):
    """Return a fake run_slot that records region names and sets stop after N calls."""
    async def fake_run_slot(name, bbox, db_path, api_key, uv_bin, project_dir, slot_duration, stop):
        calls.append(name)
        if len(calls) >= stop_after:
            stop.set()

    return fake_run_slot


async def _run_rotate(regions, max_slots, calls, stop_after):
    with patch("scripts.ais_rotate.run_slot", side_effect=_make_fake_run_slot(calls, stop_after)):
        await rotate(
            api_key="test-key",
            regions=regions,
            max_slots=max_slots,
            slot_duration=1,
            data_dir=Path("/tmp/ais_test"),
            project_dir=Path("/tmp"),
            uv_bin="/usr/bin/true",
        )


# ---------------------------------------------------------------------------
# rotate() — concurrency and round-robin behaviour
# ---------------------------------------------------------------------------

def test_rotate_runs_all_regions_once():
    """Every region is visited exactly once before stop."""
    regions = REGIONS[:4]
    calls: list[str] = []
    asyncio.run(_run_rotate(regions, max_slots=2, calls=calls, stop_after=len(regions)))
    assert sorted(calls) == sorted(n for n, _ in regions)


def test_rotate_max_slots_respected():
    """With 6 regions and 3 slots, all 6 regions are called once."""
    regions = REGIONS[:6]
    calls: list[str] = []
    asyncio.run(_run_rotate(regions, max_slots=3, calls=calls, stop_after=len(regions)))
    assert len(calls) == len(regions)
    assert sorted(calls) == sorted(n for n, _ in regions)


def test_rotate_single_slot_sequential():
    """With max_slots=1, regions run sequentially in order."""
    regions = REGIONS[:3]
    calls: list[str] = []
    asyncio.run(_run_rotate(regions, max_slots=1, calls=calls, stop_after=len(regions)))
    assert calls == [n for n, _ in regions]


def test_rotate_slots_exceeds_regions():
    """max_slots > len(regions): only len(regions) tasks start."""
    regions = REGIONS[:2]
    calls: list[str] = []
    asyncio.run(_run_rotate(regions, max_slots=5, calls=calls, stop_after=len(regions)))
    assert sorted(calls) == sorted(n for n, _ in regions)


def test_rotate_stops_immediately_on_first_slot():
    """Stop event set by the first slot prevents any further slots from starting."""
    regions = REGIONS[:4]
    calls: list[str] = []
    asyncio.run(_run_rotate(regions, max_slots=1, calls=calls, stop_after=1))
    assert len(calls) == 1


def test_rotate_round_robin_continues():
    """After one full cycle (4 regions, 2 slots), the second cycle starts correctly."""
    regions = REGIONS[:4]
    calls: list[str] = []
    asyncio.run(_run_rotate(regions, max_slots=2, calls=calls, stop_after=6))
    assert len(calls) == 6
    # First 4 calls cover all regions
    assert sorted(calls[:4]) == sorted(n for n, _ in regions)


# ---------------------------------------------------------------------------
# Region filtering (mirrors main() logic)
# ---------------------------------------------------------------------------

def _filter_regions(names_csv: str) -> list[tuple[str, list[float]]]:
    names = {r.strip() for r in names_csv.split(",")}
    result = [(n, b) for n, b in REGIONS if n in names]
    unknown = names - {n for n, _ in result}
    if unknown:
        raise ValueError(f"Unknown regions: {', '.join(sorted(unknown))}")
    return result


def test_filter_regions_subset():
    result = _filter_regions("blacksea,japansea")
    assert {n for n, _ in result} == {"blacksea", "japansea"}


def test_filter_regions_unknown_raises():
    with pytest.raises(ValueError, match="Unknown regions"):
        _filter_regions("blacksea,notaregion")


def test_filter_regions_single():
    for name, _ in REGIONS:
        result = _filter_regions(name)
        assert len(result) == 1
        assert result[0][0] == name
