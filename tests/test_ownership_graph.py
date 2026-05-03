"""
Tests for ownership graph feature engineering — focusing on the DuckDB sanctions
fallback introduced in issue #231 and the STS hub degree fixes in issue #233.
"""

import duckdb
import polars as pl
import pyarrow as pa

from pipelines.features.ownership_graph import (
    MAX_HOPS,
    _apply_direct_sanctions_fallback,
    _compute_sanctions_distance,
    _compute_sts_hub_degree,
)
from pipelines.ingest.graph_store import NODE_SCHEMAS, REL_SCHEMAS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_sanctions_mmsi(db_path: str, mmsi_values: list[str]) -> None:
    con = duckdb.connect(db_path)
    for i, mmsi in enumerate(mmsi_values):
        con.execute(
            "INSERT OR IGNORE INTO sanctions_entities "
            "(entity_id, name, mmsi, imo, flag, type, list_source) "
            "VALUES (?, ?, ?, NULL, NULL, 'Vessel', 'us_ofac_sdn')",
            [f"v-{i}", f"VESSEL {i}", mmsi],
        )
    con.close()


# ---------------------------------------------------------------------------
# _apply_direct_sanctions_fallback
# ---------------------------------------------------------------------------


def test_fallback_sets_distance_zero_for_sanctioned_mmsi(tmp_db):
    """A vessel with graph-derived distance=99 whose MMSI is in sanctions_entities
    must have its distance corrected to 0 by the DuckDB fallback."""
    _seed_sanctions_mmsi(tmp_db, ["613490000"])

    sd_df = pl.DataFrame(
        {"mmsi": ["613490000", "999000000"], "sanctions_distance": [MAX_HOPS, MAX_HOPS]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)

    row = result.filter(pl.col("mmsi") == "613490000")
    assert row["sanctions_distance"][0] == 0


def test_fallback_does_not_affect_non_sanctioned_vessel(tmp_db):
    """Vessels not in sanctions_entities must keep their graph-derived distance."""
    _seed_sanctions_mmsi(tmp_db, ["613490000"])

    sd_df = pl.DataFrame(
        {"mmsi": ["613490000", "999000000"], "sanctions_distance": [MAX_HOPS, MAX_HOPS]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)

    row = result.filter(pl.col("mmsi") == "999000000")
    assert row["sanctions_distance"][0] == MAX_HOPS


def test_fallback_does_not_downgrade_existing_distance(tmp_db):
    """A vessel already at distance=1 (graph-derived) must stay at 1, not be
    upgraded to 0 just because its MMSI also appears in sanctions_entities."""
    _seed_sanctions_mmsi(tmp_db, ["123456789"])

    sd_df = pl.DataFrame(
        {"mmsi": ["123456789"], "sanctions_distance": [1]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)

    assert result["sanctions_distance"][0] == 1


def test_fallback_handles_empty_dataframe(tmp_db):
    """Empty input must pass through without error."""
    sd_df = pl.DataFrame(schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32})
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)
    assert result.is_empty()


def test_fallback_handles_no_sanctions_in_db(tmp_db):
    """If sanctions_entities has no MMSI rows the input must be returned unchanged."""
    sd_df = pl.DataFrame(
        {"mmsi": ["613490000"], "sanctions_distance": [MAX_HOPS]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)
    assert result["sanctions_distance"][0] == MAX_HOPS


def test_fallback_corrects_vessel_not_in_lance_graph(tmp_db):
    """The primary use-case: a vessel in ais_positions (hence in the full merged
    feature matrix) but absent from vessel_meta (hence absent from the Lance Graph)
    must have its sanctions_distance corrected when its MMSI is in sanctions_entities.

    This mirrors the build_matrix.py call-site where the fallback receives the
    full 1,240-vessel merged DataFrame, not just the 14-vessel Lance Graph output."""
    _seed_sanctions_mmsi(tmp_db, ["613490000", "620999538"])

    # Simulate the full merged DataFrame: these vessels came via ais_positions
    # but were not in vessel_meta, so the Lance Graph merge gave them distance=99
    full_matrix = pl.DataFrame(
        {
            "mmsi": ["613490000", "620999538", "444000000"],
            "sanctions_distance": [MAX_HOPS, MAX_HOPS, MAX_HOPS],
        },
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(full_matrix, tmp_db)

    assert result.filter(pl.col("mmsi") == "613490000")["sanctions_distance"][0] == 0
    assert result.filter(pl.col("mmsi") == "620999538")["sanctions_distance"][0] == 0
    assert result.filter(pl.col("mmsi") == "444000000")["sanctions_distance"][0] == MAX_HOPS


# ---------------------------------------------------------------------------
# _compute_sts_hub_degree
# ---------------------------------------------------------------------------


def _make_sts_tables(vessel_mmsis: list[str], pairs: list[tuple[str, str]]) -> dict:
    """Build minimal tables dict for _compute_sts_hub_degree tests."""
    vessel_table = pa.table(
        {"mmsi": vessel_mmsis, "imo": [""] * len(vessel_mmsis), "name": [""] * len(vessel_mmsis)},
        schema=NODE_SCHEMAS["Vessel"],
    )
    sts_table = pa.table(
        {"src_id": [p[0] for p in pairs], "dst_id": [p[1] for p in pairs]},
        schema=REL_SCHEMAS["STS_CONTACT"],
    )
    return {"Vessel": vessel_table, "STS_CONTACT": sts_table}


def test_sts_hub_degree_empty_contacts():
    """Vessels with no STS contacts get degree 0."""
    tables = _make_sts_tables(["111111111", "222222222"], [])
    result = _compute_sts_hub_degree(tables)
    assert result.filter(pl.col("mmsi") == "111111111")["sts_hub_degree"][0] == 0
    assert result.filter(pl.col("mmsi") == "222222222")["sts_hub_degree"][0] == 0


def test_sts_hub_degree_src_side_counted():
    """A vessel that appears as src_id gets degree = number of distinct dst partners."""
    tables = _make_sts_tables(
        ["111111111", "222222222"],
        [("111111111", "222222222")],
    )
    result = _compute_sts_hub_degree(tables)
    assert result.filter(pl.col("mmsi") == "111111111")["sts_hub_degree"][0] == 1


def test_sts_hub_degree_dst_side_counted():
    """A vessel that appears only as dst_id must also get a non-zero degree.

    This is the bidirectionality fix: STS_CONTACT stores pairs with src < dst,
    so without the union both_dirs trick, the dst vessel would always get 0.
    """
    # "222222222" > "111111111", so 111111111 is src and 222222222 is dst
    tables = _make_sts_tables(
        ["111111111", "222222222"],
        [("111111111", "222222222")],
    )
    result = _compute_sts_hub_degree(tables)
    # 222222222 appears only as dst_id — must still get degree 1
    assert result.filter(pl.col("mmsi") == "222222222")["sts_hub_degree"][0] == 1


def test_sts_hub_degree_hub_vessel():
    """A vessel with three distinct STS partners gets degree 3."""
    tables = _make_sts_tables(
        ["111111111", "222222222", "333333333", "444444444"],
        [
            ("111111111", "222222222"),
            ("111111111", "333333333"),
            ("111111111", "444444444"),
        ],
    )
    result = _compute_sts_hub_degree(tables)
    assert result.filter(pl.col("mmsi") == "111111111")["sts_hub_degree"][0] == 3


# ---------------------------------------------------------------------------
# _compute_sanctions_distance — UBO traversal (distance 0–3 and 99)
# ---------------------------------------------------------------------------


def _make_distance_tables(
    vessel_mmsis: list[str],
    sanctioned_ids: list[str],
    owned_by: list[tuple[str, str]],
    managed_by: list[tuple[str, str]],
    controlled_by: list[tuple[str, str]],
) -> dict:
    """Build minimal tables dict for _compute_sanctions_distance tests."""
    vessel_table = pa.table(
        {"mmsi": vessel_mmsis, "imo": [""] * len(vessel_mmsis), "name": [""] * len(vessel_mmsis)},
        schema=NODE_SCHEMAS["Vessel"],
    )
    sb_table = pa.table(
        {
            "src_id": sanctioned_ids,
            "dst_id": ["regime"] * len(sanctioned_ids),
            "list": [""] * len(sanctioned_ids),
            "date": [""] * len(sanctioned_ids),
        },
        schema=REL_SCHEMAS["SANCTIONED_BY"],
    )
    ob_table = (
        pa.table(
            {
                "src_id": [r[0] for r in owned_by],
                "dst_id": [r[1] for r in owned_by],
                "since": [""] * len(owned_by),
                "until": [""] * len(owned_by),
            },
            schema=REL_SCHEMAS["OWNED_BY"],
        )
        if owned_by
        else pa.table(
            {"src_id": [], "dst_id": [], "since": [], "until": []},
            schema=REL_SCHEMAS["OWNED_BY"],
        )
    )
    mb_table = (
        pa.table(
            {
                "src_id": [r[0] for r in managed_by],
                "dst_id": [r[1] for r in managed_by],
                "since": [""] * len(managed_by),
                "until": [""] * len(managed_by),
            },
            schema=REL_SCHEMAS["MANAGED_BY"],
        )
        if managed_by
        else pa.table(
            {"src_id": [], "dst_id": [], "since": [], "until": []},
            schema=REL_SCHEMAS["MANAGED_BY"],
        )
    )
    cb_table = (
        pa.table(
            {"src_id": [r[0] for r in controlled_by], "dst_id": [r[1] for r in controlled_by]},
            schema=REL_SCHEMAS["CONTROLLED_BY"],
        )
        if controlled_by
        else pa.table({"src_id": [], "dst_id": []}, schema=REL_SCHEMAS["CONTROLLED_BY"])
    )
    return {
        "Vessel": vessel_table,
        "SANCTIONED_BY": sb_table,
        "OWNED_BY": ob_table,
        "MANAGED_BY": mb_table,
        "CONTROLLED_BY": cb_table,
    }


def test_sanctions_distance_direct():
    """A vessel whose MMSI is directly in SANCTIONED_BY gets distance 0."""
    tables = _make_distance_tables(
        vessel_mmsis=["111111111"],
        sanctioned_ids=["111111111"],
        owned_by=[],
        managed_by=[],
        controlled_by=[],
    )
    result = _compute_sanctions_distance(tables)
    assert result.filter(pl.col("mmsi") == "111111111")["sanctions_distance"][0] == 0


def test_sanctions_distance_one_hop_owned_by():
    """A vessel owned by a sanctioned company gets distance 1."""
    tables = _make_distance_tables(
        vessel_mmsis=["222222222"],
        sanctioned_ids=["company-A"],
        owned_by=[("222222222", "company-A")],
        managed_by=[],
        controlled_by=[],
    )
    result = _compute_sanctions_distance(tables)
    assert result.filter(pl.col("mmsi") == "222222222")["sanctions_distance"][0] == 1


def test_sanctions_distance_one_hop_managed_by():
    """A vessel managed by a sanctioned company gets distance 1."""
    tables = _make_distance_tables(
        vessel_mmsis=["333333333"],
        sanctioned_ids=["company-B"],
        owned_by=[],
        managed_by=[("333333333", "company-B")],
        controlled_by=[],
    )
    result = _compute_sanctions_distance(tables)
    assert result.filter(pl.col("mmsi") == "333333333")["sanctions_distance"][0] == 1


def test_sanctions_distance_two_hop():
    """A vessel owned by a company whose parent is sanctioned gets distance 2."""
    tables = _make_distance_tables(
        vessel_mmsis=["444444444"],
        sanctioned_ids=["parent-corp"],
        owned_by=[("444444444", "child-corp")],
        managed_by=[],
        controlled_by=[("child-corp", "parent-corp")],
    )
    result = _compute_sanctions_distance(tables)
    assert result.filter(pl.col("mmsi") == "444444444")["sanctions_distance"][0] == 2


def test_sanctions_distance_three_hop_ubo():
    """A vessel linked to a sanctioned grandparent (UBO) via two CONTROLLED_BY hops
    gets distance 3."""
    tables = _make_distance_tables(
        vessel_mmsis=["555555555"],
        sanctioned_ids=["grandparent-corp"],
        owned_by=[("555555555", "child-corp")],
        managed_by=[],
        controlled_by=[
            ("child-corp", "parent-corp"),  # child → parent
            ("parent-corp", "grandparent-corp"),  # parent → grandparent (sanctioned)
        ],
    )
    result = _compute_sanctions_distance(tables)
    assert result.filter(pl.col("mmsi") == "555555555")["sanctions_distance"][0] == 3


def test_sanctions_distance_no_connection():
    """A vessel with no graph link to any sanctioned entity gets distance 99."""
    tables = _make_distance_tables(
        vessel_mmsis=["666666666"],
        sanctioned_ids=["unrelated-entity"],
        owned_by=[("666666666", "clean-corp")],
        managed_by=[],
        controlled_by=[],
    )
    result = _compute_sanctions_distance(tables)
    assert result.filter(pl.col("mmsi") == "666666666")["sanctions_distance"][0] == MAX_HOPS


def test_sanctions_distance_priority_closer_wins():
    """When a vessel qualifies for multiple distances, the minimum (closest) wins."""
    # vessel is both distance-1 (managed by sanctioned) and distance-3 (UBO chain)
    tables = _make_distance_tables(
        vessel_mmsis=["777777777"],
        sanctioned_ids=["company-direct", "grandparent-corp"],
        owned_by=[("777777777", "child-corp")],
        managed_by=[("777777777", "company-direct")],
        controlled_by=[
            ("child-corp", "parent-corp"),
            ("parent-corp", "grandparent-corp"),
        ],
    )
    result = _compute_sanctions_distance(tables)
    assert result.filter(pl.col("mmsi") == "777777777")["sanctions_distance"][0] == 1
