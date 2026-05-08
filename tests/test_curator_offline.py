"""Offline tests for the curator stages — uses a hand-built fixture so the
suite never touches the live ``sapnhap.bando.com.vn`` server.
"""

from __future__ import annotations

import json
from pathlib import Path

from packages.common.config import Config
from packages.curator.regions import province_to_region
from packages.curator.stages import (
    ExtractStage,
    ParseStage,
    _explode_predecessors,
    _split_type_prefix,
    _summarise_geometry,
)


def _write_fixture(root: Path) -> None:
    raw = root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    (raw / "details").mkdir(exist_ok=True)
    (raw / "geom").mkdir(exist_ok=True)

    admin_units = [
        {"ma": "92", "ten": "Thành Phố Cần Thơ", "magoc": "0",
         "malk": "diaphanhanhchinhcaptinh_sn.1",
         "truocsapnhap": "thành phố Cần Thơ, tỉnh Sóc Trăng và tỉnh Hậu Giang"},
        {"ma": "00004", "ten": "Phường Ba Đình", "magoc": "01",
         "malk": "diaphanhanhchinhcapxa_2025.3256",
         "truocsapnhap": "Phường Quán Thánh, Phường Trúc Bạch và phần còn lại của Phường Cửa Đông"},
    ]
    (raw / "admin_units.json").write_text(
        json.dumps(admin_units, ensure_ascii=False), encoding="utf-8")

    committees = [
        {"id": 1, "ten": "Đặc khu Phú Quốc",
         "ma": "uybannhandancapxa_2025.1"},
    ]
    (raw / "committees.json").write_text(
        json.dumps(committees, ensure_ascii=False), encoding="utf-8")

    (raw / "details" / "diaphanhanhchinhcaptinh_sn.1.json").write_text(
        json.dumps({
            "id": 1,
            "dientichkm2": "6.360,83",
            "dansonguoi": "4.199.824",
            "trungtamhc": "Cần Thơ (cũ)",
            "truocsapnhap": "thành phố Cần Thơ, tỉnh Sóc Trăng và tỉnh Hậu Giang",
            "con": "103 ĐVHC",
            "ma": "92",
            "ten": "Thành Phố Cần Thơ",
            "malk": "diaphanhanhchinhcaptinh_sn.1",
            "diachi": "Số 02 Hòa Bình",
            "dthoai": "080 71162",
            "cancu": "Nghị quyết số 202/2025/QH15",
            "tentinh": None,
            "link": "https://vanban.chinhphu.vn/?docid=213930",
        }), encoding="utf-8")

    (raw / "details" / "diaphanhanhchinhcapxa_2025.3256.json").write_text(
        json.dumps({
            "id": 35, "dientichkm2": "2,97", "dansonguoi": "65.023",
            "trungtamhc": "Số 2, phố Trúc Bạch",
            "truocsapnhap": "Phường Quán Thánh, Phường Trúc Bạch",
            "ma": "00004", "ten": "Phường Ba Đình",
            "malk": "diaphanhanhchinhcapxa_2025.3256",
            "tentinh": "Thủ đô Hà Nội",
            "cancu": "Nghị quyết số 1656/NQ-UBTVQH15",
            "link": "https://vanban.chinhphu.vn/?docid=214008",
        }), encoding="utf-8")

    # Tiny polygon fixture for Cần Thơ (a triangle around its rough centroid).
    (raw / "geom" / "diaphanhanhchinhcaptinh_sn.1.geojson").write_text(json.dumps({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Polygon",
                          "coordinates": [[[105.7, 9.7], [106.0, 10.1],
                                            [105.5, 10.0], [105.7, 9.7]]]},
        }],
    }), encoding="utf-8")

    (raw / "geom" / "uybannhandancapxa_2025.1.geojson").write_text(json.dumps({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [104.0307, 10.4116]},
            "properties": {
                "a01_ten": "Đặc khu Phú Quốc",
                "a02_gc":  "Điểm dân cư trên 100000 người",
                "a03_tenxa": "Đặc khu Phú Quốc",
                "a04_tentinh": "Tỉnh An Giang",
                "a05_ma": "uybannhandancapxa_2025.1",
                "a06_dt": "575.29",
                "a07_ds": "157629",
                "a08_dthoai": None, "a09_web": None,
            },
        }],
    }), encoding="utf-8")


# ---------------------------------------------------------------------------
def test_split_type_prefix_canonicalises():
    # Both PascalCase and lowercase forms canonicalise to the
    # standard Vietnamese form (only first word capitalised).
    assert _split_type_prefix("Thành Phố Cần Thơ") == ("Thành phố", "Cần Thơ")
    assert _split_type_prefix("Thành phố Đồng Nai") == ("Thành phố", "Đồng Nai")
    assert _split_type_prefix("Thủ Đô Hà Nội") == ("Thủ đô", "Hà Nội")
    assert _split_type_prefix("Thủ đô Hà Nội") == ("Thủ đô", "Hà Nội")
    assert _split_type_prefix("Tỉnh Cao Bằng") == ("Tỉnh", "Cao Bằng")
    assert _split_type_prefix("Phường Ba Đình") == ("Phường", "Ba Đình")
    assert _split_type_prefix("Đặc khu Phú Quốc") == ("Đặc khu", "Phú Quốc")
    assert _split_type_prefix("Ủy ban nhân dân Xã Hòn Nghệ") == \
        ("Ủy ban nhân dân", "Xã Hòn Nghệ")


def test_explode_predecessors():
    s = "thành phố Cần Thơ, tỉnh Sóc Trăng và tỉnh Hậu Giang"
    assert _explode_predecessors(s) == ["thành phố Cần Thơ",
                                          "tỉnh Sóc Trăng",
                                          "tỉnh Hậu Giang"]
    s = "Phường Quán Thánh, Phường Trúc Bạch và phần còn lại của Phường Cửa Đông"
    assert _explode_predecessors(s) == ["Phường Quán Thánh",
                                          "Phường Trúc Bạch",
                                          "Phường Cửa Đông"]


def test_summarise_geometry_point():
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature",
         "geometry": {"type": "Point", "coordinates": [104.03, 10.41]},
         "properties": {}},
    ]}
    s = _summarise_geometry(fc)
    assert s["geom_type"] == "Point"
    assert abs(s["centroid_lon"] - 104.03) < 1e-6
    assert abs(s["centroid_lat"] - 10.41) < 1e-6


def test_parse_then_extract(tmp_path: Path):
    _write_fixture(tmp_path)
    parsed_dir = tmp_path / "parsed"
    extracted_dir = tmp_path / "extracted"

    cfg_parse = Config({"min_text_chars": 0, "flatten_geojson": True})
    parse = ParseStage(cfg_parse, in_dir=tmp_path / "raw", out_dir=parsed_dir)
    parse.setup()
    summary = parse.run()
    assert summary["n"] == 3                           # 1 prov + 1 commune + 1 cmte
    assert summary["by_kind"]["province"] == 1
    assert summary["by_kind"]["commune"] == 1
    assert summary["by_kind"]["committee"] == 1

    cfg_extract = Config({
        "top_keywords": 4,
        "vectorizer":   "tfidf",
        "ngram_range":  [1, 2],
        "max_df":       0.99,
        "min_df":       1,
    })
    extract = ExtractStage(cfg_extract, in_dir=parsed_dir, out_dir=extracted_dir)
    extract.setup()
    summary = extract.run()
    assert summary["n"] == 3

    # Province row should carry mekong_delta region; commune row should
    # carry red_river_delta because tentinh="Thủ đô Hà Nội".
    rows = [json.loads(line) for line in (extracted_dir / "extracted.jsonl").open()]
    by_id = {r["id"]: r for r in rows}
    assert by_id["diaphanhanhchinhcaptinh_sn.1"]["macro_region"] == "mekong_delta"
    assert by_id["diaphanhanhchinhcapxa_2025.3256"]["macro_region"] == "red_river_delta"
    assert by_id["uybannhandancapxa_2025.1"]["macro_region"] == "mekong_delta"


def test_province_to_region_handles_all_34():
    # Spot-check one province per macro-region.
    cases = [
        ("Thủ Đô Hà Nội",          "red_river_delta"),
        ("Tỉnh Lào Cai",           "northern_midlands"),
        ("Thành Phố Đà Nẵng",      "central_coast"),
        ("Tỉnh Đắk Lắk",           "central_highlands"),
        ("Thành Phố Hồ Chí Minh",  "southeast"),
        ("Thành Phố Cần Thơ",      "mekong_delta"),
    ]
    for name, want in cases:
        assert province_to_region(name) == want
