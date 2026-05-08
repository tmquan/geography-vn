"""Vietnam GeoJSON helpers for the post-merger 34-province atlas.

Unlike ``personas-vn`` (which uses the upstream geoBoundaries ADM1 file
and adds the two archipelagos as injected features), ``geography-vn``
already has authoritative province polygons in its own
``data/sapnhap-bando-vn/raw/geom/`` directory — they were scraped from
``sapnhap.bando.com.vn`` itself, the publication of record for the 2025
merger boundaries.

This module loads those polygons (via ``data/sapnhap-bando-vn/_hf/geo/
provinces.geojson`` if present, else by stitching them from the raw
``raw/geom/diaphanhanhchinhcaptinh_sn.*.geojson`` files) and additionally
appends the two offshore archipelago bounding outlines from
:mod:`packages.viz.archipelago` so every choropleth ships with the
**complete** national territory declaration: 34 provinces + Hoàng Sa +
Trường Sa.

Province-name normalisation
---------------------------
The site (and the curated parquet bundle) uses the post-merger 34-province
naming with the canonical Vietnamese capitalisation (``Thành phố Cần Thơ``,
``Thủ đô Hà Nội``). Diacritic-stripped fallback keys are also exposed so
joins against external data sources (Wikipedia, geoBoundaries, NSO PX-Web
tables) work without surprise.
"""

from __future__ import annotations

import json
import unicodedata
from functools import lru_cache
from typing import Any

from packages.common.logging import get_logger
from packages.common.paths import REPO_ROOT, ensure_dir
from packages.scraper.normalise import normalise_name
from packages.viz.archipelago import archipelago_features

log = get_logger(__name__)

# Where the post-merger province polygons live. We prefer the staged HF
# bundle (already normalised) but fall back to stitching the raw files.
HF_PROVINCES_GEOJSON = REPO_ROOT / "data" / "sapnhap-bando-vn" / "_hf" / "geo" / "provinces.geojson"
RAW_GEOM_DIR = REPO_ROOT / "data" / "sapnhap-bando-vn" / "raw" / "geom"
COMPLETE_PATH = REPO_ROOT / "data" / "sapnhap-bando-vn" / "_hf" / "geo" / "vietnam_complete.geojson"


# ---------------------------------------------------------------------------
# Notable cities / islands — used for label overlays on country-scale maps.
#
# All names are post-merger ; six centrally-administered cities + the
# capital are emphasised. The ``label_lon`` / ``label_lat`` offsets place
# city labels in the open sea (Gulf of Tonkin / South China Sea) so they
# never collide with province polygons.
# ---------------------------------------------------------------------------
NOTABLE_CITIES: list[dict[str, Any]] = [
    # Capital
    {"name_vi": "Thủ đô Hà Nội", "name_en": "Hanoi",
     "role":    "Thủ đô / Capital",
     "lon": 105.8542, "lat": 21.0285,
     "label_lon": 102.0, "label_lat": 21.0285, "capital": True},
    # 5 other centrally-administered cities (post-merger)
    {"name_vi": "TP. Hồ Chí Minh", "name_en": "Ho Chi Minh City",
     "role":    "Mega-city merged from TPHCM + Bình Dương + BR-VT",
     "lon": 106.7009, "lat": 10.7769,
     "label_lon": 104.0, "label_lat": 10.7769, "capital": False},
    {"name_vi": "TP. Đà Nẵng",    "name_en": "Da Nang",
     "role":    "Đà Nẵng + Quảng Nam",
     "lon": 108.2022, "lat": 16.0544,
     "label_lon": 110.0, "label_lat": 17.6, "capital": False},
    {"name_vi": "TP. Hải Phòng",  "name_en": "Hai Phong",
     "role":    "Hải Phòng + Hải Dương",
     "lon": 106.6881, "lat": 20.8449,
     "label_lon": 108.6, "label_lat": 22.0, "capital": False},
    {"name_vi": "TP. Huế",         "name_en": "Hue",
     "role":    "Cố đô / Former imperial capital",
     "lon": 107.5909, "lat": 16.4637,
     "label_lon": 105.4, "label_lat": 16.4637, "capital": False},
    {"name_vi": "TP. Cần Thơ",    "name_en": "Can Tho",
     "role":    "Cần Thơ + Sóc Trăng + Hậu Giang",
     "lon": 105.7469, "lat": 10.0452,
     "label_lon": 103.5, "label_lat":  8.5, "capital": False},
    # 2 culturally-major provincial seats
    {"name_vi": "TP. Vinh",        "name_en": "Vinh",
     "role":    "Tỉnh Nghệ An (unchanged)",
     "lon": 105.6920, "lat": 18.6792,
     "label_lon": 103.5, "label_lat": 18.6792, "capital": False},
    {"name_vi": "TP. Nha Trang",  "name_en": "Nha Trang",
     "role":    "Tỉnh Khánh Hòa (post-merger)",
     "lon": 109.1968, "lat": 12.2388,
     "label_lon": 110.0, "label_lat": 12.2388, "capital": False},
]


# Notable Vietnamese islands worth labelling on country-scale maps. The
# label is the **island name** (``Đảo …`` / ``Côn Đảo``), not the
# administrative-unit name — these markers exist to call out features of
# the physical geography. Each one's parent ``Đặc khu`` (commune-level
# special administrative zone) lives in ``data/communes.parquet`` instead.
NOTABLE_ISLANDS: list[dict[str, Any]] = [
    # Phú Quốc — Vietnam's largest island. Parent: Đặc khu Phú Quốc / An Giang.
    {"name_vi": "Đảo Phú Quốc",       "name_en": "Phu Quoc Island",
     "province": "Tỉnh An Giang",     "lon": 104.00, "lat": 10.22,
     "label_lon": 102.5, "label_lat":  9.6,
     "note": "Vietnam's largest island, ~590 km²"},
    # Cát Bà — biosphere reserve. Parent: Đặc khu Cát Hải / Hải Phòng.
    {"name_vi": "Đảo Cát Bà",         "name_en": "Cat Ba Island",
     "province": "Thành phố Hải Phòng", "lon": 107.05, "lat": 20.78,
     "label_lon": 109.4, "label_lat": 20.78,
     "note": "Largest island of Hạ Long Bay biosphere"},
    # Bạch Long Vĩ — most remote island in the Gulf of Tonkin.
    {"name_vi": "Đảo Bạch Long Vĩ",   "name_en": "Bach Long Vi Island",
     "province": "Thành phố Hải Phòng", "lon": 107.72, "lat": 20.13,
     "label_lon": 109.4, "label_lat": 19.7,
     "note": "Most remote island in the Gulf of Tonkin"},
    # Côn Đảo — historical penal-colony archipelago; now under TPHCM.
    # Already a proper-noun island name; no ``Đảo`` prefix needed.
    {"name_vi": "Côn Đảo",            "name_en": "Con Dao Islands",
     "province": "Thành phố Hồ Chí Minh", "lon": 106.60, "lat":  8.68,
     "label_lon": 105.0, "label_lat":  7.2,
     "note": "Offshore archipelago; now under TPHCM"},
    # Lý Sơn — volcanic island.
    {"name_vi": "Đảo Lý Sơn",         "name_en": "Ly Son Island",
     "province": "Tỉnh Quảng Ngãi",   "lon": 109.13, "lat": 15.39,
     "label_lon": 110.5, "label_lat": 15.39,
     "note": "Volcanic island, geological park"},
]


# ---------------------------------------------------------------------------
# Province-name normalisation
# ---------------------------------------------------------------------------
def _strip_diacritics(s: str) -> str:
    """NFKD + remove combining marks + special d/D handling."""
    if s is None:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    no_combine = "".join(c for c in nfkd if not unicodedata.combining(c))
    return no_combine.replace("đ", "d").replace("Đ", "D")


def normalise_province_name(name: str) -> str:
    """Return a canonical Vietnamese province name suitable for joining.

    Wraps :func:`packages.scraper.normalise.normalise_name` (which handles
    the Thành Phố / Thành phố / Thủ Đô / Thủ đô capitalisation) and
    additionally strips any tab characters and trailing whitespace that
    third-party datasets sometimes leak.
    """
    if name is None:
        return ""
    return normalise_name(str(name).replace("\t", " ").strip())


def diacritic_key(name: str) -> str:
    """Lower-case ASCII fallback key for joining against datasets that
    don't preserve Vietnamese diacritics (Wikipedia, geoBoundaries, …)."""
    return _strip_diacritics(normalise_province_name(name)).lower()


# ---------------------------------------------------------------------------
# GeoJSON loaders
# ---------------------------------------------------------------------------
def _load_provinces_from_hf() -> dict[str, Any] | None:
    """If ``scripts/upload_to_hf.py`` has been run, the staged HF bundle
    already has a ``provinces.geojson`` we can use directly.

    The staged GeoJSON's ``properties`` block uses the curator schema
    (``ten``, ``area_km2``, ``population``, …); we shim it to the
    cartographic-canonical ``shapeName`` field this module exposes.
    """
    if not HF_PROVINCES_GEOJSON.exists():
        return None
    try:
        fc = json.loads(HF_PROVINCES_GEOJSON.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("HF provinces.geojson unreadable (%s); falling back", exc)
        return None
    for f in fc.get("features") or []:
        props = f.setdefault("properties", {})
        canonical = (props.get("shapeName")
                     or props.get("ten")
                     or props.get("name")
                     or "")
        props["shapeName"] = normalise_province_name(canonical)
        props.setdefault("is_archipelago", False)
    return fc


def _stitch_provinces_from_raw() -> dict[str, Any] | None:
    """Build a province FeatureCollection directly from the raw scraped
    GeoJSONs under ``data/sapnhap-bando-vn/raw/geom/``.

    Used as a fallback when the staged HF bundle does not exist yet.
    Names are pulled from ``data/sapnhap-bando-vn/raw/admin_units.json``
    and canonicalised via :func:`normalise_province_name`.
    """
    admin_units_path = REPO_ROOT / "data" / "sapnhap-bando-vn" / "raw" / "admin_units.json"
    if not admin_units_path.exists() or not RAW_GEOM_DIR.exists():
        return None
    admin = json.loads(admin_units_path.read_text(encoding="utf-8"))
    provinces = [r for r in admin if "captinh" in r.get("malk", "")]
    out_features: list[dict[str, Any]] = []
    for rec in provinces:
        path = RAW_GEOM_DIR / f"{rec['malk']}.geojson"
        if not path.exists():
            continue
        try:
            fc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for f in fc.get("features") or []:
            geom = f.get("geometry")
            if not geom:
                continue
            out_features.append({
                "type":     "Feature",
                "id":       rec["malk"],
                "geometry": geom,
                "properties": {
                    "shapeName":      normalise_province_name(rec["ten"]),
                    "shapeName_en":   "",
                    "ma":             rec.get("ma"),
                    "is_archipelago": False,
                },
            })
    if not out_features:
        return None
    return {"type": "FeatureCollection", "features": out_features}


@lru_cache(maxsize=1)
def load_vietnam_geojson(*, with_archipelagos: bool = True,
                          refresh: bool = False) -> dict[str, Any]:
    """Return the complete FeatureCollection: 34 provinces (post-merger)
    plus, by default, the two offshore archipelago bounding outlines.

    Each feature has ``properties.shapeName`` set to the canonical
    Vietnamese name (after :func:`normalise_province_name`) and
    ``properties.is_archipelago`` set to ``True`` for the two archipelago
    features so downstream renderers can style them differently
    (dashed outline, no fill).
    """
    if not refresh and COMPLETE_PATH.exists():
        try:
            return json.loads(COMPLETE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    base = _load_provinces_from_hf() or _stitch_provinces_from_raw()
    if base is None:
        raise FileNotFoundError(
            "no province polygons on disk; run "
            "`geography-vn curate --only download parse extract` first")

    features = list(base.get("features") or [])
    for f in features:
        f["properties"]["is_archipelago"] = bool(
            f["properties"].get("is_archipelago", False))

    if with_archipelagos:
        features.extend(archipelago_features())

    out = {"type": "FeatureCollection", "features": features}
    try:
        ensure_dir(COMPLETE_PATH.parent)
        COMPLETE_PATH.write_text(json.dumps(out, ensure_ascii=False),
                                   encoding="utf-8")
    except Exception as exc:
        log.warning("could not cache vietnam_complete.geojson (%s)", exc)
    log.info("loaded vietnam_complete.geojson — %d features (%d provinces + %d archipelagos)",
              len(features),
              sum(1 for f in features if not f["properties"].get("is_archipelago")),
              sum(1 for f in features if f["properties"].get("is_archipelago")))
    return out


__all__ = [
    "NOTABLE_CITIES",
    "NOTABLE_ISLANDS",
    "load_vietnam_geojson",
    "normalise_province_name",
    "diacritic_key",
]
