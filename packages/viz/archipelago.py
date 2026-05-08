"""Hoàng Sa & Trường Sa archipelago declarations.

Every official Vietnamese cartographic publication includes the two
offshore archipelagos — **Quần đảo Hoàng Sa (Paracel Islands)** and
**Quần đảo Trường Sa (Spratly Islands)** — as bounding-box outlines on
country-scale maps. The Vietnam Cartographic Publishing House atlas at
``sapnhap.bando.com.vn`` follows that convention; so do the GSO atlases,
the Ministry of Foreign Affairs maps, and every Vietnamese-language atlas
this project would be a peer to.

This module ports the bounding-box polygons and principal-island marker
sets from
[`personas-vn`'s `packages/viz/vietnam_geo.py`](https://github.com/tmquan/personas-vn/blob/main/packages/viz/vietnam_geo.py)
so the two repos render the archipelagos identically. The polygons are
intentionally simplified to match how Vietnamese government cartography
depicts the two clusters; tighter outlines exist (see e.g. the COC zones
on MOFA maps) but are not appropriate for country-wide visualisation.

Post-2025 administrative status
-------------------------------
* **Hoàng Sa** — administered as ``Đặc khu Hoàng Sa`` under
  **Thành phố Đà Nẵng** (after the 2025 merger). Population: 0
  (sovereignty assertion only).
* **Trường Sa** — administered as ``Đặc khu Trường Sa`` under
  **Tỉnh Khánh Hòa**. Population: 153 registered residents (2024 census).

Both special administrative units appear in the curated parquet bundle as
rows under ``data/communes.parquet``; this module exists separately so the
**bounding outlines** of the broader archipelagos (which are larger than
just the registered settlements) can be drawn on top.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Bounding polygons (counter-clockwise per RFC 7946)
# ---------------------------------------------------------------------------
HOANG_SA_POLYGON: list[list[float]] = [
    # Bounding box around the principal Hoàng Sa islands listed below
    # (Tri Tôn at 111.20°E, Linh Côn at 112.73°E; Tri Tôn at 15.78°N,
    # Phú Lâm at 16.83°N) with ~0.30° cartographic padding for visual
    # breathing room. Tighter than MOFA's full sovereignty claim
    # (15°45′N–17°15′N, 111°10′E–113°00′E) but a touch bigger than a
    # strict bounding box of the visible markers.
    [110.85, 15.45],
    [113.10, 15.45],
    [113.10, 17.20],
    [110.85, 17.20],
    [110.85, 15.45],
]
TRUONG_SA_POLYGON: list[list[float]] = [
    # Bounding box around the principal Trường Sa islands listed below
    # (Trường Sa Lớn at 111.93°E, Sơn Ca at 114.48°E; An Bang at 7.88°N,
    # Song Tử Tây at 11.43°N). Shifted ~0.30° east of a strict bounding
    # box so the archipelago carries more open-sea separation from the
    # Vietnam mainland to the west.
    [111.80,  7.40],
    [115.30,  7.40],
    [115.30, 11.85],
    [111.80, 11.85],
    [111.80,  7.40],
]


# ---------------------------------------------------------------------------
# Principal islands inside each archipelago — drawn as small markers so the
# bounding outline is not visually empty. Coordinates from Wikipedia infoboxes
# (DMS-to-decimal converted; sanity-checked against MOFA atlas).
# ---------------------------------------------------------------------------
HOANG_SA_ISLANDS: list[dict[str, Any]] = [
    {"name_vi": "Đảo Phú Lâm",   "name_en": "Woody Island",        "lon": 112.33, "lat": 16.83},
    {"name_vi": "Đảo Tri Tôn",   "name_en": "Triton Island",       "lon": 111.20, "lat": 15.78},
    {"name_vi": "Đảo Linh Côn",  "name_en": "Lincoln Island",      "lon": 112.73, "lat": 16.67},
    {"name_vi": "Đảo Quang Hòa", "name_en": "Duncan Island",       "lon": 111.70, "lat": 16.45},
]
TRUONG_SA_ISLANDS: list[dict[str, Any]] = [
    {"name_vi": "Đảo Trường Sa Lớn", "name_en": "Spratly Island",  "lon": 111.93, "lat":  8.64},
    {"name_vi": "Song Tử Tây",       "name_en": "Southwest Cay",   "lon": 114.33, "lat": 11.43},
    {"name_vi": "Đảo Sinh Tồn",      "name_en": "Sin Cowe Island", "lon": 114.33, "lat":  9.88},
    {"name_vi": "Đảo Phan Vinh",     "name_en": "Pearson Reef",    "lon": 113.69, "lat":  8.95},
    {"name_vi": "Đảo An Bang",       "name_en": "Amboyna Cay",     "lon": 112.92, "lat":  7.88},
    {"name_vi": "Đảo Nam Yết",       "name_en": "Namyit Island",   "lon": 114.37, "lat": 10.18},
    {"name_vi": "Đá Cô Lin",         "name_en": "Collins Reef",    "lon": 114.26, "lat":  9.74},
    {"name_vi": "Đảo Sơn Ca",        "name_en": "Sand Cay",        "lon": 114.48, "lat": 10.38},
]
SCATTERED_ISLAND_MARKERS: list[dict[str, Any]] = (
    [{**i, "archipelago": "Hoàng Sa"}  for i in HOANG_SA_ISLANDS] +
    [{**i, "archipelago": "Trường Sa"} for i in TRUONG_SA_ISLANDS]
)


# ---------------------------------------------------------------------------
# Convenience metadata blobs — same shape as personas-vn's HOANG_SA / TRUONG_SA
# ---------------------------------------------------------------------------
HOANG_SA = {
    "name_vi": "Quần đảo Hoàng Sa",
    "name_en": "Paracel Islands",
    "name_special_unit": "Đặc khu Hoàng Sa",
    "admin_post_merger": "Thành phố Đà Nẵng",
    "lon_min": HOANG_SA_POLYGON[0][0], "lat_min": HOANG_SA_POLYGON[0][1],
    "lon_max": HOANG_SA_POLYGON[2][0], "lat_max": HOANG_SA_POLYGON[2][1],
    "centre":  [
        (HOANG_SA_POLYGON[0][0] + HOANG_SA_POLYGON[2][0]) / 2,
        (HOANG_SA_POLYGON[0][1] + HOANG_SA_POLYGON[2][1]) / 2,
    ],
    "polygon": HOANG_SA_POLYGON,
    "islands": HOANG_SA_ISLANDS,
}
TRUONG_SA = {
    "name_vi": "Quần đảo Trường Sa",
    "name_en": "Spratly Islands",
    "name_special_unit": "Đặc khu Trường Sa",
    "admin_post_merger": "Tỉnh Khánh Hòa",
    "lon_min": TRUONG_SA_POLYGON[0][0], "lat_min": TRUONG_SA_POLYGON[0][1],
    "lon_max": TRUONG_SA_POLYGON[2][0], "lat_max": TRUONG_SA_POLYGON[2][1],
    "centre":  [
        (TRUONG_SA_POLYGON[0][0] + TRUONG_SA_POLYGON[2][0]) / 2,
        (TRUONG_SA_POLYGON[0][1] + TRUONG_SA_POLYGON[2][1]) / 2,
    ],
    "polygon": TRUONG_SA_POLYGON,
    "islands": TRUONG_SA_ISLANDS,
}


# ---------------------------------------------------------------------------
# GeoJSON Feature factory — what gets injected into the choropleth map
# ---------------------------------------------------------------------------
def archipelago_features() -> list[dict[str, Any]]:
    """Return two GeoJSON ``Feature`` dicts (Hoàng Sa + Trường Sa) ready
    to be appended to a ``FeatureCollection``.
    """
    out: list[dict[str, Any]] = []
    for meta in (HOANG_SA, TRUONG_SA):
        out.append({
            "type": "Feature",
            "id":   f"archipelago-{meta['name_en'].lower().replace(' ', '-')}",
            "properties": {
                "shapeName":         meta["name_vi"],
                "shapeName_en":      meta["name_en"],
                "name_special_unit": meta["name_special_unit"],
                "admin_post_merger": meta["admin_post_merger"],
                "is_archipelago":    True,
            },
            "geometry": {
                "type":        "Polygon",
                "coordinates": [meta["polygon"]],
            },
        })
    return out
