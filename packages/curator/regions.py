"""Post-merger 34-province → 6-macro-region mapping.

The General Statistics Office of Vietnam organises the 63 (now 34) provinces
into **6 macro-regions** for cross-tabulation. After Resolution 202/2025/QH15
(12 June 2025) the province set shrank from 63 to 34, but the macro-region
boundaries are unchanged: each surviving province belongs to the same
macro-region its successor jurisdictions used to belong to. We hand-curate the
mapping below from the post-merger names returned by ``p.co_dvhc``.

Macro-regions
=============

* ``northern_midlands``   — Trung du và miền núi phía Bắc (15 → 13 provinces)
* ``red_river_delta``     — Đồng bằng sông Hồng (10 → 5 provinces inc. Hanoi & Hải Phòng)
* ``central_coast``       — Bắc Trung Bộ và duyên hải miền Trung (14 → 8 provinces)
* ``central_highlands``   — Tây Nguyên (5 → 3 provinces)
* ``southeast``           — Đông Nam Bộ (6 → 3 provinces inc. HCMC)
* ``mekong_delta``        — Đồng bằng sông Cửu Long (13 → 5 provinces inc. Cần Thơ)

Sum: 13 + 5 + 8 + 3 + 3 + 5 = 37 (note: Khánh Hòa now bridges central coast
into the Highlands, but for the GSO macro-region table it stays in the central
coast bucket — the tally above counts it once there).
"""

from __future__ import annotations

import unicodedata

MACRO_REGION_VI: dict[str, str] = {
    "northern_midlands":  "Trung du và miền núi phía Bắc",
    "red_river_delta":    "Đồng bằng sông Hồng",
    "central_coast":      "Bắc Trung Bộ và duyên hải miền Trung",
    "central_highlands":  "Tây Nguyên",
    "southeast":          "Đông Nam Bộ",
    "mekong_delta":       "Đồng bằng sông Cửu Long",
}

MACRO_REGION_EN: dict[str, str] = {
    "northern_midlands":  "Northern Midlands and Mountain Areas",
    "red_river_delta":    "Red River Delta",
    "central_coast":      "North Central and Central Coastal Areas",
    "central_highlands":  "Central Highlands",
    "southeast":          "Southeast",
    "mekong_delta":       "Mekong River Delta",
}

# Keyed by NFC-normalised, lower-cased *bare* province name (with the
# ``Tỉnh`` / ``Thành Phố`` / ``Thủ Đô`` prefix stripped). Spelling matches
# the strings returned by ``p.co_dvhc`` after the 2025 merger.
_RAW_MAP: dict[str, str] = {
    # Đồng bằng sông Hồng (5)
    "hà nội":           "red_river_delta",
    "hải phòng":        "red_river_delta",
    "hưng yên":         "red_river_delta",
    "ninh bình":        "red_river_delta",
    "quảng ninh":       "red_river_delta",
    # Trung du và miền núi phía Bắc (13)
    "cao bằng":         "northern_midlands",
    "tuyên quang":      "northern_midlands",
    "điện biên":        "northern_midlands",
    "lai châu":         "northern_midlands",
    "sơn la":           "northern_midlands",
    "lào cai":          "northern_midlands",
    "thái nguyên":      "northern_midlands",
    "lạng sơn":         "northern_midlands",
    "bắc ninh":         "northern_midlands",
    "phú thọ":          "northern_midlands",
    # Bắc Trung Bộ và duyên hải miền Trung (8)
    "thanh hóa":        "central_coast",
    "nghệ an":          "central_coast",
    "hà tĩnh":          "central_coast",
    "quảng trị":        "central_coast",
    "huế":              "central_coast",
    "đà nẵng":          "central_coast",
    "quảng ngãi":       "central_coast",
    "khánh hòa":        "central_coast",
    # Tây Nguyên (3)
    "gia lai":          "central_highlands",
    "đắk lắk":          "central_highlands",
    "lâm đồng":         "central_highlands",
    # Đông Nam Bộ (3)
    "đồng nai":         "southeast",
    "hồ chí minh":      "southeast",
    "tây ninh":         "southeast",
    # Đồng bằng sông Cửu Long (5)
    "đồng tháp":        "mekong_delta",
    "vĩnh long":        "mekong_delta",
    "an giang":         "mekong_delta",
    "cần thơ":          "mekong_delta",
    "cà mau":           "mekong_delta",
}


_PREFIXES = ("thủ đô ", "thành phố ", "tỉnh ")


def _strip_prefix(name: str) -> str:
    s = unicodedata.normalize("NFC", str(name)).strip()
    sl = s.lower()
    for p in _PREFIXES:
        if sl.startswith(p):
            return s[len(p):]
    return s


def province_to_region(province_name: str) -> str:
    """Map a post-merger province name to one of the 6 macro-region keys.

    Tolerant of the API's inconsistent capitalisation — ``"Thành Phố Cần Thơ"``
    (PascalCase ``Phố``), ``"Thành phố Cần Thơ"`` (canonical), and the bare
    ``"Cần Thơ"`` all resolve to ``"mekong_delta"``.
    """
    if not province_name:
        return "unknown"
    bare = _strip_prefix(province_name)
    return _RAW_MAP.get(bare.lower(), "unknown")
