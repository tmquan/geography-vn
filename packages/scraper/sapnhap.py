"""Client for the four POST endpoints exposed by ``sapnhap.bando.com.vn``.

The site is a thin PHP front-end (``D:\\map34tinh\\s.index.php``) over a QGIS
Server WMS/WFS deployment; the four endpoints all return JSON (with various
non-JSON ``Content-Type`` headers, hence the lenient parser in
:mod:`packages.common.http`).

Endpoint inventory
==================

==============================  =================================  ===========================================
Endpoint                        Form data                          Returns
==============================  =================================  ===========================================
``p.co_dvhc``                   ``ma=0``                           list[AdminUnit] — 34 provinces + 3,321 communes
``p.co_uyban``                  ``ma=0``                           list[Committee] — 3,357 commune people's committees
``p.co_dvhc_id``                ``malk=<feature_id>``              list[AdminUnitDetail] — full attributes (1 row)
``pread_json``                  ``id=<feature_id>``                GeoJSON FeatureCollection (polygon or point)
==============================  =================================  ===========================================

Feature ID conventions
----------------------

* ``diaphanhanhchinhcaptinh_sn.<1..132>``  — province polygons (post-merger).
  Only 34 of the 132 IDs survive the merger; ``p.co_dvhc`` lists exactly those.
* ``diaphanhanhchinhcapxa_2025.<1..3500>`` — commune polygons.
  3,321 survive; the rest are dissolved/skipped.
* ``uybannhandancapxa_2025.<1..3357>``    — commune-level people's-committee
  point markers (one per commune, plus duplicates for special administrative
  units like Đặc khu Phú Quốc).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from packages.common.http import HttpClient, HttpError
from packages.common.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Vietnamese number parsers
# ---------------------------------------------------------------------------
# The site formats numbers Vietnamese-style: ``.`` is the thousands separator
# and ``,`` is the decimal separator. ``"6.360,83"`` means 6,360.83.

# Mixed locale: ``p.co_dvhc_id`` uses Vietnamese formatting (``"6.360,83"`` ==
# 6,360.83) while the GeoJSON ``properties`` block uses English (``"575.29"``
# == 575.29 km², ``"157629"`` == 157,629 people). The parsers below cover
# both, biased by the expected field semantics.

_BLANK_TOKENS = {"", "đang cập nhật", "n/a", "null", "none"}


def parse_vi_decimal(value: str | None) -> float | None:
    """Parse a real-valued field. Handles three idioms:

    * Vietnamese comma-decimal:    ``"6.360,83"`` -> 6360.83
    * Vietnamese thousands:        ``"4.199.824"`` -> 4199824.0
    * English plain:               ``"575.29"`` -> 575.29
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in _BLANK_TOKENS:
        return None
    s = s.replace(" ", "")
    # Vietnamese unambiguous case: at least one comma -> comma is decimal.
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None
    # No comma. If it has 0 or 1 dot, treat the dot as a decimal separator
    # (English convention). If 2+ dots, they must be thousands separators.
    if s.count(".") >= 2:
        s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return None


def parse_vi_int(value: str | None) -> int | None:
    """Parse an integer field, stripping every Vietnamese / English thousand
    separator. ``"4.199.824"`` -> 4199824, ``"65.023"`` -> 65023,
    ``"157629"`` -> 157629, ``"đang cập nhật"`` -> None.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in _BLANK_TOKENS:
        return None
    # Drop every dot, comma, and whitespace — these can only ever be
    # separators in a population/area count (no real fractional people).
    s = s.replace(".", "").replace(",", "").replace(" ", "")
    try:
        return int(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Typed listings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AdminUnitListing:
    ma: str
    ten: str
    magoc: str
    malk: str               # ``diaphanhanhchinhcaptinh_sn.<n>`` or ``...capxa_2025.<n>``
    truocsapnhap: str       # human-readable lineage (Vietnamese prose)

    @property
    def level(self) -> str:
        return "province" if "captinh" in self.malk else "commune"

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AdminUnitListing:
        return cls(
            ma=str(d.get("ma", "")),
            ten=str(d.get("ten", "")),
            magoc=str(d.get("magoc", "")),
            malk=str(d.get("malk", "")),
            truocsapnhap=str(d.get("truocsapnhap", "") or ""),
        )


@dataclass(frozen=True)
class CommitteeListing:
    id: int
    ten: str
    ma: str                 # ``uybannhandancapxa_2025.<n>``

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CommitteeListing:
        return cls(
            id=int(d.get("id") or 0),
            ten=str(d.get("ten", "")),
            ma=str(d.get("ma", "")),
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
@dataclass
class SapnhapClient:
    """Thin typed wrapper over the four POST endpoints.

    Re-uses an :class:`HttpClient` (with on-disk cache + retries) for every
    request, so re-running the scraper is free.
    """

    http: HttpClient

    # ----- listings (one call each) ---------------------------------------
    def list_admin_units(self) -> list[AdminUnitListing]:
        resp = self.http.post_json("/p.co_dvhc", {"ma": "0"})
        rows = resp.json or []
        return [AdminUnitListing.from_dict(r) for r in rows]

    def list_committees(self) -> list[CommitteeListing]:
        resp = self.http.post_json("/p.co_uyban", {"ma": "0"})
        rows = resp.json or []
        return [CommitteeListing.from_dict(r) for r in rows]

    # ----- per-unit details -----------------------------------------------
    def get_admin_unit_detail(self, malk: str) -> dict[str, Any] | None:
        """Returns the single-row attributes for an admin unit by its
        ``malk`` (``diaphanhanhchinhcaptinh_sn.<n>`` or ``...capxa_2025.<n>``).
        """
        try:
            resp = self.http.post_json("/p.co_dvhc_id", {"malk": malk})
        except HttpError as exc:
            log.warning("p.co_dvhc_id failed for %s: %s", malk, exc)
            return None
        rows = resp.json or []
        return rows[0] if rows else None

    def get_geojson(self, feature_id: str) -> dict[str, Any] | None:
        """Returns a ``FeatureCollection`` for one ``diaphanhanhchinh*`` or
        ``uybannhandancapxa_*`` feature id. Returns ``None`` if the server
        returned the empty placeholder ``{"features": []}``.
        """
        try:
            resp = self.http.post_json("/pread_json", {"id": feature_id})
        except HttpError as exc:
            log.warning("pread_json failed for %s: %s", feature_id, exc)
            return None
        body = resp.json
        if not isinstance(body, dict):
            return None
        features = body.get("features") or []
        if not features:
            return None
        return body

    # ----- statistics overlay (optional) ----------------------------------
    def get_thongke(self, ma: str, cap: str) -> list[dict[str, Any]]:
        """Statistical overlay: count of {ck, moc, ca, hc, qs, kt, cho, th, bv}
        per admin unit (used in the live UI tooltip). ``cap`` ∈
        {``qg``, ``tinh``, ``huyen``, ``xa``}; the post-merger map only uses
        ``tinh`` and ``xa``.
        """
        try:
            resp = self.http.post_json("/pco_thongke", {"ma": ma, "cap": cap})
        except HttpError as exc:
            log.warning("pco_thongke failed for %s/%s: %s", cap, ma, exc)
            return []
        return resp.json or []
