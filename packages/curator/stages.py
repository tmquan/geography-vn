"""Five curator stages: download → parse → extract → embed → reduce.

Every stage exposes the NeMo-Curator-compatible
``setup() / run() / teardown()`` lifecycle so the same code can be driven by
either the in-house sequential executor (default) or by a real
``nemo_curator.core.pipeline.Pipeline`` when ``--backend nemo_curator`` is
used. Each stage reads JSONL/Parquet from its input directory and writes its
own output directory; failures inside one stage never destroy earlier work.
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from packages.common.config import Config
from packages.common.http import HttpClient
from packages.common.logging import get_logger
from packages.common.paths import ensure_dir
from packages.curator.regions import province_to_region
from packages.scraper.normalise import normalise_name
from packages.scraper.sapnhap import (
    SapnhapClient,
    parse_vi_decimal,
    parse_vi_int,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers — geometry summary
# ---------------------------------------------------------------------------
def _safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def _summarise_geometry(fc: dict[str, Any] | None) -> dict[str, Any]:
    """Reduce a GeoJSON FeatureCollection to {bbox, centroid_lon, centroid_lat,
    geom_type, n_vertices, wkt}. We use shapely when available; otherwise we
    fall back to a ring-walk centroid that is good enough for plotting.
    """
    out: dict[str, Any] = {
        "bbox": None,
        "centroid_lon": None,
        "centroid_lat": None,
        "geom_type": None,
        "n_vertices": 0,
        "wkt": None,
    }
    if not fc or not isinstance(fc, dict):
        return out
    feats = fc.get("features") or []
    if not feats:
        return out
    geom = feats[0].get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    out["geom_type"] = gtype
    out["bbox"] = fc.get("bbox") or feats[0].get("bbox")

    try:
        from shapely.geometry import shape

        sh = shape(geom)
        c = sh.centroid
        out["centroid_lon"] = float(c.x)
        out["centroid_lat"] = float(c.y)
        out["wkt"] = sh.wkt
        # n_vertices = number of coordinate tuples in the linear rings
        out["n_vertices"] = _count_vertices(coords)
        return out
    except Exception:
        # Hand-rolled centroid for the rare environments without shapely.
        if gtype == "Point" and isinstance(coords, list) and len(coords) >= 2:
            out["centroid_lon"], out["centroid_lat"] = float(coords[0]), float(coords[1])
            out["n_vertices"] = 1
        else:
            xs, ys = _flatten_coords(coords)
            if xs and ys:
                out["centroid_lon"] = float(sum(xs) / len(xs))
                out["centroid_lat"] = float(sum(ys) / len(ys))
                out["n_vertices"] = len(xs)
        return out


def _count_vertices(coords: Any) -> int:
    if not isinstance(coords, list):
        return 0
    if coords and isinstance(coords[0], (int, float)):
        return 1
    return sum(_count_vertices(c) for c in coords)


def _flatten_coords(coords: Any) -> tuple[list[float], list[float]]:
    if not isinstance(coords, list):
        return [], []
    if coords and isinstance(coords[0], (int, float)) and len(coords) >= 2:
        return [float(coords[0])], [float(coords[1])]
    xs: list[float] = []
    ys: list[float] = []
    for c in coords:
        x, y = _flatten_coords(c)
        xs.extend(x)
        ys.extend(y)
    return xs, ys


# ---------------------------------------------------------------------------
# Stage 1 — download
# ---------------------------------------------------------------------------
@dataclass
class DownloadStage:
    """Walk the four POST endpoints exposed by ``sapnhap.bando.com.vn`` and
    persist the raw responses under ``out_dir``::

        raw/
        ├── admin_units.json          # one shot (3,355 rows)
        ├── committees.json           # one shot (3,357 rows)
        ├── details/
        │   └── diaphanhanhchinhcaptinh_sn.<n>.json    # 3,355 files
        ├── geom/
        │   ├── diaphanhanhchinhcaptinh_sn.<n>.geojson # 3,355 polygons
        │   └── uybannhandancapxa_2025.<n>.geojson     # 3,357 points
        └── _cache/                   # per-URL HTTP cache, re-runs are free

    Wall time on first crawl: ~12 minutes at the default 0.10 s delay.
    Subsequent runs are instantaneous (every URL is cached on disk).
    """

    config: Config
    out_dir: Path
    name: str = "download"

    def setup(self) -> None:
        self._cache_dir = ensure_dir(self.out_dir / "_cache")
        ensure_dir(self.out_dir / "details")
        ensure_dir(self.out_dir / "geom")

    def teardown(self) -> None:
        pass

    def run(self) -> dict[str, Any]:
        cfg = self.config
        with HttpClient(
            base_url=str(cfg.base_url),
            user_agent=str(cfg.user_agent),
            verify_ssl=bool(cfg.verify_ssl),
            timeout_s=float(cfg.request_timeout_s),
            retries=int(cfg.retries),
            retry_backoff_s=float(cfg.retry_backoff_s),
            delay_between_requests_s=float(cfg.delay_between_requests_s),
            cache_dir=self._cache_dir,
        ) as http:
            client = SapnhapClient(http=http)

            # 1) Listings — one POST each.
            admin_units = client.list_admin_units()
            (self.out_dir / "admin_units.json").write_text(
                json.dumps([a.__dict__ for a in admin_units], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            log.info("download: admin_units listing -> %d rows", len(admin_units))

            committees = client.list_committees()
            (self.out_dir / "committees.json").write_text(
                json.dumps([c.__dict__ for c in committees], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            log.info("download: committees listing  -> %d rows", len(committees))

            # 2) Per-unit detail (~3.4K calls).
            n_details = 0
            n_details_failed = 0
            if bool(cfg.get("fetch_details", True)):
                cap = int(cfg.get("max_admin_units") or 0)
                rows = admin_units[:cap] if cap > 0 else admin_units
                for i, unit in enumerate(rows, 1):
                    out_path = self.out_dir / "details" / f"{_safe_filename(unit.malk)}.json"
                    if out_path.exists():
                        n_details += 1
                        continue
                    detail = client.get_admin_unit_detail(unit.malk)
                    if detail is None:
                        n_details_failed += 1
                        continue
                    out_path.write_text(
                        json.dumps(detail, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    n_details += 1
                    if i % 200 == 0:
                        log.info("download: detail %d/%d (%s)", i, len(rows), unit.malk)
                log.info("download: details total=%d failed=%d", n_details, n_details_failed)

            # 3) GeoJSON polygons + points (~6.7K calls).
            n_geom = 0
            n_geom_empty = 0
            n_geom_failed = 0
            if bool(cfg.get("fetch_geometries", True)):
                cap_a = int(cfg.get("max_admin_units") or 0)
                cap_c = int(cfg.get("max_committees") or 0)
                ids: list[str] = []
                ids.extend(u.malk for u in (admin_units[:cap_a] if cap_a > 0 else admin_units))
                ids.extend(c.ma for c in (committees[:cap_c] if cap_c > 0 else committees))
                t0 = time.time()
                for i, fid in enumerate(ids, 1):
                    out_path = self.out_dir / "geom" / f"{_safe_filename(fid)}.geojson"
                    if out_path.exists():
                        n_geom += 1
                        continue
                    fc = client.get_geojson(fid)
                    if fc is None:
                        n_geom_empty += 1
                        # Persist a stub so we don't keep retrying.
                        out_path.write_text(
                            json.dumps({"type": "FeatureCollection", "features": []}),
                            encoding="utf-8",
                        )
                        continue
                    out_path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
                    n_geom += 1
                    if i % 200 == 0:
                        rate = i / max(time.time() - t0, 1e-3)
                        log.info("download: geom %d/%d (%.1f req/s)", i, len(ids), rate)
                log.info("download: geom total=%d empty=%d failed=%d",
                          n_geom, n_geom_empty, n_geom_failed)

        return {
            "n_admin_units": len(admin_units),
            "n_committees":  len(committees),
            "n_details":     n_details,
            "n_geom":        n_geom,
            "n_geom_empty":  n_geom_empty,
            "output":        str(self.out_dir),
        }


# ---------------------------------------------------------------------------
# Stage 2 — parse
# ---------------------------------------------------------------------------
@dataclass
class ParseStage:
    """Read every raw JSON, normalise Vietnamese number strings, summarise
    geometries, and emit one canonical record per entity to ``parsed.jsonl``
    plus a ``parsed.parquet`` mirror.

    Output schema (one row per entity)::

        id            str       "diaphanhanhchinhcaptinh_sn.108" (== malk)
        kind          str       "province" | "commune" | "committee"
        ma            str       NSO 2-digit code (provinces) or 5-char (communes)
        ten           str       canonical Vietnamese name
        type          str       "Tỉnh" | "Thành Phố" | "Phường" | "Xã" | "Đặc khu" | …
        ten_short     str       ten with the type prefix stripped
        area_km2      float?    parsed from "6.360,83"
        population    int?      parsed from "4.199.824"
        density       float?    population / area_km2
        capital       str?      trungtamhc (administrative centre)
        address       str?
        phone         str?
        decree        str?      cancu (decree of authority)
        decree_url    str?
        predecessors  str?      truocsapnhap (raw prose)
        parent_ma     str?      committees + communes carry their parent's NSO ma
        parent_ten    str?
        centroid_lon  float?
        centroid_lat  float?
        bbox          list?     [lon_min, lat_min, lon_max, lat_max]
        geom_type     str?      Polygon | MultiPolygon | Point | …
        wkt           str?      shapely WKT (only with parse.flatten_geojson=True)
    """

    config: Config
    in_dir: Path
    out_dir: Path
    name: str = "parse"

    _admin_lookup: dict[str, dict[str, Any]] = field(default_factory=dict)
    _committee_lookup: dict[str, dict[str, Any]] = field(default_factory=dict)

    def setup(self) -> None:
        ensure_dir(self.out_dir)
        admin_path = self.in_dir / "admin_units.json"
        committee_path = self.in_dir / "committees.json"
        if admin_path.exists():
            for r in json.loads(admin_path.read_text(encoding="utf-8")):
                self._admin_lookup[r["malk"]] = r
        if committee_path.exists():
            for r in json.loads(committee_path.read_text(encoding="utf-8")):
                self._committee_lookup[r["ma"]] = r

    def teardown(self) -> None:
        pass

    def run(self) -> dict[str, Any]:
        if not self._admin_lookup and not self._committee_lookup:
            self.setup()

        flatten = bool(self.config.get("flatten_geojson", True))
        out_jsonl = self.out_dir / "parsed.jsonl"
        out_parquet = self.out_dir / "parsed.parquet"
        rows: list[dict[str, Any]] = []

        # First, build a lookup from province ma -> ten so we can stamp
        # every commune/committee with its parent.
        province_by_ma: dict[str, str] = {}
        for malk, r in self._admin_lookup.items():
            if "captinh" in malk:
                province_by_ma[str(r.get("ma", ""))] = r.get("ten", "")
        # Also build commune-malk -> commune name for committee parents.
        commune_by_malk: dict[str, str] = {}
        commune_ma_by_malk: dict[str, str] = {}
        for malk, r in self._admin_lookup.items():
            if "capxa" in malk:
                commune_by_malk[malk] = r.get("ten", "")
                commune_ma_by_malk[malk] = str(r.get("ma", ""))

        # ----- admin units: 34 provinces + 3,321 communes ------------------
        for malk, listing in self._admin_lookup.items():
            kind = "province" if "captinh" in malk else "commune"
            detail_path = self.in_dir / "details" / f"{_safe_filename(malk)}.json"
            detail: dict[str, Any] = {}
            if detail_path.exists():
                try:
                    detail = json.loads(detail_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    detail = {}

            geom_path = self.in_dir / "geom" / f"{_safe_filename(malk)}.geojson"
            fc = None
            if geom_path.exists():
                try:
                    fc = json.loads(geom_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    fc = None
            geom = _summarise_geometry(fc)

            ten = normalise_name(listing.get("ten", ""))
            ttype, ten_short = _split_type_prefix(ten)
            area = parse_vi_decimal(detail.get("dientichkm2"))
            pop = parse_vi_int(detail.get("dansonguoi"))
            density = (pop / area) if (area and pop) else None

            # Parent-province for communes: ``tentinh`` field in detail.
            parent_ten = normalise_name(detail.get("tentinh") or "") or None
            parent_ma = None
            if parent_ten:
                for ma, name in province_by_ma.items():
                    if _norm(name) == _norm(parent_ten) or _norm(parent_ten) in _norm(name):
                        parent_ma = ma
                        break

            row = {
                "id":           malk,
                "kind":         kind,
                "ma":           str(listing.get("ma", "")),
                "ten":          ten,
                "type":         ttype,
                "ten_short":    ten_short,
                "area_km2":     area,
                "population":   pop,
                "density":      density,
                "capital":      _none_if_blank(detail.get("trungtamhc")),
                "address":      _none_if_blank(detail.get("diachi")),
                "phone":        _none_if_blank(detail.get("dthoai")),
                "decree":       _none_if_blank(detail.get("cancu")),
                "decree_url":   _none_if_blank(detail.get("link")),
                "predecessors": _none_if_blank(listing.get("truocsapnhap")),
                "parent_ma":    parent_ma,
                "parent_ten":   parent_ten,
                "centroid_lon": geom["centroid_lon"],
                "centroid_lat": geom["centroid_lat"],
                "bbox":         geom["bbox"],
                "geom_type":    geom["geom_type"],
                "n_vertices":   geom["n_vertices"],
                "wkt":          geom["wkt"] if flatten else None,
            }
            rows.append(row)

        # ----- committees: 3,357 commune-level point markers ---------------
        for cid, listing in self._committee_lookup.items():
            geom_path = self.in_dir / "geom" / f"{_safe_filename(cid)}.geojson"
            fc = None
            if geom_path.exists():
                try:
                    fc = json.loads(geom_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    fc = None
            geom = _summarise_geometry(fc)

            # ``pread_json`` returns the rich properties block — peel it.
            props: dict[str, Any] = {}
            if fc and fc.get("features"):
                props = fc["features"][0].get("properties") or {}

            ten = normalise_name(props.get("a01_ten") or listing.get("ten", ""))
            ttype, ten_short = _split_type_prefix(ten)
            area = parse_vi_decimal(props.get("a06_dt"))
            pop = parse_vi_int(props.get("a07_ds"))
            density = (pop / area) if (area and pop) else None
            parent_ten_xa = normalise_name(props.get("a03_tenxa") or "") or None
            parent_ten_tinh = normalise_name(props.get("a04_tentinh") or "") or None

            # Map parent-province name to its ma code.
            parent_ma = None
            for ma, name in province_by_ma.items():
                if parent_ten_tinh and (_norm(name) == _norm(parent_ten_tinh)
                                        or _norm(parent_ten_tinh) in _norm(name)):
                    parent_ma = ma
                    break

            row = {
                "id":           cid,
                "kind":         "committee",
                "ma":           "",
                "ten":          ten,
                "type":         ttype or "Ủy ban",
                "ten_short":    ten_short,
                "area_km2":     area,
                "population":   pop,
                "density":      density,
                "capital":      None,
                "address":      None,
                "phone":        _none_if_blank(props.get("a08_dthoai")),
                "decree":       None,
                "decree_url":   _none_if_blank(props.get("a09_web")),
                "predecessors": _none_if_blank(props.get("a02_gc")),
                "parent_ma":    parent_ma,
                "parent_ten":   parent_ten_tinh,
                "parent_ten_xa": parent_ten_xa,
                "centroid_lon": geom["centroid_lon"],
                "centroid_lat": geom["centroid_lat"],
                "bbox":         geom["bbox"],
                "geom_type":    geom["geom_type"],
                "n_vertices":   geom["n_vertices"],
                "wkt":          geom["wkt"] if flatten else None,
            }
            rows.append(row)

        # Persist.
        with out_jsonl.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        df = pd.DataFrame(rows)
        df.to_parquet(out_parquet, index=False)

        kinds = Counter(r["kind"] for r in rows)
        log.info("parse: %d rows (%s) -> %s",
                  len(rows), dict(kinds), out_jsonl.name)
        return {
            "n":      len(rows),
            "by_kind": dict(kinds),
            "output_jsonl": str(out_jsonl),
            "output_parquet": str(out_parquet),
        }


_TYPE_PREFIXES = (
    "Ủy ban nhân dân ",
    "Thủ đô ",
    "Thành phố ",
    "Đặc khu ",
    "Tỉnh ",
    "Phường ",
    "Xã ",
    "Thị trấn ",
)


def _split_type_prefix(name: str) -> tuple[str, str]:
    """('Thành Phố Cần Thơ', ...) → ('Thành phố', 'Cần Thơ').

    Type prefix is canonicalised to the standard Vietnamese capitalisation
    (only the first word capitalised) — the source API mixes ``Thành Phố``
    and ``Thành phố`` arbitrarily across endpoints.
    """
    if not name:
        return "", ""
    canon = normalise_name(name)
    for pref in _TYPE_PREFIXES:
        if canon.startswith(pref):
            return pref.strip(), canon[len(pref):].strip()
    return "", canon.strip()


def _norm(s: str) -> str:
    import unicodedata
    return unicodedata.normalize("NFC", str(s or "")).strip().lower()


def _none_if_blank(v: Any) -> Any:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"null", "none", "đang cập nhật"}:
        return None
    return s


# ---------------------------------------------------------------------------
# Stage 3 — extract
# ---------------------------------------------------------------------------
@dataclass
class ExtractStage:
    """Derive the analytical columns the analysis notebook + the visualizer
    need. Specifically:

    * ``macro_region`` — six GSO macro-regions (uses ``packages.curator.regions``).
    * ``parent_province`` for communes & committees (already attached in parse,
      but we re-validate against the post-merger 34-list).
    * ``predecessors_list`` — explode the ``truocsapnhap`` prose into a clean
      list of predecessor names (drop "phần còn lại của", "một phần", "TN", …).
    * ``n_predecessors`` — len(predecessors_list).
    * ``keywords`` — top-N TF-IDF unigrams + bigrams over ``predecessors``.
    * ``embed_text`` — single canonical descriptor used by the embed stage.
    """

    config: Config
    in_dir: Path
    out_dir: Path
    name: str = "extract"

    def setup(self) -> None:
        ensure_dir(self.out_dir)
        self._top_n = int(self.config.get("top_keywords", 8))

    def teardown(self) -> None:
        pass

    def run(self) -> dict[str, Any]:
        if not hasattr(self, "_top_n"):
            self.setup()
        in_path = self.in_dir / "parsed.jsonl"
        out_jsonl = self.out_dir / "extracted.jsonl"
        out_parquet = self.out_dir / "extracted.parquet"

        rows: list[dict[str, Any]] = []
        with in_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # 1) Macro-region — provinces map directly; communes /
                # committees map via parent_ten.
                if r["kind"] == "province":
                    region = province_to_region(r["ten"])
                else:
                    region = province_to_region(r.get("parent_ten") or "")
                r["macro_region"] = region

                # 2) Predecessor explode.
                pred = r.get("predecessors") or ""
                pred_list = _explode_predecessors(pred)
                r["predecessors_list"] = pred_list
                r["n_predecessors"] = len(pred_list)

                # 3) Canonical embed descriptor.
                r["embed_text"] = _build_embed_text(r)

                rows.append(r)

        # 4) TF-IDF keywords over predecessors prose.
        texts = [r.get("predecessors") or "" for r in rows]
        kws = _compute_keywords(texts, top_n=self._top_n,
                                ngram_range=tuple(self.config.get("ngram_range", [1, 2])),
                                max_df=float(self.config.get("max_df", 0.85)),
                                min_df=int(self.config.get("min_df", 2)),
                                kind=str(self.config.get("vectorizer", "tfidf")))
        for r, k in zip(rows, kws):
            r["keywords"] = k

        with out_jsonl.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        df = pd.DataFrame(rows)
        df.to_parquet(out_parquet, index=False)

        hist_kind = Counter(r["kind"] for r in rows)
        hist_region = Counter(r["macro_region"] for r in rows)
        log.info("extract: n=%d  kinds=%s  regions=%s",
                  len(rows), dict(hist_kind), dict(hist_region))
        return {
            "n": len(rows),
            "by_kind": dict(hist_kind),
            "by_region": dict(hist_region),
            "output_parquet": str(out_parquet),
        }


_PRED_DROP_PREFIXES = (
    "phần còn lại của ", "phần còn lại ",
    "một phần diện tích tn ", "một phần diện tích ", "một phần ",
    "phần lớn ",
    "khu vực ",
)


def _explode_predecessors(prose: str) -> list[str]:
    """Best-effort split of the ``truocsapnhap`` Vietnamese prose into a list
    of distinct predecessor names. Handles separators ``,``, ``và``,
    ``cùng``, ``;``, drops mereological qualifiers ("phần còn lại của",
    "một phần"), and trims ``sau khi sắp xếp`` clauses.
    """
    if not prose:
        return []
    s = prose
    s = re.split(r"\s+sau khi (?:sắp xếp|s\u1eafp x\u1ebfp).*$", s,
                  maxsplit=1, flags=re.IGNORECASE)[0]
    s = re.sub(r"\s+(?:và|cùng)\s+", ", ", s)
    s = re.sub(r";", ",", s)
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        pl = p.lower()
        for pref in _PRED_DROP_PREFIXES:
            if pl.startswith(pref):
                p = p[len(pref):]
                pl = p.lower()
        p = p.strip().rstrip(".")
        if not p:
            continue
        key = pl
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def _build_embed_text(r: dict[str, Any]) -> str:
    """Single canonical Vietnamese descriptor used as the embedding input."""
    parts: list[str] = []
    parts.append(r.get("ten") or "")
    if r.get("parent_ten"):
        parts.append(f"(thuộc {r['parent_ten']})")
    if r.get("type"):
        parts.append(f"loại: {r['type']}")
    if r.get("predecessors"):
        parts.append(f"sáp nhập từ: {r['predecessors']}")
    if r.get("capital"):
        parts.append(f"trung tâm hành chính: {r['capital']}")
    if r.get("decree"):
        parts.append(f"căn cứ: {r['decree']}")
    return " ; ".join(p for p in parts if p)


def _compute_keywords(texts: list[str], *, top_n: int, ngram_range: tuple,
                       max_df: float, min_df: int, kind: str) -> list[list[str]]:
    if not texts or kind != "tfidf":
        return [[] for _ in texts]
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
    except ImportError:
        log.warning("scikit-learn missing; skipping keyword extraction")
        return [[] for _ in texts]
    try:
        vec = TfidfVectorizer(
            max_df=max_df,
            min_df=min_df,
            ngram_range=ngram_range,
            token_pattern=r"(?u)\b[\wÀ-ỹ]{3,}\b",
        )
        matrix = vec.fit_transform(texts)
    except ValueError as exc:
        log.warning("TF-IDF skipped: %s", exc)
        return [[] for _ in texts]
    vocab = vec.get_feature_names_out()
    out: list[list[str]] = []
    for i in range(matrix.shape[0]):
        row = matrix.getrow(i).toarray()[0]
        if not row.any():
            out.append([])
            continue
        top = sorted(enumerate(row), key=lambda x: -x[1])[:top_n]
        out.append([str(vocab[j]) for j, w in top if w > 0])
    return out


# ---------------------------------------------------------------------------
# Stage 4 — embed
# ---------------------------------------------------------------------------
@dataclass
class EmbedStage:
    """Embed every record's ``embed_text`` field with a sentence-transformers
    model. Output is ``embedded.parquet`` carrying the meta columns + a
    ``vector`` column (list[float]).
    """

    config: Config
    in_dir: Path
    out_dir: Path
    name: str = "embed"

    def setup(self) -> None:
        ensure_dir(self.out_dir)
        self._batch_size = int(self.config.get("batch_size", 64))
        self._normalize = bool(self.config.get("normalize", True))
        self._text_field = str(self.config.get("text_field", "embed_text"))
        self._max_records = self.config.get("max_records")
        self._model_name = str(self.config.get("model"))
        self._device = str(self.config.get("device", "cpu"))
        self._model = None

    def teardown(self) -> None:
        self._model = None

    def run(self) -> dict[str, Any]:
        if not hasattr(self, "_batch_size"):
            self.setup()

        in_path = self.in_dir / "extracted.parquet"
        out_path = self.out_dir / "embedded.parquet"

        df = pd.read_parquet(in_path)
        if self._max_records:
            df = df.head(int(self._max_records)).copy()
        texts = df[self._text_field].fillna("").astype(str).tolist()

        from sentence_transformers import SentenceTransformer

        if self._model is None:
            self._model = SentenceTransformer(self._model_name, device=self._device)
        log.info("embed: encoding %d texts (%s) on %s",
                  len(texts), self._model_name, self._device)
        vectors = self._model.encode(
            texts,
            batch_size=self._batch_size,
            normalize_embeddings=self._normalize,
            convert_to_numpy=True,
            show_progress_bar=False,
        )

        out_df = df.copy()
        out_df["vector"] = list(vectors)
        out_df.to_parquet(out_path, index=False)
        log.info("embed: wrote %s (n=%d, dim=%d)",
                  out_path.name, len(out_df), vectors.shape[1])
        return {
            "n": len(out_df),
            "dim": int(vectors.shape[1]),
            "model": self._model_name,
            "output": str(out_path),
        }


# ---------------------------------------------------------------------------
# Stage 5 — reduce
# ---------------------------------------------------------------------------
@dataclass
class ReduceStage:
    """Project the embedding vectors down to 2-D for plotting and (optionally)
    run a density-based HDBSCAN cluster pass.

    Output is ``reduced.parquet`` carrying every meta column from the embed
    stage plus ``x``, ``y``, and (when ``reduce.cluster: true``) ``cluster``.
    """

    config: Config
    in_dir: Path
    out_dir: Path
    name: str = "reduce"

    def setup(self) -> None:
        ensure_dir(self.out_dir)

    def teardown(self) -> None:
        pass

    def run(self) -> dict[str, Any]:
        import numpy as np

        in_path = self.in_dir / "embedded.parquet"
        out_path = self.out_dir / "reduced.parquet"
        df = pd.read_parquet(in_path)
        if df.empty:
            log.warning("reduce: empty %s", in_path)
            df.assign(x=[], y=[]).to_parquet(out_path, index=False)
            return {"n": 0, "output": str(out_path)}

        X = np.stack(df["vector"].to_list())
        algo = str(self.config.get("algorithm", "umap")).lower()
        coords = self._project(X, algo)
        # Drop the bulky WKT geometry column too — the UMAP scatter does
        # not need it and it adds 20+ MB of redundant data to a parquet
        # already mirrored by parsed/extracted.parquet.
        drop_cols = [c for c in ("vector", "wkt") if c in df.columns]
        out = df.drop(columns=drop_cols).copy()
        out["x"] = coords[:, 0]
        out["y"] = coords[:, 1]

        do_cluster = bool(self.config.get("cluster", False))
        if do_cluster:
            out["cluster"] = self._cluster(X)
        out.to_parquet(out_path, index=False)
        log.info("reduce: wrote %s (n=%d, algo=%s, clustered=%s)",
                  out_path.name, len(out), algo, do_cluster)
        return {
            "n": len(out),
            "algorithm": algo,
            "clustered": do_cluster,
            "output": str(out_path),
        }

    def _project(self, X, algo: str):
        n = X.shape[0]
        if algo == "umap" and n >= 4:
            try:
                import umap

                reducer = umap.UMAP(
                    n_components=int(self.config.get("n_components", 2)),
                    n_neighbors=min(int(self.config.get("n_neighbors", 15)), max(2, n - 1)),
                    min_dist=float(self.config.get("min_dist", 0.1)),
                    metric=str(self.config.get("metric", "cosine")),
                    random_state=int(self.config.get("random_state", 42)),
                )
                return reducer.fit_transform(X)
            except Exception as exc:
                log.warning("UMAP failed (%s); falling back to PCA", exc)
        if algo == "tsne" and n >= 5:
            from sklearn.manifold import TSNE

            return TSNE(
                n_components=int(self.config.get("n_components", 2)),
                random_state=int(self.config.get("random_state", 42)),
                perplexity=min(30, max(5, n // 4)),
                init="pca",
            ).fit_transform(X)
        from sklearn.decomposition import PCA

        return PCA(n_components=int(self.config.get("n_components", 2))).fit_transform(X)

    @staticmethod
    def _cluster(X) -> list[int]:
        n = X.shape[0]
        if n < 4:
            return [0] * n
        try:
            from sklearn.cluster import HDBSCAN

            min_cluster_size = max(8, n // 80)
            min_samples = max(3, min_cluster_size // 4)
            return HDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                metric="euclidean",
                cluster_selection_method="eom",
            ).fit_predict(X).tolist()
        except Exception:
            return [0] * n
