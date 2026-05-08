"""Stage and (optionally) push the curated dataset to the HuggingFace Hub.

Modeled on ``personas-vn/scripts/upload_to_hf.py`` (which itself follows
ViLA's ``data/anle.toaan.gov.vn/_to_hf.py`` template — the NeMo-Curator
best-practice layout for a multi-stage dataset on the Hub).

Three steps:

1. **consolidate** — read every per-stage parquet under
   ``data/sapnhap-bando-vn/{parsed,extracted,reduced}`` and write a thin,
   typed bundle under ``data/sapnhap-bando-vn/_hf/``.
2. **render assets** — copy the figure pack from ``docs/figures/analysis``
   plus the analytical notebook + narrative docs.
3. **upload** — push every artefact to the right ``path_in_repo`` via
   ``HfApi.upload_folder`` with explicit ``delete_patterns`` (atomic refresh
   of stale shards in the same commit).

Hub layout pushed
-----------------
::

    .
    ├── README.md                       # dataset card (auto-generated)
    ├── _stats.json                     # numbers / tables the README quotes
    ├── data/
    │   ├── provinces.parquet            # 34 rows × 19 cols (with WKT geometry)
    │   ├── communes.parquet             # 3,321 rows
    │   ├── committees.parquet           # 3,357 rows
    │   └── all.parquet                  # union of the three above
    ├── geo/
    │   ├── provinces.geojson            # FeatureCollection of 34 polygons
    │   └── communes.geojson             # FeatureCollection of 3,321 polygons
    ├── reduced/
    │   └── reduced.parquet              # UMAP 2-D coords + cluster ids
    ├── notebooks/
    │   └── DATAANALYSIS.ipynb
    ├── docs/
    │   ├── DATAPROCESSING.md
    │   └── DATAANALYSIS.md
    ├── figures/
    │   └── analysis/{01..14}_*.{png,html}
    └── raw/
        ├── admin_units.json
        └── committees.json

Configs declared in the README YAML frontmatter::

    load_dataset("you/sapnhap-bando-vn", "all")          # 6,712 rows (default)
    load_dataset("you/sapnhap-bando-vn", "provinces")    #     34
    load_dataset("you/sapnhap-bando-vn", "communes")     #  3,321
    load_dataset("you/sapnhap-bando-vn", "committees")   #  3,357

CLI
---
::

    # Stage everything under data/sapnhap-bando-vn/_hf/, no upload
    python -m scripts.upload_to_hf --no-upload

    # Build + push (needs $HUGGINGFACE_HUB_TOKEN or `huggingface-cli login`)
    python -m scripts.upload_to_hf --repo tmquan/sapnhap-bando-vn

    # Skip the consolidate step (use existing _hf/data/)
    python -m scripts.upload_to_hf --skip-consolidate
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from packages.common.logging import get_logger
from packages.common.paths import REPO_ROOT, ensure_dir

log = get_logger(__name__)

DATASET_ROOT = REPO_ROOT / "data" / "sapnhap-bando-vn"
RAW_DIR = DATASET_ROOT / "raw"
PARSED_DIR = DATASET_ROOT / "parsed"
EXTRACTED_DIR = DATASET_ROOT / "extracted"
EMBEDDED_DIR = DATASET_ROOT / "embedded"
REDUCED_DIR = DATASET_ROOT / "reduced"

HF_DIR = DATASET_ROOT / "_hf"
HF_DATA_DIR = HF_DIR / "data"
HF_GEO_DIR = HF_DIR / "geo"
HF_REDUCED_DIR = HF_DIR / "reduced"
HF_FIGURES_ANALYSIS_DIR = HF_DIR / "figures" / "analysis"
HF_FIGURES_MAPS_DIR = HF_DIR / "figures" / "maps"
HF_NOTEBOOKS_DIR = HF_DIR / "notebooks"
HF_DOCS_DIR = HF_DIR / "docs"
HF_RAW_DIR = HF_DIR / "raw"

NOTEBOOKS = {
    "DATAANALYSIS.ipynb": REPO_ROOT / "DATAANALYSIS.ipynb",
}
NARRATIVE_DOCS = {
    "DATAPROCESSING.md": REPO_ROOT / "DATAPROCESSING.md",
    "DATAANALYSIS.md":   REPO_ROOT / "DATAANALYSIS.md",
}
FIGURE_SECTIONS: dict[str, Path] = {
    "analysis": REPO_ROOT / "docs" / "figures" / "analysis",
    "maps":     REPO_ROOT / "docs" / "figures" / "maps",
}

DEFAULT_REPO = "tmquan/sapnhap-bando-vn"


# ---------------------------------------------------------------------------
# Step 1 — consolidate
# ---------------------------------------------------------------------------
def consolidate() -> dict[str, Any]:
    log.info("[1/3] Consolidating curator artefacts into _hf/ …")
    extracted_pq = EXTRACTED_DIR / "extracted.parquet"
    if not extracted_pq.exists():
        raise FileNotFoundError(
            f"missing {extracted_pq}; run `geography-vn curate` first."
        )

    df = pd.read_parquet(extracted_pq)
    ensure_dir(HF_DATA_DIR)
    ensure_dir(HF_GEO_DIR)
    ensure_dir(HF_REDUCED_DIR)
    ensure_dir(HF_RAW_DIR)

    # --- per-kind splits (preserves WKT geom column) -----------------------
    for kind in ("province", "commune", "committee"):
        sub = df[df["kind"] == kind].copy()
        # ``predecessors_list`` is a list[str]; PyArrow handles it fine.
        out = HF_DATA_DIR / f"{kind}s.parquet"
        sub.to_parquet(out, compression="zstd", index=False)
        log.info("  wrote %s (%d rows)", out.relative_to(REPO_ROOT), len(sub))

    df.to_parquet(HF_DATA_DIR / "all.parquet",
                   compression="zstd", index=False)
    log.info("  wrote %s (%d rows)",
              (HF_DATA_DIR / "all.parquet").relative_to(REPO_ROOT), len(df))

    # --- assemble GeoJSON FeatureCollections per kind ----------------------
    n_geo = _build_geojson(df)
    log.info("  geojson: %d provinces, %d communes",
              n_geo["provinces"], n_geo["communes"])

    # --- copy reduced.parquet (UMAP) ---------------------------------------
    reduced_pq = REDUCED_DIR / "reduced.parquet"
    if reduced_pq.exists():
        shutil.copy2(reduced_pq, HF_REDUCED_DIR / "reduced.parquet")
        n_reduced = len(pd.read_parquet(HF_REDUCED_DIR / "reduced.parquet"))
        log.info("  copied reduced.parquet (%d rows)", n_reduced)
    else:
        n_reduced = 0
        log.warning("  no reduced.parquet found — skipping")

    # --- copy raw listings (small, useful for re-runs) --------------------
    for nm in ("admin_units.json", "committees.json"):
        src = RAW_DIR / nm
        if src.exists():
            shutil.copy2(src, HF_RAW_DIR / nm)

    # --- stats snapshot ----------------------------------------------------
    stats = build_stats(df, n_reduced=n_reduced, n_geo=n_geo)
    (HF_DIR / "_stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("  wrote %s", (HF_DIR / "_stats.json").relative_to(REPO_ROOT))

    write_readme(stats)
    log.info("  wrote %s", (HF_DIR / "README.md").relative_to(REPO_ROOT))

    copy_artefacts()
    return stats


def _build_geojson(df: pd.DataFrame) -> dict[str, int]:
    """Assemble two FeatureCollections (provinces + communes) from the WKT
    column emitted by ``parse.flatten_geojson``. Committees stay scattered
    points and ship via ``data/committees.parquet`` instead.
    """
    try:
        from shapely import wkt
        from shapely.geometry import mapping
    except ImportError:
        log.warning("shapely missing — skipping geojson assembly")
        return {"provinces": 0, "communes": 0}

    out: dict[str, int] = {}
    for kind in ("province", "commune"):
        sub = df[(df["kind"] == kind) & df["wkt"].notna()].copy()
        features: list[dict[str, Any]] = []
        for _, r in sub.iterrows():
            try:
                geom = wkt.loads(r["wkt"])
            except Exception:
                continue
            props = {
                "id":            r["id"],
                "ten":           r["ten"],
                "type":          r["type"],
                "ma":            r["ma"],
                "area_km2":      _scalar(r.get("area_km2")),
                "population":    _scalar(r.get("population")),
                "density":       _scalar(r.get("density")),
                "capital":       r.get("capital"),
                "decree":        r.get("decree"),
                "predecessors":  r.get("predecessors"),
                "macro_region":  r.get("macro_region"),
                "parent_ma":     r.get("parent_ma"),
                "parent_ten":    r.get("parent_ten"),
            }
            features.append({
                "type": "Feature",
                "properties": {k: v for k, v in props.items() if v is not None},
                "geometry": mapping(geom),
            })
        fc = {"type": "FeatureCollection", "features": features}
        out_path = HF_GEO_DIR / f"{kind}s.geojson"
        out_path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
        out[f"{kind}s"] = len(features)
    return out


def _scalar(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (v != v):  # NaN
        return None
    return v


def build_stats(df: pd.DataFrame, *, n_reduced: int, n_geo: dict[str, int]) -> dict[str, Any]:
    by_kind = df["kind"].value_counts().to_dict()
    by_region = (df.groupby(["macro_region", "kind"])
                  .size().reset_index(name="n")
                  .pivot(index="macro_region", columns="kind", values="n")
                  .fillna(0).astype(int).to_dict(orient="index"))
    provinces = df[df["kind"] == "province"]
    communes = df[df["kind"] == "commune"]
    pop_total = int(provinces["population"].dropna().sum())
    area_total = float(provinces["area_km2"].dropna().sum())
    return {
        "n_total":          len(df),
        "by_kind":          by_kind,
        "by_macro_region":  by_region,
        "n_provinces":      len(provinces),
        "n_communes":       len(communes),
        "n_committees":     int(by_kind.get("committee", 0)),
        "n_reduced":        n_reduced,
        "n_geo_polygons":   n_geo,
        "total_population": pop_total,
        "total_area_km2":   round(area_total, 2),
        "merger_fanout_max":
            int(df["n_predecessors"].dropna().astype(int).max() or 0),
    }


def copy_artefacts() -> None:
    """Sync the figure packs (analysis + maps), notebooks, and narrative
    docs into the HF staging tree.
    """
    for section, src_dir in FIGURE_SECTIONS.items():
        dst = HF_DIR / "figures" / section
        dst.mkdir(parents=True, exist_ok=True)
        if not src_dir.exists():
            continue
        for src in sorted(src_dir.iterdir()):
            if src.suffix.lower() in (".png", ".html"):
                shutil.copy2(src, dst / src.name)
    HF_NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)
    for name, src in NOTEBOOKS.items():
        if src.exists():
            shutil.copy2(src, HF_NOTEBOOKS_DIR / name)
    HF_DOCS_DIR.mkdir(parents=True, exist_ok=True)
    for name, src in NARRATIVE_DOCS.items():
        if src.exists():
            shutil.copy2(src, HF_DOCS_DIR / name)


def write_readme(stats: dict[str, Any]) -> None:
    """Emit ``_hf/README.md`` with HF dataset-card YAML frontmatter."""
    yaml_configs = """\
configs:
- config_name: all
  data_files:
  - split: train
    path: data/all.parquet
  default: true
- config_name: provinces
  data_files:
  - split: train
    path: data/provinces.parquet
- config_name: communes
  data_files:
  - split: train
    path: data/communes.parquet
- config_name: committees
  data_files:
  - split: train
    path: data/committees.parquet
"""

    body = f"""---
language:
- vi
- en
license: cc-by-nc-4.0
size_categories:
- 1K<n<10K
task_categories:
- tabular-classification
- tabular-regression
- text-classification
tags:
- vietnamese
- geography
- administrative-units
- post-merger-2025
- nq-202-2025-qh15
- gso
{yaml_configs}---

# sapnhap-bando-vn — Vietnam's 2025 administrative-merger atlas

A complete, table-by-table mirror of <https://sapnhap.bando.com.vn/>
— the official Vietnam Cartographic Publishing House atlas of the
**post-merger administrative units** introduced by **National Assembly
Resolution 202/2025/QH15 of 12 June 2025** and the **34 follow-up
Standing Committee resolutions of 16 June 2025**.

The merger collapsed Vietnam from **63 first-level units to 34** (28
provinces + 6 centrally-administered cities) and re-drew the second
tier from **705 districts ÷ 10,599 communes** down to **3,321 communes /
wards / special administrative units**. This dataset captures every
surviving entity together with its merger lineage, area, population,
administrative centre, decree of authority, and a polygon (or point)
geometry.

## At a glance

| Stat                                  | Value             |
| ------------------------------------- | ----------------- |
| First-level admin units (post-merger) | **{stats['n_provinces']}**            |
| Second-level admin units (post-merger)| **{stats['n_communes']:,}**         |
| People's-committee headquarters       | **{stats['n_committees']:,}**         |
| Total Vietnam population (2024)       | **{stats['total_population']:,}**       |
| Total Vietnam land area (km²)         | **{stats['total_area_km2']:,}**       |
| Province polygons (GeoJSON)           | **{stats['n_geo_polygons'].get('provinces', 0)}** |
| Commune polygons (GeoJSON)            | **{stats['n_geo_polygons'].get('communes', 0):,}** |
| Max merger fanout (predecessor units) | **{stats['merger_fanout_max']}**            |

## What's on the Hub

```
.
├── README.md                  (this file)
├── _stats.json                numbers / tables this card quotes
├── data/
│   ├── all.parquet            6,712 rows = provinces + communes + committees
│   ├── provinces.parquet      34 rows
│   ├── communes.parquet       3,321 rows
│   └── committees.parquet     3,357 rows
├── geo/
│   ├── provinces.geojson      34 polygons (FeatureCollection)
│   └── communes.geojson       3,321 polygons (FeatureCollection)
├── reduced/
│   └── reduced.parquet        UMAP 2-D coords + HDBSCAN cluster id
├── figures/analysis/          14 PNG + 14 HTML interactive Plotly figures
├── notebooks/DATAANALYSIS.ipynb
├── docs/{{DATAPROCESSING,DATAANALYSIS}}.md
└── raw/{{admin_units,committees}}.json   (source listings)
```

Every figure under ``figures/`` ships in **both formats** — the static
PNG for inline rendering in this dataset card, plus a self-contained
**interactive HTML** with the full Plotly toolkit (pan / zoom / hover
tooltips). Click any HTML link or download the ``.html`` to open it
locally in a browser.

## Per-macro-region inventory

| Macro-region (EN)                          | Provinces | Communes | Committees |
| ------------------------------------------ | --------: | -------: | ---------: |
""" + _render_region_table(stats["by_macro_region"]) + """

## Curator pipeline

The on-disk shape under this repo is produced by the same five-stage
NeMo Curator pipeline as `personas-vn`:

```
download → parse → extract → embed → reduce
```

* **download** — POST to four endpoints exposed by ``sapnhap.bando.com.vn``
  (``p.co_dvhc``, ``p.co_uyban``, ``p.co_dvhc_id``, ``pread_json``);
  ~6,700 calls; cached to disk so re-runs are free.
* **parse** — normalise Vietnamese-formatted numbers (``"4.199.824"`` →
  4 199 824), summarise GeoJSON to centroid + bbox + WKT, attach
  parent-province for every commune & committee.
* **extract** — TF-IDF keyword extraction over the merger-lineage
  prose; macro-region attachment from the curated 34 → 6 GSO mapping.
* **embed** — ``sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2``
  on CPU (384-d) over the canonical Vietnamese descriptors.
* **reduce** — UMAP → 2-D coords + density-based HDBSCAN clusters.

See [`docs/DATAPROCESSING.md`](docs/DATAPROCESSING.md) for the full
crawl protocol, [`docs/DATAANALYSIS.md`](docs/DATAANALYSIS.md) for the
analytical walkthrough, and the [`notebooks/DATAANALYSIS.ipynb`](notebooks/DATAANALYSIS.ipynb)
notebook for the executed Plotly outputs.

## Usage

```python
from datasets import load_dataset

# Default config (all 6,712 rows)
ds = load_dataset("REPO_ID")["train"]
print(ds.column_names)
# -> ['id', 'kind', 'ma', 'ten', 'type', 'ten_short', 'area_km2',
#     'population', 'density', 'capital', 'address', 'phone', 'decree',
#     'decree_url', 'predecessors', 'parent_ma', 'parent_ten',
#     'centroid_lon', 'centroid_lat', 'bbox', 'geom_type', 'wkt',
#     'predecessors_list', 'n_predecessors', 'macro_region',
#     'embed_text', 'keywords']

# Just the 34 first-level units
provinces = load_dataset("REPO_ID", "provinces")["train"]

# GeoJSON (download separately — datasets doesn't load .geojson)
import huggingface_hub as hf, json
path = hf.hf_hub_download("REPO_ID", "geo/provinces.geojson",
                           repo_type="dataset")
fc = json.loads(open(path, "r", encoding="utf-8").read())
print(len(fc["features"]))
```

## Citation

The underlying data belongs to the **Ministry of Agriculture and
Environment** and the **Vietnam Cartographic Publishing House**
(Nhà Xuất Bản Tài Nguyên - Môi Trường và Bản Đồ Việt Nam):

* ISBN: 978-632-622-303-3
* Publication ID: 1027-2026/CXBIPH/03-129/BĐ
* Published: 2026 (Quyết định số 30/QĐ-NXBTNMT, 16 April 2026)
* Source: <https://sapnhap.bando.com.vn/> · <https://bando.com.vn>

Authoritative legal sources for the merger:

* National Assembly Resolution 202/2025/QH15 (12 June 2025)
* Standing Committee resolutions of 16 June 2025 (×34)
* Government decrees published at <https://vanban.chinhphu.vn>

If this mirror or the analytical figures are useful in academic work,
please credit the publishers above.
""".strip()

    (HF_DIR / "README.md").write_text(body, encoding="utf-8")


def _render_region_table(by_region: dict[str, dict[str, int]]) -> str:
    from packages.curator.regions import MACRO_REGION_EN
    rows: list[str] = []
    for key in MACRO_REGION_EN:
        d = by_region.get(key, {})
        rows.append(
            f"| {MACRO_REGION_EN[key]:42s} | {d.get('province', 0):9d} | "
            f"{d.get('commune', 0):8,d} | {d.get('committee', 0):10,d} |"
        )
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# Step 2 — render assets (notebooks + figures)
# ---------------------------------------------------------------------------
def render_assets() -> None:
    log.info("[2/3] Verifying notebook + figure inventory …")
    for section, src_dir in FIGURE_SECTIONS.items():
        if src_dir.exists():
            n_png = len(list(src_dir.glob("*.png")))
            n_html = len(list(src_dir.glob("*.html")))
            log.info("  %s: %d PNGs, %d HTMLs", section, n_png, n_html)
        else:
            log.warning("  no %s — run the corresponding renderer first",
                          src_dir.relative_to(REPO_ROOT))
    copy_artefacts()


# ---------------------------------------------------------------------------
# Step 3 — upload
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Upload:
    local: Path
    in_repo: str
    delete: tuple[str, ...] | None = None
    message: str = ""


def _uploads() -> tuple[Upload, ...]:
    return (
        Upload(local=HF_DIR / "README.md", in_repo="README.md",
                message="Refresh dataset card"),
        Upload(local=HF_DIR / "_stats.json", in_repo="_stats.json",
                message="Refresh stats snapshot"),
        Upload(local=HF_DATA_DIR, in_repo="data",
                delete=("*.parquet",),
                message="Refresh per-kind parquet bundle"),
        Upload(local=HF_GEO_DIR, in_repo="geo",
                delete=("*.geojson",),
                message="Refresh GeoJSON FeatureCollections"),
        Upload(local=HF_REDUCED_DIR, in_repo="reduced",
                delete=("*.parquet",),
                message="Refresh UMAP-reduced parquet"),
        Upload(local=HF_NOTEBOOKS_DIR, in_repo="notebooks",
                delete=("*.ipynb",),
                message="Refresh DATAANALYSIS notebook"),
        Upload(local=HF_DOCS_DIR, in_repo="docs",
                delete=("*.md",),
                message="Refresh narrative docs"),
        Upload(local=HF_FIGURES_ANALYSIS_DIR, in_repo="figures/analysis",
                delete=("*.png", "*.html"),
                message="Refresh analytical figure pack"),
        Upload(local=HF_FIGURES_MAPS_DIR, in_repo="figures/maps",
                delete=("*.png", "*.html"),
                message="Refresh cartographic figure pack (NVIDIA + dual archipelagos)"),
        Upload(local=HF_RAW_DIR, in_repo="raw",
                message="Refresh raw listings"),
    )


def upload(repo: str) -> None:
    from huggingface_hub import HfApi, create_repo

    log.info("[3/3] Uploading to %s …", repo)
    os.environ.setdefault("HF_HUB_ENABLE_HF_TRANSFER", "1")

    api = HfApi()
    create_repo(repo, repo_type="dataset", exist_ok=True)
    log.info("  repo ready: https://huggingface.co/datasets/%s", repo)

    for u in _uploads():
        if not u.local.exists():
            log.warning("  skip %s (not found)", u.local)
            continue
        log.info("  push %s -> %s", u.local.name, u.in_repo)
        if u.local.is_file():
            api.upload_file(
                path_or_fileobj=str(u.local),
                path_in_repo=u.in_repo,
                repo_id=repo,
                repo_type="dataset",
                commit_message=u.message or f"Refresh {u.in_repo}",
            )
        else:
            api.upload_folder(
                folder_path=str(u.local),
                path_in_repo=u.in_repo,
                repo_id=repo,
                repo_type="dataset",
                delete_patterns=list(u.delete) if u.delete else None,
                commit_message=u.message or f"Refresh {u.in_repo}",
            )
    log.info("  done. https://huggingface.co/datasets/%s", repo)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(description="Build + upload the geography-vn dataset.")
    p.add_argument("--repo", default=DEFAULT_REPO,
                   help=f"HF repo id (default: {DEFAULT_REPO})")
    p.add_argument("--skip-consolidate", action="store_true",
                   help="skip the parquet roll-up (use existing _hf/data/)")
    p.add_argument("--skip-assets", action="store_true",
                   help="skip refreshing the figure bundle")
    p.add_argument("--no-upload", action="store_true",
                   help="build the bundle locally but do not push")
    args = p.parse_args()

    if not args.skip_consolidate:
        consolidate()
    if not args.skip_assets:
        render_assets()
    if not args.no_upload:
        upload(args.repo)
    log.info("All done.")


if __name__ == "__main__":
    main()
