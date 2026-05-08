# geography-vn

Vietnam's **post-merger administrative-units atlas** packaged as a
**5-stage NeMo Curator pipeline**: scrape <https://sapnhap.bando.com.vn/>,
publish to HuggingFace, and ship a deep analytical notebook.

The site is the **Bộ Nông nghiệp và Môi trường / Nhà Xuất Bản Tài Nguyên -
Môi Trường và Bản Đồ Việt Nam** electronic atlas of every administrative
unit produced by **Resolution 202/2025/QH15 (12 June 2025)** — the law
that collapsed Vietnam from **63 first-level units to 34** (28 provinces +
6 centrally-administered cities) and re-drew the second tier from 705
districts × 10,599 communes down to **3,321 communes / wards / special
administrative units**.

```
sapnhap.bando.com.vn ──┐
                       │ stage 1: download  (~6.7K POSTs to 4 endpoints)
                       ▼
            data/sapnhap-bando-vn/raw/{admin_units,committees,details/*,geom/*}
                       │ stage 2: parse     (HTML→text, vi-num parse, geom summary)
                       ▼
            data/sapnhap-bando-vn/parsed/parsed.{jsonl,parquet}
                       │ stage 3: extract   (TF-IDF, macro-region, predecessors)
                       ▼
            data/sapnhap-bando-vn/extracted/extracted.{jsonl,parquet}
                       │ stage 4: embed     (SBERT 384-d on the embed_text column)
                       ▼
            data/sapnhap-bando-vn/embedded/embedded.parquet
                       │ stage 5: reduce    (UMAP 2-D + HDBSCAN cluster)
                       ▼
            data/sapnhap-bando-vn/reduced/reduced.parquet
                       │ scripts/upload_to_hf.py + scripts/analyze.py
                       ▼
        HuggingFace dataset            +    14 PNG/HTML figure pack
        (per-kind parquets,                  + DATAANALYSIS notebook
         GeoJSON polygons,                   + DATAANALYSIS.md walkthrough
         README dataset card)
```

The package layout mirrors the philosophy of `personas-vn`'s curator
([`packages/curator/`](https://github.com/tmquan/personas-vn/tree/main/packages/curator))
and ViLA's
[`packages/datasites/anle/`](https://github.com/tmquan/ViLA/tree/main/packages/datasites/anle):
flat namespace packages under `packages/`, runnable scripts under
`scripts/`, runnable apps under `apps/`. The five stage classes implement
the same `setup() / run() / teardown()` lifecycle as
`nemo_curator.core.stage.ProcessingStage`, so the same code drives both
the in-house sequential executor (default) and a real
`nemo_curator.core.pipeline.Pipeline` running through `InProcessExecutor`,
`XennaExecutor`, or `RayDataExecutor`.

Long-form pedagogical guides:

* [`DATAPROCESSING.md`](DATAPROCESSING.md) — the five-stage curator
  pipeline (download → parse → extract → embed → reduce), endpoint by
  endpoint, with the source-side data model and the on-disk shape it
  produces.
* [`DATAANALYSIS.md`](DATAANALYSIS.md) — what the curated data actually
  says: which 34 provinces survived, the merger fanout per surviving
  unit, the 6-region macro-inventory, the urban / rural population split,
  and a UMAP semantic map of the 6,712 entities (provinces + communes +
  committees).
* [`DATAANALYSIS.ipynb`](DATAANALYSIS.ipynb) — the executed notebook with
  every figure and quantitative summary inline.

## Architecture

The repo layout mirrors `personas-vn`'s philosophy: a flat `packages/`
namespace, runnable scripts under `scripts/`, OmegaConf-driven YAML
configs under `configs/`. Adds `packages/viz/` for the Vietnamese
geographic helpers + NVIDIA + LaTeX-serif Plotly theme.

```
geography-vn/
├── pyproject.toml
├── configs/
│   └── curator.yaml             # OmegaConf YAML (${var} interpolation)
├── packages/
│   ├── common/                  # config (OmegaConf), HTTP client (cache + retries),
│   │                            #   logging, path helpers
│   ├── scraper/                 # SapnhapClient, vi-locale parsers,
│   │                            #   normalise.py (Thành Phố → Thành phố etc.)
│   ├── curator/                 # 5 ProcessingStage-compatible stages +
│   │                            #   34 → 6 macro-region mapping
│   ├── viz/                     # NVIDIA + LaTeX-serif Plotly theme +
│   │                            #   Vietnamese geographic helpers:
│   │                            #     archipelago.py — Hoàng Sa / Trường Sa
│   │                            #     vietnam_geo.py — load_vietnam_geojson()
│   │                            #     style.py     — apply_nvidia_latex_style + save_figure
│   └── pipeline/                # CLI entry point (`geography-vn curate`)
├── scripts/
│   ├── upload_to_hf.py          # consolidate → render → push HF dataset
│   ├── analyze.py               # render the 14-figure analytical pack
│   └── render_maps.py           # render the 5-figure cartographic pack
│                                # (NVIDIA-green choropleths + dual archipelagos)
├── data/                        # gitignored
│   └── sapnhap-bando-vn/
│       ├── raw/                 # download stage (cached on disk; ~120 MB)
│       ├── parsed/              # parse stage (parsed.{jsonl,parquet})
│       ├── extracted/           # extract stage (with macro_region etc.)
│       ├── embedded/            # embed stage (384-d vectors)
│       ├── reduced/             # reduce stage (UMAP x/y + cluster)
│       └── _hf/                 # HF bundle staged by scripts/upload_to_hf.py
├── docs/figures/
│   ├── analysis/                # 14 PNG + 14 HTML pairs (analyze.py)
│   └── maps/                    # 5 PNG + 5 HTML pairs  (render_maps.py)
└── tests/                       # 45 tests (parsers, normaliser, region map,
                                 #            type-prefix split, e2e fixture)
```

### Visual style

All figures share a single Plotly template (`nvidia_latex`) registered by
`packages.viz.style.register_plotly_template`:

* **White canvas** (`#FFFFFF` paper + plot bg) — never off-white, never grey.
* **NVIDIA Green** primary (`#76B900`), with a curated 6-region categorical
  palette + sequential `NV_SEQUENTIAL` for choropleths.
* **LaTeX-style serif typography** — `Latin Modern Roman → LM Roman 10 →
  Computer Modern → CMU Serif → STIX Two Text → Times New Roman → Times`.
  Renders as Computer-Modern when the viewer has a TeX install,
  gracefully falls back to Times otherwise.

The cartographic pack additionally declares both **offshore archipelagos**
(Hoàng Sa + Trường Sa) as dashed bounding outlines with bilingual labels
+ principal-island markers, mirroring the standard Vietnamese-atlas
convention used by `personas-vn`'s `scripts/render_maps.py`.

## Source surface

The site exposes four POST endpoints — every one returns JSON, even when
the server lies in its `Content-Type` header:

| Endpoint                | Form data                       | Returns                                              |
| ----------------------- | ------------------------------- | ---------------------------------------------------- |
| `POST /p.co_dvhc`        | `ma=0`                           | List of every admin unit (34 provinces + 3,321 communes) |
| `POST /p.co_uyban`       | `ma=0`                           | List of 3,357 commune people's-committee headquarters |
| `POST /p.co_dvhc_id`     | `malk=<feature_id>`              | Full attribute row (area, population, decree, …)     |
| `POST /pread_json`       | `id=<feature_id>`                | GeoJSON FeatureCollection (Polygon / MultiPolygon / Point) |

Feature-id conventions:

* `diaphanhanhchinhcaptinh_sn.<n>`   — province polygons (only 34 of the
  132 pre-merger ids survive).
* `diaphanhanhchinhcapxa_2025.<n>`  — commune polygons; 3,321 alive.
* `uybannhandancapxa_2025.<n>`     — point markers for every commune
  people's committee.

Total crawl footprint: ~6,700 small JSON / GeoJSON files; ~120 MB on disk.

## Quickstart

> **zsh tip:** the snippets below contain inline `#` comments. zsh ignores
> them only after `setopt interactivecomments`.

**1. Install** (Python ≥ 3.10, conda env named `vn`):

```bash
conda create -n vn python=3.11 -y
conda activate vn
pip install -e ".[curator,viz,hf,dev]"
```

The `[curator]` extra pulls torch, sentence-transformers, umap-learn,
and scikit-learn (~3 GB). `[viz]` adds plotly + matplotlib + kaleido.
`[hf]` adds huggingface_hub + datasets. `[dev]` adds pytest + ruff.

**2. Run the full curator pipeline** (~50 min on first run; cached
afterwards so re-runs are seconds):

```bash
geography-vn curate                                    # all 5 stages
geography-vn curate --only download                    # just the crawl
geography-vn curate --skip download                    # parse → reduce
geography-vn curate --backend nemo_curator             # NeMo Curator executor
geography-vn curate --config configs/curator.yaml \    # OmegaConf merge
                    configs/local.yaml                  # of multiple configs
```

Approximate wall times on an M-series Mac:

| Step                                           | Records              | Wall time |
| ---------------------------------------------- | -------------------- | --------- |
| `curate --only download` (full crawl)          | 6,712 raw            | ~50 min   |
| `curate --skip download`                       | 6,712 → 6,712 emb.   | ~2 min    |
| `geography-vn curate` (cached re-run)          | 6,712                | ~3 min    |

**3. Render figure packs** (writes to `docs/figures/{analysis,maps}/`):

```bash
python -m scripts.analyze       # 14 analytical figures (bar / scatter / UMAP)
python -m scripts.render_maps   # 5 cartographic figures + dual archipelagos
```

**4. Stage and push the HF dataset:**

```bash
# Stage everything under data/sapnhap-bando-vn/_hf/, no upload
python -m scripts.upload_to_hf --no-upload

# Build + push (needs HUGGINGFACE_HUB_TOKEN or `huggingface-cli login`)
python -m scripts.upload_to_hf --repo <your-org>/sapnhap-bando-vn
```

**5. Read the analytical walkthrough:**

```bash
jupyter notebook DATAANALYSIS.ipynb
```

…or open the rendered version inline in the HuggingFace dataset card.

## Configuration

One YAML file, [`configs/curator.yaml`](configs/curator.yaml). Every
stage has its own block:

* `download` — base URL, polite delay (0.10 s default = ~10 req/s),
  caps for partial crawls (`max_admin_units`, `max_committees`),
  feature-flags for the geometry walk and the per-unit detail walk.
* `parse` — minimum text length, `flatten_geojson` toggle for the WKT
  column.
* `extract` — TF-IDF parameters (ngram range, max_df, min_df,
  top_keywords).
* `embed` — model name (auto-routes between local SBERT and NIM hosted
  embeddings), batch size, max_records cap.
* `reduce` — UMAP parameters, optional `cluster: true` for HDBSCAN.

## Data model

A single canonical row per entity (provinces + communes + committees) —
identical schema across all three kinds, so they share `data/all.parquet`
on the Hub:

| column                | example                                | notes                                            |
| --------------------- | -------------------------------------- | ------------------------------------------------ |
| `id`                   | `diaphanhanhchinhcaptinh_sn.108`       | feature id                                       |
| `kind`                 | `province` / `commune` / `committee`   |                                                   |
| `ma`                   | `01` / `00004`                         | NSO 2-digit (province) or 5-char (commune) code |
| `ten`                  | `Thủ Đô Hà Nội`                        | canonical Vietnamese name                        |
| `type`                 | `Tỉnh` / `Thành Phố` / `Phường` / …    | type prefix                                      |
| `area_km2`             | `3358.6`                                | parsed via `parse_vi_decimal`                    |
| `population`           | `8587000`                               | parsed via `parse_vi_int`                        |
| `density`              | `2557.4`                                | `population / area_km2`                          |
| `capital`              | `Hà Nội`                                | `trungtamhc`                                     |
| `decree`               | `Nghị quyết số 202/2025/QH15`           | `cancu`                                          |
| `decree_url`           | `https://vanban.chinhphu.vn/?…`         |                                                   |
| `predecessors`         | `Hà Nội cũ, một phần Hà Tây`            | merger lineage prose                             |
| `predecessors_list`    | `["Hà Nội cũ", "Hà Tây"]`               | exploded list                                    |
| `n_predecessors`       | `2`                                     |                                                   |
| `parent_ma`            | `01` (for communes & committees)       | NSO code of the parent province                  |
| `parent_ten`           | `Thủ đô Hà Nội`                         |                                                   |
| `centroid_lon` / `lat` | `105.85, 21.02`                          | from the GeoJSON geometry                        |
| `bbox`                 | `[105.7, 20.7, 106.1, 21.4]`            |                                                   |
| `geom_type`            | `MultiPolygon`                          |                                                   |
| `wkt`                  | `MULTIPOLYGON((…))`                     | when `parse.flatten_geojson=true`                |
| `macro_region`         | `red_river_delta`                       | one of 6 GSO macro-regions                       |
| `embed_text`           | (composed Vietnamese descriptor)        | what the embed stage encodes                     |
| `keywords`             | `["sáp nhập", "phường ba đình", …]`     | top-N TF-IDF terms                                |

## NeMo Curator backend

Pass `--backend nemo_curator` to `geography-vn curate` and the same five
stage objects are wrapped as `nemo_curator.core.stage.ProcessingStage`
sub-classes and run through
`nemo_curator.backends.experimental.in_process.InProcessExecutor`. The
on-disk shape is identical, so the rest of the pipeline (HF upload,
analysis notebook) does not care which executor ran. To go distributed,
swap `InProcessExecutor` for `XennaExecutor` / `RayDataExecutor`.

## Testing

```bash
pip install -e ".[dev]"
python -m pytest -q
```

23 tests cover:

* **Vietnamese / English number parsers** — `parse_vi_decimal` and
  `parse_vi_int` against the actual idioms the API returns.
* **Listing classification** — `AdminUnitListing.level` for provinces vs
  communes; `CommitteeListing.from_dict` for the people's-committee tier.
* **Predecessor explode** — strips mereological qualifiers ("phần còn
  lại của", "một phần") and trims trailing "sau khi sắp xếp" clauses.
* **Geometry summary** — point centroids, polygon centroids, bbox.
* **End-to-end parse + extract** against a hand-built fixture, asserting
  the macro-region attachment for every kind.
* **34-province → 6-region mapping** — spot-check across all six GSO
  macro-regions.

No tests touch the network — the offline fixture exercises every code path.

## Disclaimer

The underlying data belongs to the **Ministry of Agriculture and
Environment** and the **Vietnam Cartographic Publishing House** (Nhà
Xuất Bản Tài Nguyên - Môi Trường và Bản Đồ Việt Nam):

* ISBN: 978-632-622-303-3
* Publication ID: 1027-2026/CXBIPH/03-129/BĐ
* Source: <https://sapnhap.bando.com.vn/> · <https://bando.com.vn>

Authoritative legal sources for the merger:

* National Assembly Resolution **202/2025/QH15** (12 June 2025)
* Standing Committee resolutions of 16 June 2025 (×34)
* Government decrees published at <https://vanban.chinhphu.vn>

When using this mirror, please credit the publishers above and respect
the source license terms.
