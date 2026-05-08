# DATAPROCESSING — five-stage curator pipeline

This document walks through the five-stage NeMo-Curator-compatible pipeline
that turns the raw web responses from <https://sapnhap.bando.com.vn/> into
the typed, joinable, embedding-augmented parquet bundle that ships to
HuggingFace and feeds [`DATAANALYSIS.md`](DATAANALYSIS.md).

The five stages mirror the same layout used by ViLA's
[`packages/datasites/anle/`](https://github.com/tmquan/ViLA/tree/main/packages/datasites/anle)
and `personas-vn`'s
[`packages/curator/`](https://github.com/tmquan/personas-vn/tree/main/packages/curator):

```
download → parse → extract → embed → reduce
```

Each stage reads from the previous stage's output directory and writes to its
own. A failure inside one stage never destroys earlier work, and re-running a
single stage in isolation (`--only embed`, `--skip download`, …) is the
intended development loop.

```
data/sapnhap-bando-vn/
├── raw/         ← stage 1
│   ├── admin_units.json          (one POST: 3,355 listings)
│   ├── committees.json           (one POST: 3,357 committee markers)
│   ├── details/<malk>.json       (~3,355 POSTs to p.co_dvhc_id)
│   ├── geom/<id>.geojson         (~6,712 POSTs to pread_json)
│   └── _cache/                   (per-URL HTTP cache; re-runs are free)
├── parsed/      ← stage 2: parsed.jsonl + parsed.parquet
├── extracted/   ← stage 3: extracted.jsonl + extracted.parquet
├── embedded/    ← stage 4: embedded.parquet (n × 384-d vectors)
└── reduced/     ← stage 5: reduced.parquet (UMAP 2-D + cluster id)
```

## Source surface

The site at `sapnhap.bando.com.vn/` is a thin PHP front-end
(`D:\map34tinh\s.index.php`) over a QGIS Server WMS/WFS deployment. It
exposes four POST endpoints we care about — every one returns JSON, even
when the server lies in its `Content-Type` header:

| Endpoint                | Form data                       | Returns                                              |
| ----------------------- | ------------------------------- | ---------------------------------------------------- |
| `POST /p.co_dvhc`        | `ma=0`                           | List of every admin unit (34 prov + 3,321 communes)  |
| `POST /p.co_uyban`       | `ma=0`                           | List of 3,357 commune people's-committee headquarters |
| `POST /p.co_dvhc_id`     | `malk=<feature_id>`              | Full attribute row (area, population, decree, …)     |
| `POST /pread_json`       | `id=<feature_id>`                | GeoJSON FeatureCollection (Polygon / MultiPolygon / Point) |

Feature-id conventions:

* `diaphanhanhchinhcaptinh_sn.<n>`   — province polygons (only 34 of the 132
  pre-merger ids survive).
* `diaphanhanhchinhcapxa_2025.<n>`  — commune polygons; 3,321 alive, the
  rest dissolved into neighbours.
* `uybannhandancapxa_2025.<n>`     — point markers for every commune
  people's committee (n = 1 … 3,357).

The PHP front-end occasionally injects an HTML warning preamble before the
JSON body when QGIS Server is mid-restart — `packages.common.http` peels
that off transparently.

## Stage 1 — download

```python
DownloadStage(config.download, raw_dir).run()
```

* **Two listing POSTs** capture the complete inventory in one shot each
  (`/p.co_dvhc` and `/p.co_uyban`).
* **Per-unit detail walk** (~3,355 POSTs to `/p.co_dvhc_id`) pulls the rich
  attribute row for every admin unit: area in km², population, capital,
  predecessors prose, decree of authority, link to the official decree at
  `vanban.chinhphu.vn`.
* **Per-feature geometry walk** (~6,712 POSTs to `/pread_json`) pulls the
  polygon for every admin unit and the point marker for every committee.
* Every URL is cached on disk under `raw/_cache/`; re-runs hit the cache and
  finish in seconds.
* `delay_between_requests_s: 0.10` keeps the crawl polite — ~10 req/s, no
  hint of rate limiting from the server.
* Wall time on first run: ~12 minutes for the listings + details, plus
  ~22 minutes for the geometries; ~35 minutes end-to-end on a home
  broadband line.

The full crawl materialises **roughly 6,700 small JSON / GeoJSON files**
totaling ~120 MB.

## Stage 2 — parse

```python
ParseStage(config.parse, raw_dir, parsed_dir).run()
```

Three jobs:

1. **Normalise Vietnamese-formatted numbers.** The `p.co_dvhc_id` endpoint
   uses Vietnamese locale (`"6.360,83"` = 6,360.83 km²); the GeoJSON
   `properties` block uses English (`"575.29"` = 575.29 km²,
   `"157629"` = 157,629 people). `parse_vi_decimal` and `parse_vi_int` in
   `packages/scraper/sapnhap.py` cover both idioms.
2. **Summarise GeoJSON.** Each FeatureCollection collapses to a single row
   with `centroid_lon`, `centroid_lat`, `bbox`, `geom_type`, `n_vertices`,
   and (when `parse.flatten_geojson=true`) a shapely-emitted WKT string.
   We use shapely's true centroid for polygons; for the rare environments
   without shapely, a ring-walk arithmetic-mean centroid is good enough for
   plotting.
3. **Stamp parent-province for every commune & committee.** The
   `tentinh` attribute on `p.co_dvhc_id` and the `a04_tentinh` attribute on
   the GeoJSON committee features carry the parent-province name; we
   resolve it against the 34-row province list to attach a stable
   `parent_ma` (NSO 2-digit province code).

Output is a single canonical row per entity (province, commune, or
committee), written as both `parsed.jsonl` and `parsed.parquet`. Schema:

| column          | type     | notes                                                         |
| --------------- | -------- | ------------------------------------------------------------- |
| `id`             | str      | feature id (`==` malk)                                        |
| `kind`           | str      | `province` / `commune` / `committee`                          |
| `ma`             | str      | NSO 2-digit province code or 5-char commune code              |
| `ten`            | str      | canonical Vietnamese name                                     |
| `type`           | str      | `Tỉnh` / `Thành Phố` / `Phường` / `Xã` / `Đặc khu` / …        |
| `ten_short`      | str      | `ten` with the type prefix stripped                           |
| `area_km2`       | float    | parsed via `parse_vi_decimal`                                 |
| `population`     | int      | parsed via `parse_vi_int`                                     |
| `density`        | float    | `population / area_km2`                                       |
| `capital`        | str?     | `trungtamhc` (administrative-centre address)                  |
| `address`        | str?     |                                                                |
| `phone`          | str?     |                                                                |
| `decree`         | str?     | `cancu` (e.g. `Nghị quyết số 202/2025/QH15`)                  |
| `decree_url`     | str?     | usually a `vanban.chinhphu.vn` permalink                      |
| `predecessors`   | str?     | raw `truocsapnhap` prose                                      |
| `parent_ma`      | str?     | NSO-code of the parent province (for communes & committees)   |
| `parent_ten`     | str?     |                                                                |
| `centroid_lon/lat` | float? | from the geometry summary                                     |
| `bbox`           | list?    | `[lon_min, lat_min, lon_max, lat_max]`                        |
| `geom_type`      | str?     | `Polygon` / `MultiPolygon` / `Point`                           |
| `wkt`            | str?     | shapely WKT (only when `flatten_geojson=true`)                 |

## Stage 3 — extract

```python
ExtractStage(config.extract, parsed_dir, extracted_dir).run()
```

Adds the analytical columns the downstream notebook + the visualizer need:

* **`macro_region`** — every entity is mapped to one of the six GSO
  macro-regions (`northern_midlands`, `red_river_delta`, `central_coast`,
  `central_highlands`, `southeast`, `mekong_delta`). The mapping table
  lives in `packages/curator/regions.py` and is hand-curated against the
  post-merger 34-province list.
* **`predecessors_list`** — explodes the `truocsapnhap` Vietnamese prose
  into a deduplicated list of predecessor names. Handles separators
  (`,`, `và`, `cùng`, `;`), strips mereological qualifiers ("phần còn lại
  của", "một phần"), and trims trailing "sau khi sắp xếp" clauses.
* **`n_predecessors`** — `len(predecessors_list)`.
* **`keywords`** — top-N TF-IDF unigrams + bigrams over the merger-lineage
  prose; uses a Vietnamese-friendly token pattern
  (`r"(?u)\b[\wÀ-ỹ]{3,}\b"`) so diacritics survive tokenisation.
* **`embed_text`** — single canonical Vietnamese descriptor (name + parent
  + type + predecessors + capital + decree) that the embedding stage
  consumes.

Output: `extracted.jsonl` + `extracted.parquet`.

## Stage 4 — embed

```python
EmbedStage(config.embed, extracted_dir, embedded_dir).run()
```

Encodes every record's `embed_text` field with
[`sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2)
on CPU (384-d, normalised, `batch_size=64`). The full corpus of ~6,700
short Vietnamese descriptors finishes in ~3 minutes on an M-series Mac.

For NIM-hosted embeddings (e.g. `nvidia/llama-3.2-nv-embedqa-1b-v2`)
the same backend abstraction used by `personas-vn` would slot in here —
set `embed.backend: nim` and a `PERSONAS_VN_LLM_API_KEY` env var.

Output: `embedded.parquet` with the meta columns plus a `vector`
column (list[float]).

## Stage 5 — reduce

```python
ReduceStage(config.reduce, embedded_dir, reduced_dir).run()
```

* **UMAP** projection to 2-D with cosine metric (15 neighbours,
  `min_dist=0.1`, `random_state=20260508`).
* **Density-based HDBSCAN** clustering (`min_cluster_size=⌊n/80⌋`),
  emitting an integer `cluster` column with `-1` reserved for low-density
  noise points.

Output: `reduced.parquet` — every meta column from the embed stage plus
`x`, `y`, and (when `reduce.cluster=true`) `cluster`. This is the parquet
that feeds the UMAP plots in [`DATAANALYSIS.ipynb`](DATAANALYSIS.ipynb)
and the curator-tab in any future Gradio visualizer.

## NeMo Curator backend

Pass `--backend nemo_curator` to `geography-vn curate` and the same five
stage objects are wrapped as `nemo_curator.core.stage.ProcessingStage`
sub-classes and handed to a real `nemo_curator.core.pipeline.Pipeline`
running through `nemo_curator.backends.experimental.in_process.InProcessExecutor`.
The wire-shape on disk is identical, so the rest of the pipeline (HF
upload, analysis notebook) does not care which executor ran. To go
distributed, swap `InProcessExecutor` for
`XennaExecutor` / `RayDataExecutor` — no code changes needed to the
stages themselves.

## Re-running individual stages

```bash
# Re-run only the embed + reduce stages (cheap when the corpus stayed put
# but the model changed):
geography-vn curate --only embed reduce

# Re-run everything except the slow geometry crawl:
geography-vn curate --skip download   # cached anyway, but explicit is faster
```

The on-disk per-URL cache (`raw/_cache/`) means that even
`--only download` re-runs are near-instantaneous after the first crawl —
only newly-published feature ids hit the network.
