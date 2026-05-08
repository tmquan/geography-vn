# DATAANALYSIS — what the curated data actually says

This document is the analytical companion to the curated parquet bundle
produced by `geography-vn curate`. It walks through the 14-figure pack
under [`docs/figures/analysis/`](docs/figures/analysis/) (rendered by
`scripts/analyze.py` and embedded inline in
[`DATAANALYSIS.ipynb`](DATAANALYSIS.ipynb)) and tells the story of
**Vietnam after Resolution 202/2025/QH15** through the data alone.

> **Source:** every number below comes from
> `data/sapnhap-bando-vn/extracted/extracted.parquet` (6,712 rows = 34
> provinces + 3,321 communes + 3,357 commune people's-committee
> headquarters). The parquet was produced by the five-stage NeMo Curator
> pipeline described in [`DATAPROCESSING.md`](DATAPROCESSING.md).

## TL;DR

| Stat                                  | Value             |
| ------------------------------------- | ----------------- |
| First-level units (post-merger)       | **34**            |
| Second-level units (post-merger)      | **3,321**         |
| People's-committee headquarters       | **3,357**         |
| Total Vietnam population (2024)       | **113,571,926**   |
| Total Vietnam land area (km²)         | **331,325.62**    |
| Largest province by population        | TPHCM — **14,002,598**   |
| Largest province by area              | Lâm Đồng — **24,233 km²** |
| Densest province                      | Hà Nội — **2,621 / km²**  |
| Most-merged commune                   | Phường Văn Miếu - Quốc Tử Giám — **16 predecessor wards** |
| Distinct authorising decrees          | **37**            |
| Provinces that kept their old borders | **11** out of 34  |

## 1 — Inventory ([fig 01](docs/figures/analysis/01_admin_kind_donut.png))

The atlas publishes three entity kinds, in roughly equal sub-counts:

| Kind        | Count   | What it is                                              |
| ----------- | ------- | ------------------------------------------------------- |
| `province`   | 34      | First-level admin units (28 provinces + 6 cities)        |
| `commune`    | 3,321   | Second-level units (phường / xã / đặc khu after the merger) |
| `committee`  | 3,357   | Commune people's-committee headquarters (point markers) |

The committee count slightly exceeds the commune count (3,357 > 3,321)
because some communes have multiple registered committee buildings (the
seat plus a satellite office) and a handful of pre-merger committees
remain marked on the map even after their parent commune dissolved.

## 2 — Macro-region balance ([fig 02](docs/figures/analysis/02_macro_region_breakdown.png))

The 34 surviving provinces redistribute across the six GSO macro-regions
unevenly. The north (where the 2025 merger was the most aggressive) keeps
**10 + 5 = 15 provinces** under two macro-regions; the centre keeps
**8 + 3 = 11**; the south keeps **3 + 5 = 8**. Including the
people's-committee tier (which inherits its parent province's region):

| Macro-region (EN)                          | Provinces | Communes | Committees | Total |
| ------------------------------------------ | --------: | -------: | ---------: | ----: |
| Northern Midlands and Mountain Areas       | **10**    |   841    |    842     | 1,693 |
| Red River Delta                            | **5**     |   527    |    502     | 1,034 |
| North Central and Central Coastal Areas    | **8**     |   738    |    737     | 1,483 |
| Central Highlands                          | **3**     |   361    |    380     |   744 |
| Southeast                                  | **3**     |   359    |    358     |   720 |
| Mekong River Delta                         | **5**     |   495    |    537     | 1,037 |
| (unknown — disputed-zone duplicates)       |    0      |     0    |      1     |     1 |

Note that the **Northern Midlands** retains the highest *count* of
first-level units (10) despite being the region the merger consolidated
hardest — pre-merger it had 14 provinces. The **Southeast** ends up the
smallest by province count (3) because TPHCM swallowed both Bình Dương
and Bà Rịa-Vũng Tàu into a single mega-city.

## 3 — Province population ([fig 03](docs/figures/analysis/03_province_population.png))

The top-five most populous provinces, after the merger:

| Rank | Province                | Population   | Area (km²) | Density (/km²) | Predecessors |
| ---- | ----------------------- | -----------: | ---------: | -------------: | -----------: |
| 1    | Thành Phố Hồ Chí Minh    | 14,002,598   |  6,772.59  | 2,067.5        | 3            |
| 2    | Thủ Đô Hà Nội           |  8,807,523   |  3,359.84  | 2,621.4        | 1            |
| 3    | Tỉnh An Giang            |  4,952,238   |  9,888.91  |   500.8        | 2            |
| 4    | Thành Phố Hải Phòng      |  4,664,124   |  3,194.72  | 1,460.0        | 2            |
| 5    | Thành phố Đồng Nai      |  4,491,408   | 12,737.18  |   352.6        | 4            |

TPHCM's merger absorbed **Bình Dương** (industrial belt) and **Bà Rịa
- Vũng Tàu** (the southern coast & Côn Đảo) into one super-city, doubling
its population from ~7 M to **14 M** — now larger than New York's metro.

Đồng Nai is the only province in the country that absorbed **four**
predecessor units (the original Đồng Nai + Bình Phước + parts of two
neighbours), making it the largest-area populated unit in the south.

## 4 — Province area ([fig 04](docs/figures/analysis/04_province_area.png))

The top-five largest provinces by km², all in the central highlands or
upper-central coast:

| Rank | Province          | Area (km²)  | Population | Density (/km²) | Predecessors |
| ---- | ----------------- | ----------: | ---------: | -------------: | -----------: |
| 1    | Tỉnh Lâm Đồng    | 24,233.07   |  3,872,999 | 159.8           | 3            |
| 2    | Tỉnh Gia Lai      | 21,576.53   |  3,583,693 | 166.1           | 2            |
| 3    | Tỉnh Đắk Lắk      | 18,096.40   |  3,346,853 | 184.9           | 2            |
| 4    | Tỉnh Nghệ An      | 16,486.50   |  3,831,694 | 232.4           | 1            |
| 5    | Tỉnh Quảng Ngãi   | 14,832.55   |  2,161,755 | 145.7           | 2            |

Lâm Đồng is the merger's largest single creation: the old Lâm Đồng +
**Bình Thuận** (south-central coast) + **Đắk Nông** (Central Highlands)
fused into a 24,233 km² province that now stretches from the central
plateau to the South China Sea coast.

## 5 — Province density ([fig 05](docs/figures/analysis/05_province_density.png))

| Rank | Province                | Density (/km²) | Population | Area (km²) |
| ---- | ----------------------- | -------------: | ---------: | ---------: |
| 1    | Thủ Đô Hà Nội           | 2,621.4        |  8,807,523 |  3,359.84  |
| 2    | Thành Phố Hồ Chí Minh    | 2,067.5        | 14,002,598 |  6,772.59  |
| 3    | Thành Phố Hải Phòng      | 1,460.0        |  4,664,124 |  3,194.72  |
| 4    | Tỉnh Hưng Yên            | 1,418.8        |  3,567,943 |  2,514.81  |
| 5    | Tỉnh Ninh Bình           | 1,119.1        |  4,412,264 |  3,942.62  |

Hà Nội remains the densest province even though it kept its pre-merger
borders unchanged (its `n_predecessors == 1`). Hưng Yên, **post-merger**
absorbing Thái Bình, jumps into the top 4 — the merged Red River Delta
provinces concentrate density there.

## 6 — Communes per province ([fig 06](docs/figures/analysis/06_communes_per_province.png))

The lower-tier consolidation is more uneven:

* **Min**: 38 communes (Tỉnh Lai Châu — sparsely populated mountains)
* **Max**: 168 communes (Thành phố Hồ Chí Minh — mega-city)
* **Median**: 99 communes per province
* **Mean**: 97.7 communes per province

Bottom five (sparsest):

| Province           | Communes |
| ------------------ | -------: |
| Tỉnh Lai Châu      |    38    |
| Thành phố Huế      |    40    |
| Tỉnh Điện Biên     |    45    |
| Tỉnh Quảng Ninh    |    54    |
| Tỉnh Cao Bằng      |    56    |

Top five (densest):

| Province                   | Communes |
| -------------------------- | -------: |
| Thành phố Hồ Chí Minh      |   168    |
| Tỉnh Thanh Hóa             |   166    |
| Tỉnh Phú Thọ               |   148    |
| Tỉnh Gia Lai               |   135    |
| Tỉnh Nghệ An               |   130    |

## 7 — Province merger fanout ([fig 07](docs/figures/analysis/07_merger_fanout_provinces.png))

How many predecessor provinces fed each surviving first-level unit:

| n_predecessors | Provinces | Names                                                                                                    |
| -------------- | --------: | -------------------------------------------------------------------------------------------------------- |
| **1**           | **11**    | Hà Nội, Huế, Cao Bằng, Điện Biên, Hà Tĩnh, Lai Châu, Lạng Sơn, Nghệ An, Quảng Ninh, Sơn La, Thanh Hóa     |
| **2**           | **16**    | Đà Nẵng, Hải Phòng, An Giang, Bắc Ninh, Cà Mau, Đắk Lắk, Đồng Tháp, Gia Lai, Hưng Yên, Khánh Hòa, … |
| **3**           | **6**     | Cần Thơ, Hồ Chí Minh, Lâm Đồng, Ninh Bình, Phú Thọ, Vĩnh Long                                          |
| **4**           | **1**     | Đồng Nai                                                                                                  |

Eleven provinces kept their old borders — they were already large enough
or geographically isolated enough that the merger left them alone. The
modal merger absorbed exactly one neighbour. Đồng Nai is the singular
four-way merger.

## 8 — Commune merger fanout ([fig 08](docs/figures/analysis/08_merger_fanout_communes.png))

The lower tier was consolidated much more aggressively. Of the 3,321
surviving communes:

| n_predecessors | Communes | %     |
| -------------- | -------: | ----: |
|  **1**          |    139   |  4.2% |
|  **2**          |    758   | 22.8% |
|  **3**          |  1,498   | 45.1% |
|  **4**          |    559   | 16.8% |
|  **5**          |    179   |  5.4% |
|  **6**          |     78   |  2.3% |
|  **7+**         |    110   |  3.4% |
| **max**         |     16   |       |

The modal commune absorbed **3 predecessor wards/xã**. The mean fanout
is 3.4 (compared to ~1.6 for the first-level tier).

The **most-merged commune is Phường Văn Miếu - Quốc Tử Giám** in Hà
Nội, which fused **16 predecessor wards** into a single 4-character
neighbourhood. Hà Nội dominates the high-fanout extreme — 8 of the top
15 communes by fanout are in Hà Nội, reflecting the post-merger desire
to flatten the dense old ward grid into larger, more administratively
manageable units. The other notable mega-mergers are coastal special
zones: **Đặc khu Cát Hải** (Hải Phòng, 12 preds), **Đặc khu Vân Đồn**
(Quảng Ninh, 12 preds).

## 9 — Commune size distribution ([fig 09](docs/figures/analysis/09_commune_size_distribution.png))

A log-log scatter of commune area vs population reveals three regimes:

* **Special administrative zones (Đặc khu)** — sit in the bottom-left:
  tiny populations on tiny islands. Hoàng Sa (the Paracels — 0
  registered population, 350 km²), Trường Sa (Spratly — 153 people,
  496 km²), Cồn Cỏ (139 people, 2.3 km²), Bạch Long Vĩ (686 people,
  3.07 km²). All have ambiguous-to-disputed sovereignty status and
  serve mainly as markers of national territory.
* **Highland xã** — the upper-left band: tens of thousands of
  inhabitants spread across 200-1100 km² of mountain terrain. Buôn Đôn
  (Đắk Lắk, 1,114 km², 6.6K people) is the largest by area in the entire
  set. The Trường Sơn cordillera communes in Quảng Trị (Thượng Trạch,
  Trường Sơn, Kim Ngân) all exceed 800 km² each.
* **Urban phường** — the lower-right cluster: 50-130K people on a few
  km² of city. Phường Hải Châu (Đà Nẵng, 7.58 km², 131K people) is the
  densest by far.

## 10 — Decree corpus ([fig 10](docs/figures/analysis/10_decree_map.png))

The merger is authorised by **37 distinct decrees**: 1 from the National
Assembly itself (`Nghị quyết số 202/2025/QH15`) and 34 follow-up
Standing Committee resolutions (`NQ-UBTVQH15`) of 16 June 2025 that flesh
out the lower-tier merger commune-by-commune. The top decrees by number
of units cited:

| Decree                            | Units cited |
| --------------------------------- | ----------: |
| `Nghị quyết số 1685/NQ-UBTVQH15`   | 168          |
| `Nghị quyết số 1686/NQ-UBTVQH15`   | 166          |
| `Nghị quyết số 1676/NQ-UBTVQH15`   | 148          |
| `Nghị quyết số 1664/NQ-UBTVQH15`   | 135          |
| `Nghị quyết số 1678/NQ-UBTVQH15`   | 130          |
| `Nghị quyết số 1674/NQ-UBTVQH15`   | 129          |
| `Nghị quyết số 1656/NQ-UBTVQH15`   | 126          |
| … (30 more)                       | …            |

Each `NQ-1656…1690` decree corresponds to one of the 34 surviving
provinces. The top decree (NQ-1685) authorises all 168 wards of TPHCM;
NQ-1686 covers the 166 communes of Thanh Hóa; etc. Every commune row in
the parquet has a `decree_url` column pointing to the official text at
`vanban.chinhphu.vn`.

## 11 — Cartographic pack with dual-archipelago declaration

Geographic visualisations live in their own folder under
[`docs/figures/maps/`](docs/figures/maps/) — rendered standalone by
`python -m scripts.render_maps` because kaleido + Mapbox-GL crashes
inside a Jupyter kernel. Five figures total, each with the **actual
scraped province polygons** (not bubbles) and the standard
Vietnamese-atlas declaration of both offshore archipelagos:
* **Quần đảo Hoàng Sa** (Paracel Islands) — dashed bounding outline
  ~15.45°N–17.20°N × 110.85°E–113.10°E with markers for Phú Lâm, Tri Tôn,
  Linh Côn, Quang Hòa. Administered by **Thành phố Đà Nẵng** under the
  post-2025 geography (Đặc khu Hoàng Sa).
* **Quần đảo Trường Sa** (Spratly Islands) — dashed bounding outline
  ~7.40°N–11.85°N × 111.80°E–115.30°E with markers for Trường Sa Lớn,
  Song Tử Tây, Sinh Tồn, Phan Vinh, An Bang, Nam Yết, Cô Lin, Sơn Ca.
  Administered by **Tỉnh Khánh Hòa** (Đặc khu Trường Sa).

Each map additionally carries: ★ Thủ đô Hà Nội (capital), 7 secondary
cities (TP. Hồ Chí Minh / Đà Nẵng / Hải Phòng / Huế / Vinh / Cần Thơ /
Nha Trang) with leader-line labels into open sea, and 5 island callouts
(Đảo Phú Quốc / Cát Bà / Bạch Long Vĩ / Lý Sơn + Côn Đảo).

| File                                            | Visualisation                                                |
| ----------------------------------------------- | ------------------------------------------------------------ |
| `maps/01_provinces_population.png`               | population choropleth (sequential NVIDIA-green)              |
| `maps/02_provinces_density.png`                  | people / km²                                                 |
| `maps/03_provinces_area.png`                     | land area (km²)                                              |
| `maps/04_communes_scatter.png`                   | 3,321 commune centroids by macro-region                      |
| `maps/05_committees_scatter.png`                 | 3,357 commune people's-committee headquarters by lat/lon     |

The cartographic pack uses NVIDIA Sans typography on a square 1100 × 1100
canvas; the analytical pack above uses the LaTeX-serif `nvidia_latex`
template at 1200 × 900. Maps follow brand convention, charts follow
academic-paper convention.

## 12 — Curator UMAP ([fig 11](docs/figures/analysis/11_curator_umap_kind.png), [fig 12](docs/figures/analysis/12_curator_umap_region.png))

A 2-D UMAP projection of the 6,712-point sentence-transformers
embedding of every entity's `embed_text` descriptor. Three observations:

1. **Kind separation is partial.** Provinces and committees occupy
   compact, well-separated tail regions of the map (provinces
   because their descriptors include rich predecessor-province prose
   that other entities do not; committees because every committee
   description follows the standard "Ủy ban nhân dân …" template).
   Communes fill the middle and overlap with both extremes.
2. **Macro-region structure emerges.** Even though the embed text
   does not explicitly mention macro-region, the UMAP × macro-region
   colouring shows clean lobes for the Mekong Delta, the Northern
   Midlands, and the Central Highlands — the model picks up on
   regional naming conventions (`Đặc khu Phú Quốc` clusters with the
   southern coast, `Xã Mường Khương` with the Northwest, etc.).
3. **HDBSCAN finds ~30-50 dense subclusters** within the broader
   regional structure. Most are sub-province-level: a single
   province's communes cluster together because their descriptors
   share a common `parent_ten` tag, the same parent decree, and
   similar predecessor-name patterns ("Phường … và Phường … sau khi
   sắp xếp"). The `cluster` column in `reduced.parquet` carries the
   integer label per row, with `-1` reserved for low-density noise
   points.

## Caveats & known limits

* **`Đặc khu Hoàng Sa` has zero registered population.** The Paracel
  Islands are administered by the People's Republic of China and have
  not been physically Vietnamese-controlled since 1974. The Vietnamese
  atlas marks them as a special administrative zone of Đà Nẵng with a
  populations of zero — a sovereignty assertion, not a usable
  demographic figure.
* **Population numbers are 2024 mid-year estimates** as published with
  the merger decrees (rounded thousands at the commune level). They
  predate the merger by 6–12 months and may not yet reflect intra-merger
  population migration.
* **Two parent-province strings come back inconsistent:** "Thủ đô Hà Nội"
  vs "Thủ Đô Hà Nội" (different `Đ`/`đ` capitalisation depending on the
  endpoint). The `parent_ma` column normalises this — join on `parent_ma`
  not on `parent_ten` for analysis.
* **3 communes carry the name "Xã Hoàng Hoa Thám"** (each in a different
  province) and 1 carries "Đặc khu Phú Quốc". Use `id` (== feature id)
  not `ten` as the primary key.
* **The committee macro-region** is derived from the GeoJSON
  `properties.a04_tentinh` field, which can be missing for ~20 of the
  3,357 committees (mostly defunct pre-merger seats). Those rows show
  `macro_region == "unknown"` in the parquet.

## How to reproduce

```bash
# 1) Download + curate (~35 min on first run, then cached)
geography-vn curate

# 2) Render the figure pack (~2 min)
python -m scripts.analyze

# 3) Re-execute the notebook (optional)
jupyter nbconvert --to notebook --execute --inplace DATAANALYSIS.ipynb

# 4) Stage and push the HF dataset
python -m scripts.upload_to_hf --no-upload
python -m scripts.upload_to_hf --repo <your-org>/sapnhap-bando-vn
```

See [`DATAPROCESSING.md`](DATAPROCESSING.md) for the curator-pipeline
internals.
