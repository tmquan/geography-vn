"""Generate the deep-analysis figure pack from the curated parquet bundle.

This is the headless figure-rendering script that produces every PNG + HTML
pair that the dataset card / README / DATAANALYSIS notebook embed. Driven by
the same parquet files the visualizer reads, so a single ``geography-vn
curate`` run is enough to feed both surfaces.

Output layout (mirrors ``personas-vn/docs/figures/``)::

    docs/figures/analysis/
    ├── 01_admin_kind_donut.{png,html}
    ├── 02_macro_region_breakdown.{png,html}
    ├── 03_province_population.{png,html}
    ├── 04_province_area.{png,html}
    ├── 05_province_density.{png,html}
    ├── 06_communes_per_province.{png,html}
    ├── 07_merger_fanout_provinces.{png,html}
    ├── 08_merger_fanout_communes.{png,html}
    ├── 09_commune_size_distribution.{png,html}
    ├── 10_decree_map.{png,html}
    ├── 11_provinces_choropleth.{png,html}
    ├── 12_committees_scatter.{png,html}
    ├── 13_curator_umap_kind.{png,html}
    └── 14_curator_umap_region.{png,html}

Both formats are written: the PNG is what the dataset card embeds inline, the
HTML is the fully-interactive Plotly version (pan / zoom / hover).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from packages.common.config import load_config
from packages.common.logging import get_logger
from packages.common.paths import REPO_ROOT, ensure_dir
from packages.curator.regions import MACRO_REGION_EN
from packages.viz.style import (
    FIG_H,
    FIG_W,
    NV_BLACK,
    NV_GREEN,
    NV_GREEN_DARK,
    NV_LIGHT_GREY,
    apply_nvidia_latex_style,
    register_plotly_template,
    save_figure,
)

log = get_logger(__name__)

# Apply the NVIDIA + LaTeX-serif Plotly template at import time so every
# downstream ``go.Figure()`` / ``px.bar()`` starts with the right look.
register_plotly_template("nvidia_latex")

# Six-region categorical palette. NVIDIA Green is reserved for the macro-
# region (Mekong Delta) where Cần Thơ + the persona project's home base
# sit; the other five regions get curated brand-adjacent hues that read
# correctly on a white canvas.
REGION_COLOURS: dict[str, str] = {
    "northern_midlands":  "#1F4E79",   # deep blue (Đông Bắc)
    "red_river_delta":    NV_GREEN_DARK,
    "central_coast":      "#C97C00",   # warm amber
    "central_highlands":  "#7F4E2C",   # rich brown
    "southeast":          NV_BLACK,
    "mekong_delta":       NV_GREEN,
    "unknown":            NV_LIGHT_GREY,
}
KIND_COLOURS: dict[str, str] = {
    "province":  NV_GREEN_DARK,
    "commune":   NV_GREEN,
    "committee": NV_BLACK,
}

# Analytical pack inventory. Geographic visualisations (choropleths,
# committee point-cloud) deliberately live in ``docs/figures/maps/`` —
# rendered by ``scripts/render_maps.py`` with the proper Mapbox-style
# canvas, real province polygons, and the dual-archipelago declaration
# (Hoàng Sa + Trường Sa). The analytical pack here owns bar charts,
# scatter, distributions, and the curator UMAP.
FIGURE_REGISTRY: list[tuple[str, str, str]] = [
    ("01_admin_kind_donut",          "Vietnam post-merger admin units",
        "Resolution 202/2025/QH15 collapsed 63 → 34 provinces and re-drew the lower tier."),
    ("02_macro_region_breakdown",    "Macro-region inventory after the merger",
        "How the 34 provinces and 3,321 communes redistribute across the 6 GSO macro-regions."),
    ("03_province_population",       "34 provinces by population",
        "Population per first-level admin unit (TPHCM and Hà Nội dwarf the rest)."),
    ("04_province_area",             "34 provinces by area",
        "Area in km² per first-level admin unit; reveals the merged Highland mega-provinces."),
    ("05_province_density",          "34 provinces by population density",
        "people / km², latest figures published with the merger decree."),
    ("06_communes_per_province",     "Communes per province after the merger",
        "Lower-tier counts per first-level unit; range 9 (Đặc khu) – 200+ (TPHCM)."),
    ("07_merger_fanout_provinces",   "How many predecessors fed each surviving province",
        "1 = the province kept its borders; 2-3 = absorbed neighbours."),
    ("08_merger_fanout_communes",    "How many predecessor wards/xã fed each surviving commune",
        "Many communes absorb 4-6 predecessor units after the cấp-huyện collapse."),
    ("09_commune_size_distribution", "Commune-level area + population distribution",
        "Long-tailed: a handful of mega-communes (urban districts) carry > 100K residents."),
    ("10_decree_map",                "Decree corpus distribution",
        "Which UBTVQH and QH decrees authorise the merger of each unit."),
    ("11_curator_umap_kind",         "Curator UMAP — every entity coloured by kind",
        "Sentence-transformers embedding of the merger-lineage prose."),
    ("12_curator_umap_region",       "Curator UMAP — coloured by macro-region",
        "Same projection; reveals semantic clusters that follow geography."),
]


def write_pair(fig: go.Figure, out_dir: Path, slug: str,
                *, width: int = FIG_W, height: int = FIG_H) -> None:
    """Apply the NVIDIA + LaTeX-serif theme and persist both PNG + HTML.

    Every analytical figure ships at the canonical
    :data:`packages.viz.style.FIG_W` × :data:`packages.viz.style.FIG_H`
    pixel size so the dataset card / DATAANALYSIS pages don't see a
    viewport jump between figures.
    """
    apply_nvidia_latex_style(fig)
    save_figure(fig, slug, width=width, height=height, out_dir=out_dir)


def _attach_region_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["macro_region_en"] = df["macro_region"].map(MACRO_REGION_EN).fillna("Other")
    return df


def _region_colour_map() -> dict[str, str]:
    """Map English macro-region label → hex colour, in canonical order."""
    return {MACRO_REGION_EN[k]: v
            for k, v in REGION_COLOURS.items()
            if k in MACRO_REGION_EN}


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------
def fig_kind_donut(df: pd.DataFrame) -> go.Figure:
    counts = df["kind"].value_counts().rename_axis("kind").reset_index(name="n")
    fig = px.pie(counts, names="kind", values="n", hole=0.55,
                 color="kind", color_discrete_map=KIND_COLOURS)
    fig.update_traces(textinfo="label+percent+value", pull=[0.02] * len(counts))
    fig.update_layout(
        title=f"Vietnam post-merger admin units (n = {len(df)})",
        showlegend=False,
    )
    return fig


def fig_macro_region(df: pd.DataFrame) -> go.Figure:
    df = _attach_region_label(df)
    pivot = (df.groupby(["macro_region_en", "kind"])
                .size().reset_index(name="n"))
    fig = px.bar(pivot, x="macro_region_en", y="n", color="kind",
                 color_discrete_map=KIND_COLOURS, text="n",
                 category_orders={"macro_region_en":
                                   list(MACRO_REGION_EN.values())})
    fig.update_traces(textposition="outside")
    fig.update_layout(
        title="Macro-region inventory: provinces, communes, committees",
        xaxis_title=None, yaxis_title="count",
        legend_title="entity kind",
    )
    return fig


def fig_province_metric(df: pd.DataFrame, *, metric: str, title: str,
                          unit: str) -> go.Figure:
    sub = df[(df["kind"] == "province") & df[metric].notna()].copy()
    sub = sub.sort_values(metric, ascending=True)
    sub = _attach_region_label(sub)
    fig = px.bar(sub, x=metric, y="ten", orientation="h",
                  color="macro_region_en",
                  color_discrete_map=_region_colour_map(),
                  hover_data={metric: ":.0f", "ten": False, "macro_region_en": True})
    fig.update_layout(
        title=title, xaxis_title=unit, yaxis_title=None,
        legend_title="macro-region",
    )
    return fig


def fig_communes_per_province(df: pd.DataFrame) -> go.Figure:
    sub = df[df["kind"] == "commune"]
    counts = (sub.groupby("parent_ten").size()
                  .reset_index(name="n_communes")
                  .sort_values("n_communes", ascending=True))
    counts["macro_region"] = counts["parent_ten"].map(
        lambda p: _province_region(df, p))
    counts["macro_region_en"] = counts["macro_region"].map(MACRO_REGION_EN).fillna("Other")
    fig = px.bar(counts, x="n_communes", y="parent_ten", orientation="h",
                  color="macro_region_en",
                  color_discrete_map=_region_colour_map(),
                  text="n_communes")
    fig.update_traces(textposition="outside")
    fig.update_layout(title="Communes per province (post-merger)",
                       xaxis_title="number of communes / wards",
                       yaxis_title=None,
                       legend_title="macro-region")
    return fig


def fig_merger_fanout(df: pd.DataFrame, *, kind: str, title: str) -> go.Figure:
    sub = df[df["kind"] == kind].copy()
    counts = (sub["n_predecessors"].fillna(0).astype(int)
               .value_counts().sort_index().reset_index())
    counts.columns = ["n_predecessors", "n_units"]
    fig = px.bar(counts, x="n_predecessors", y="n_units", text="n_units",
                  color_discrete_sequence=[NV_GREEN])
    fig.update_traces(textposition="outside")
    fig.update_layout(
        title=title,
        xaxis_title="number of predecessor units absorbed",
        yaxis_title=f"number of {kind}s",
    )
    return fig


def fig_commune_size_distribution(df: pd.DataFrame) -> go.Figure:
    sub = df[(df["kind"] == "commune") & df["population"].notna()
             & df["area_km2"].notna()].copy()
    sub = _attach_region_label(sub)
    fig = px.scatter(
        sub, x="area_km2", y="population",
        color="macro_region_en",
        color_discrete_map=_region_colour_map(),
        hover_name="ten", hover_data=["parent_ten", "type"],
        log_x=True, log_y=True, opacity=0.55,
    )
    fig.update_layout(
        title="Commune size distribution (log-log)",
        xaxis_title="area (km², log scale)",
        yaxis_title="population (log scale)",
        legend_title="macro-region",
    )
    return fig


def fig_decree_map(df: pd.DataFrame) -> go.Figure:
    sub = df.dropna(subset=["decree"]).copy()
    sub["decree_short"] = sub["decree"].str.replace(r"\s+", " ",
                                                       regex=True).str.strip()
    counts = (sub.groupby(["decree_short", "kind"]).size()
                  .reset_index(name="n").sort_values("n", ascending=False)
                  .head(20))
    fig = px.bar(counts, x="n", y="decree_short", color="kind",
                  orientation="h", color_discrete_map=KIND_COLOURS, text="n")
    fig.update_traces(textposition="outside")
    fig.update_layout(
        title="Top decrees authorising the merger (count by entity kind)",
        xaxis_title="number of units cited", yaxis_title=None,
        legend_title="entity kind",
    )
    return fig


def fig_curator_umap(df: pd.DataFrame, *, by: str) -> go.Figure:
    df = _attach_region_label(df)
    if by == "kind":
        colour_col = "kind"
        cmap = KIND_COLOURS
    else:
        colour_col = "macro_region_en"
        cmap = _region_colour_map()
    fig = px.scatter(
        df, x="x", y="y", color=colour_col,
        color_discrete_map=cmap, hover_name="ten",
        hover_data={"parent_ten": True, "kind": True, "macro_region_en": True,
                     "x": False, "y": False},
        opacity=0.6,
    )
    fig.update_traces(marker=dict(size=4))
    fig.update_layout(
        title=f"Curator UMAP — coloured by {by}",
        xaxis_title="UMAP-1", yaxis_title="UMAP-2",
        legend_title=by,
    )
    return fig


def _province_region(df: pd.DataFrame, name: str) -> str:
    sub = df[(df["kind"] == "province") & (df["ten"] == name)]
    if sub.empty:
        return "unknown"
    return str(sub["macro_region"].iloc[0])


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run_all(*, root: Path, out_dir: Path) -> dict[str, Any]:
    extracted = root / "extracted" / "extracted.parquet"
    reduced = root / "reduced" / "reduced.parquet"
    if not extracted.exists():
        raise FileNotFoundError(
            f"missing {extracted}; run `python -m packages.pipeline.cli curate` first."
        )

    df = pd.read_parquet(extracted)
    log.info("loaded %d rows from %s", len(df), extracted)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) inventory
    write_pair(fig_kind_donut(df),                   out_dir, "01_admin_kind_donut")
    write_pair(fig_macro_region(df),                 out_dir, "02_macro_region_breakdown")
    # 2) province-level summaries
    write_pair(fig_province_metric(df, metric="population", unit="people",
                                     title="34 provinces by population"),
                                     out_dir, "03_province_population")
    write_pair(fig_province_metric(df, metric="area_km2", unit="km²",
                                     title="34 provinces by area"),
                                     out_dir, "04_province_area")
    write_pair(fig_province_metric(df, metric="density", unit="people / km²",
                                     title="34 provinces by population density"),
                                     out_dir, "05_province_density")
    write_pair(fig_communes_per_province(df),        out_dir, "06_communes_per_province")
    # 3) merger fanout
    write_pair(fig_merger_fanout(df, kind="province",
                                   title="Province merger fanout"),
                                   out_dir, "07_merger_fanout_provinces")
    write_pair(fig_merger_fanout(df, kind="commune",
                                   title="Commune merger fanout"),
                                   out_dir, "08_merger_fanout_communes")
    # 4) distributions
    write_pair(fig_commune_size_distribution(df),   out_dir, "09_commune_size_distribution")
    write_pair(fig_decree_map(df),                  out_dir, "10_decree_map")
    # 5) curator embedding (if reduced.parquet exists). Geographic
    # visualisations (choropleth / committee scatter) deliberately live
    # in ``docs/figures/maps/`` — see ``scripts/render_maps.py``.
    if reduced.exists():
        rdf = pd.read_parquet(reduced)
        write_pair(fig_curator_umap(rdf, by="kind"),         out_dir, "11_curator_umap_kind")
        write_pair(fig_curator_umap(rdf, by="macro_region"), out_dir, "12_curator_umap_region")
    else:
        log.warning("no %s yet — skipping UMAP figures", reduced)

    return {"out_dir": str(out_dir),
             "n_figures": len(list(out_dir.glob("*.png")))}


def main() -> None:
    p = argparse.ArgumentParser(description="Render the deep-analysis figure pack.")
    p.add_argument("--config", default="configs/curator.yaml",
                   help="curator/viz config to consult for output paths "
                        "(default: configs/curator.yaml)")
    p.add_argument("--root", default=None,
                   help="override curator artefact root (default from config)")
    p.add_argument("--out", default=None,
                   help="override figure output directory (default from config)")
    args = p.parse_args()

    cfg = load_config(args.config)
    root_str = args.root or str(cfg.dataset.root)
    out_str = args.out or str(cfg.viz.get("analysis_dir") or "docs/figures/analysis")
    root = REPO_ROOT / root_str if not Path(root_str).is_absolute() else Path(root_str)
    out_dir = REPO_ROOT / out_str if not Path(out_str).is_absolute() else Path(out_str)
    summary = run_all(root=root, out_dir=ensure_dir(out_dir))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
