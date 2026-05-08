"""Render the geographic-visualisation pack to ``docs/figures/maps/``.

The cartographic stack is the Vietnamese-atlas convention used by
``personas-vn``'s ``scripts/render_maps.py``: a white-bg Mapbox-style
canvas, NVIDIA brand colours + LaTeX-serif typography, and the **dual
archipelago declaration** (Hoàng Sa + Trường Sa as dashed bounding
outlines with bilingual labels and principal-island markers).

Five map figures are produced:

==================  ============================================================
File                 Visualisation
==================  ============================================================
``01_provinces_choropleth.png``  Population by post-merger province (choropleth)
``02_provinces_density.png``     Density (people / km²) by province (choropleth)
``03_provinces_area.png``        Area (km²) by province (choropleth)
``04_communes_scatter.png``      3,321 commune polygons rendered as centroids
``05_committees_scatter.png``    3,357 commune people's committees as points
==================  ============================================================

Every figure ships in BOTH formats (PNG + interactive HTML) under
``docs/figures/maps/``. The HF dataset card embeds the PNGs and links
the HTMLs as "interactive" companions.

The script runs as a standalone Python process (NOT inside a Jupyter
kernel) so kaleido + Mapbox-GL works for PNG export — a Jupyter kernel
loses Mapbox-GL's worker and triggers ``KaleidoError 525``.

Usage::

    python -m scripts.render_maps
    python -m scripts.render_maps --root data/sapnhap-bando-vn --out docs/figures/maps
"""

# Imports below the ``sys.path.insert`` are intentional — the script is
# runnable as ``python scripts/render_maps.py`` from the repo root, so we
# have to extend ``sys.path`` before importing ``packages.*``.
# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from packages.common.config import load_config
from packages.common.logging import get_logger
from packages.common.paths import ensure_dir
from packages.curator.regions import MACRO_REGION_EN
from packages.viz.archipelago import HOANG_SA, TRUONG_SA
from packages.viz.style import (
    FIG_H_MAP,
    FIG_W_MAP,
    NV_BLACK,
    NV_DARK,
    NV_DISCRETE,
    NV_FONT_FAMILY,
    NV_GREEN,
    NV_GREEN_DARK,
    NV_LIGHT_GREY,
    NV_SEQUENTIAL,
    NV_WHITE,
    save_figure,
)
from packages.viz.vietnam_geo import (
    NOTABLE_CITIES,
    NOTABLE_ISLANDS,
    load_vietnam_geojson,
    normalise_province_name,
)

log = get_logger(__name__)

# Cartographic figures use **NVIDIA Sans** (the brand sans-serif), not the
# Latin-Modern serif used by the analytical pack. Maps follow brand
# guidelines (which read better against polygon labels at country scale);
# bar / scatter charts follow academic-paper convention.
MAP_FONT = NV_FONT_FAMILY

pio.templates["nvidia_maps"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor=NV_WHITE,
        plot_bgcolor=NV_WHITE,
        font=dict(family=MAP_FONT, color=NV_BLACK, size=14),
        colorway=NV_DISCRETE,
    ),
)
pio.templates.default = "nvidia_maps"


# ---------------------------------------------------------------------------
# Mapbox layout primitives — pixel-perfect, identical across every figure
# ---------------------------------------------------------------------------
# Tuned for a SQUARE 1100 × 1100 canvas: Vietnam is taller than wide, so a
# square frame fills more of the visible area than the previous 4:3 frame
# (which left ~25 % whitespace on each horizontal margin).
#
#   x: [0.01, 0.85]  →  pixels   11 .. 935  (924 wide)
#   y: [0.04, 0.94]  →  pixels   66 .. 1034 (968 tall)
#
# Top 6 % is reserved for the title (66 px in 1100 → fits 18-pt font with
# breathing room); bottom 4 % for the publication footer / future
# annotation slot. Right 15 % carves out the colorbar / legend column.
_MAPBOX_DOMAIN_X = (0.01, 0.85)
_MAPBOX_DOMAIN_Y = (0.04, 0.94)

_COLORBAR_KW = dict(
    xref="container", x=0.87, xanchor="left",
    yref="container", y=0.50, yanchor="middle",
    len=0.78, lenmode="fraction",
    thickness=18,
)

# Legend for the scatter maps lives in the upper-LEFT corner, below the
# title — the country's western flank is open Laos / Yunnan territory in
# the projection and never collides with city / island labels. The old
# lower-left position (y=0.30) overlapped the ``Đảo Phú Quốc`` leader-line.
_LEGEND_KW = dict(
    xref="paper", yref="paper",
    x=0.02, xanchor="left",
    y=0.86, yanchor="top",
    bgcolor="rgba(255,255,255,0.95)",
    bordercolor=NV_LIGHT_GREY, borderwidth=1,
)


def vietnam_map_layout(*, title: str) -> dict:
    """Common layout for every cartographic figure.

    Vietnam sits at IDENTICAL pixel coordinates in every export so a reader
    scrolling through ``docs/figures/maps/`` never sees the country "jump"
    between figures. Three primitives lock that down:

    * ``margin = 0`` everywhere — defeats Plotly's auto-margin shifting.
    * ``mapbox.domain`` carves a fixed paper-fraction rectangle.
    * ``mapbox.center`` and ``zoom`` are constant.
    """
    return dict(
        title=dict(text=title,
                    font=dict(family=MAP_FONT, color=NV_DARK, size=18),
                    x=0.02, xanchor="left",
                    y=0.985, yanchor="top",
                    automargin=False, pad=dict(l=0, r=0, t=0, b=0)),
        paper_bgcolor=NV_WHITE, plot_bgcolor=NV_WHITE,
        margin=dict(l=0, r=0, t=0, b=0, autoexpand=False),
        mapbox=dict(style="white-bg",
                     center=dict(lon=108.0, lat=15.6),
                     zoom=4.6,
                     domain=dict(x=list(_MAPBOX_DOMAIN_X),
                                  y=list(_MAPBOX_DOMAIN_Y))),
    )


# ---------------------------------------------------------------------------
# Archipelago overlays (Hoàng Sa + Trường Sa)
# ---------------------------------------------------------------------------
def _archipelago_outline_trace(meta: dict) -> go.Scattermapbox:
    """Dashed-look polygon for one archipelago.

    Scattermapbox does not support ``dash`` natively; we approximate by
    emitting every other segment along each polygon edge, which reads as a
    crisp dashed border at country scale.
    """
    poly = meta["polygon"]
    lons: list[float | None] = []
    lats: list[float | None] = []
    n_segs = 32
    for i in range(len(poly) - 1):
        x0, y0 = poly[i]
        x1, y1 = poly[i + 1]
        for k in range(n_segs):
            if k % 2 == 0:
                t0, t1 = k / n_segs, (k + 1) / n_segs
                lons.extend([x0 + (x1 - x0) * t0, x0 + (x1 - x0) * t1, None])
                lats.extend([y0 + (y1 - y0) * t0, y0 + (y1 - y0) * t1, None])
    return go.Scattermapbox(
        lon=lons, lat=lats, mode="lines",
        line=dict(color=NV_GREEN_DARK, width=2),
        hoverinfo="skip", showlegend=False,
    )


def _archipelago_label_trace(meta: dict) -> go.Scattermapbox:
    """Bilingual archipelago label, anchored just below the bounding box.

    Mapbox-GL caveats handled here:

    * ``mode='text'`` alone crashes — use ``markers+text`` with an
      invisible 0-size marker.
    * ``textfont.family`` triggers a Mapbox-GL glyph fetch, which fails
      on ``mapbox.style='white-bg'``. We omit ``family`` and accept
      Mapbox-GL's default Open Sans (Vietnamese diacritics still render).
    * Mapbox-GL does NOT parse HTML / pseudo-HTML in text — emit plain.
    """
    return go.Scattermapbox(
        lon=[meta["centre"][0]],
        lat=[meta["lat_min"] - 0.20],
        mode="markers+text",
        text=[meta["name_vi"]],
        textfont=dict(color=NV_BLACK, size=12),
        textposition="bottom center",
        marker=dict(size=1, color="rgba(0,0,0,0)"),
        hoverinfo="skip", showlegend=False,
    )


def _archipelago_islands_trace(islands: list[dict[str, Any]]) -> go.Scattermapbox:
    return go.Scattermapbox(
        lon=[i["lon"] for i in islands],
        lat=[i["lat"] for i in islands],
        mode="markers",
        marker=dict(size=8, color=NV_GREEN_DARK),
        text=[f"{i['name_vi']} / {i['name_en']}" for i in islands],
        hovertemplate="<b>%{text}</b><extra></extra>",
        showlegend=False,
    )


def add_archipelago_overlays(fig: go.Figure) -> go.Figure:
    """Append the dual-archipelago declaration: outlines, principal-island
    markers, and bilingual labels (in that trace order so Mapbox-GL's
    symbol-collision pruning keeps the labels visible).
    """
    for meta in (HOANG_SA, TRUONG_SA):
        fig.add_trace(_archipelago_outline_trace(meta))
        fig.add_trace(_archipelago_islands_trace(meta["islands"]))
    # Labels go LAST — Mapbox-GL won't prune them in favour of an earlier
    # city label.
    for meta in (HOANG_SA, TRUONG_SA):
        fig.add_trace(_archipelago_label_trace(meta))
    return fig


# ---------------------------------------------------------------------------
# City / island leader-line overlays — same CAD-elbow technique as personas-vn
# ---------------------------------------------------------------------------
def _leader_line_traces(
    items: list[dict[str, Any]],
    *,
    marker_size: int = 11,
    marker_color: str = NV_GREEN,
    text_size: int = 11,
    text_color: str = NV_DARK,
    line_color: str = "rgba(0,0,0,0.45)",
    line_width: float = 0.7,
    hover_extras: list[str] | None = None,
) -> list[go.Scattermapbox]:
    """Render each item as: marker at (lon, lat) + thin leader line + label
    at (label_lon, label_lat). Standard cartographic offset-label technique.
    """
    if not items:
        return []
    line_lons: list[float | None] = []
    line_lats: list[float | None] = []
    marker_lons: list[float] = []
    marker_lats: list[float] = []
    text_right_lons: list[float] = []
    text_right_lats: list[float] = []
    text_right_str: list[str] = []
    text_left_lons: list[float] = []
    text_left_lats: list[float] = []
    text_left_str: list[str] = []
    text_center_lons: list[float] = []
    text_center_lats: list[float] = []
    text_center_str: list[str] = []
    hover_lines: list[str] = []

    for idx, it in enumerate(items):
        mlon, mlat = it["lon"], it["lat"]
        llon = it.get("label_lon", mlon)
        llat = it.get("label_lat", mlat)
        marker_lons.append(mlon)
        marker_lats.append(mlat)
        hover_lines.append((hover_extras[idx] if hover_extras
                              else f"<b>{it['name_vi']}</b>")
                            + "<extra></extra>")

        dlon = llon - mlon
        dlat = llat - mlat

        if abs(dlon) < 0.05 and abs(dlat) < 0.05:
            text_center_lons.append(llon)
            text_center_lats.append(llat)
            text_center_str.append(it["name_vi"])
            continue
        if abs(dlon) < 0.05:
            line_lons.extend([mlon, llon, None])
            line_lats.extend([mlat, llat, None])
            text_center_lons.append(llon)
            text_center_lats.append(llat)
            text_center_str.append(it["name_vi"])
        elif abs(dlat) < 0.05:
            line_lons.extend([mlon, llon, None])
            line_lats.extend([mlat, llat, None])
            (text_right_lons if dlon > 0 else text_left_lons).append(llon)
            (text_right_lats if dlon > 0 else text_left_lats).append(llat)
            (text_right_str if dlon > 0 else text_left_str).append(it["name_vi"])
        else:
            # CAD elbow: vertical from marker to (mlon, llat), then horizontal.
            line_lons.extend([mlon, mlon, llon, None])
            line_lats.extend([mlat, llat, llat, None])
            (text_right_lons if dlon > 0 else text_left_lons).append(llon)
            (text_right_lats if dlon > 0 else text_left_lats).append(llat)
            (text_right_str if dlon > 0 else text_left_str).append(it["name_vi"])

    def _text_trace(lons, lats, txts, position):
        return go.Scattermapbox(
            lon=lons, lat=lats, mode="markers+text",
            text=txts,
            textfont=dict(color=text_color, size=text_size),
            textposition=position,
            marker=dict(size=1, color="rgba(0,0,0,0)"),
            hoverinfo="skip", showlegend=False,
        )

    out: list[go.Scattermapbox] = []
    if line_lons:
        out.append(go.Scattermapbox(
            lon=line_lons, lat=line_lats, mode="lines",
            line=dict(color=line_color, width=line_width),
            hoverinfo="skip", showlegend=False,
        ))
    out.append(go.Scattermapbox(
        lon=marker_lons, lat=marker_lats, mode="markers",
        marker=dict(size=marker_size, color=marker_color),
        hovertemplate=hover_lines, showlegend=False,
    ))
    if text_right_str:
        out.append(_text_trace(text_right_lons, text_right_lats,
                                text_right_str, "middle right"))
    if text_left_str:
        out.append(_text_trace(text_left_lons, text_left_lats,
                                text_left_str, "middle left"))
    if text_center_str:
        out.append(_text_trace(text_center_lons, text_center_lats,
                                text_center_str, "middle center"))
    return out


def _cities_traces() -> list[go.Scattermapbox]:
    capital_items = [{**c, "name_vi": f"★ {c['name_vi']}"}
                       for c in NOTABLE_CITIES if c.get("capital")]
    other_items = [c for c in NOTABLE_CITIES if not c.get("capital")]
    out: list[go.Scattermapbox] = []
    if capital_items:
        cap_hover = [
            f"<b>{c['name_vi']} / {c['name_en']}</b><br>{c['role']}<br>"
            f"({c['lon']:.2f}°E, {c['lat']:.2f}°N)"
            for c in NOTABLE_CITIES if c.get("capital")
        ]
        out.extend(_leader_line_traces(
            capital_items,
            marker_size=15, marker_color=NV_BLACK,
            text_size=13,   text_color=NV_BLACK,
            hover_extras=cap_hover,
        ))
    if other_items:
        other_hover = [
            f"<b>{c['name_vi']} / {c['name_en']}</b><br>{c['role']}<br>"
            f"({c['lon']:.2f}°E, {c['lat']:.2f}°N)"
            for c in other_items
        ]
        out.extend(_leader_line_traces(
            other_items,
            marker_size=10, marker_color=NV_BLACK,
            text_size=11,   text_color=NV_BLACK,
            hover_extras=other_hover,
        ))
    return out


def _islands_traces() -> list[go.Scattermapbox]:
    hover = [
        f"<b>{i['name_vi']} / {i['name_en']}</b><br>{i['note']}<br>"
        f"({i['lon']:.2f}°E, {i['lat']:.2f}°N)"
        for i in NOTABLE_ISLANDS
    ]
    return _leader_line_traces(
        NOTABLE_ISLANDS,
        marker_color=NV_GREEN, text_color=NV_DARK, hover_extras=hover,
    )


def add_overlays(fig: go.Figure) -> go.Figure:
    """Append the full cartographic overlay stack: archipelagos first
    (under everything), then the named-island leaders, then the cities,
    then the archipelago labels (last, so Mapbox-GL doesn't prune them).
    """
    # 1) Archipelago outlines + island markers (no labels yet)
    for meta in (HOANG_SA, TRUONG_SA):
        fig.add_trace(_archipelago_outline_trace(meta))
        fig.add_trace(_archipelago_islands_trace(meta["islands"]))
    # 2) Named islands
    for t in _islands_traces():
        fig.add_trace(t)
    # 3) Capital + cities
    for t in _cities_traces():
        fig.add_trace(t)
    # 4) Archipelago labels last
    for meta in (HOANG_SA, TRUONG_SA):
        fig.add_trace(_archipelago_label_trace(meta))
    return fig


# ---------------------------------------------------------------------------
# Choropleth + scatter builders
# ---------------------------------------------------------------------------
GEO = load_vietnam_geojson()
_PROVINCE_NAMES = [f["properties"]["shapeName"] for f in GEO["features"]
                    if not f["properties"].get("is_archipelago")]


def _base_outline_trace() -> go.Choroplethmapbox:
    """Always-on transparent choropleth covering every province + the two
    archipelago boxes — guarantees a visible border on every polygon
    regardless of whether the data trace covered it.
    """
    return go.Choroplethmapbox(
        geojson=GEO,
        locations=[f["properties"]["shapeName"] for f in GEO["features"]],
        z=[0] * len(GEO["features"]),
        featureidkey="properties.shapeName",
        colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
        marker=dict(line=dict(color=NV_BLACK, width=0.8), opacity=1.0),
        showscale=False, hoverinfo="skip",
    )


def build_choropleth(values: pd.DataFrame, *, title: str,
                       colorbar_title: str, hover_unit: str = "",
                       colorscale=NV_SEQUENTIAL) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(_base_outline_trace())
    fig.add_trace(go.Choroplethmapbox(
        geojson=GEO,
        locations=values["province"],
        z=values["value"],
        featureidkey="properties.shapeName",
        colorscale=colorscale,
        marker=dict(line=dict(color=NV_BLACK, width=0.8), opacity=0.85),
        colorbar=dict(
            title=dict(text=colorbar_title,
                        font=dict(family=MAP_FONT, color=NV_BLACK, size=12),
                        side="right"),
            tickfont=dict(family=NV_FONT_FAMILY, color=NV_BLACK, size=11),
            outlinecolor=NV_LIGHT_GREY, outlinewidth=0.5,
            **_COLORBAR_KW,
        ),
        hovertemplate=("<b>%{location}</b><br>%{z:,.2f}"
                        + (" " + hover_unit if hover_unit else "")
                        + "<extra></extra>"),
    ))
    add_overlays(fig)
    fig.update_layout(**vietnam_map_layout(title=title))
    return fig


def build_scatter(points: pd.DataFrame, *, title: str,
                    colour_col: str = "macro_region_en",
                    size: int = 4, opacity: float = 0.55) -> go.Figure:
    """Scatter the post-merger commune centroids (or committee points) over
    the same Vietnam canvas with full overlay stack.
    """
    fig = go.Figure()
    fig.add_trace(_base_outline_trace())
    region_colours = {
        "Northern Midlands and Mountain Areas": "#1F4E79",
        "Red River Delta":                      NV_GREEN_DARK,
        "North Central and Central Coastal Areas": "#C97C00",
        "Central Highlands":                    "#7F4E2C",
        "Southeast":                            NV_BLACK,
        "Mekong River Delta":                   NV_GREEN,
        "Other":                                NV_LIGHT_GREY,
    }
    for region, hex_colour in region_colours.items():
        sub = points[points[colour_col] == region]
        if sub.empty:
            continue
        fig.add_trace(go.Scattermapbox(
            lon=sub["centroid_lon"], lat=sub["centroid_lat"],
            mode="markers",
            marker=dict(size=size, color=hex_colour, opacity=opacity),
            name=region,
            hovertext=sub["ten"] + " (" + sub["parent_ten"].fillna("") + ")",
            hovertemplate="<b>%{hovertext}</b><extra></extra>",
        ))
    add_overlays(fig)
    fig.update_layout(
        **vietnam_map_layout(title=title),
        legend=dict(
            font=dict(family=MAP_FONT, color=NV_BLACK, size=11),
            **_LEGEND_KW,
        ),
    )
    return fig


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def _frame_provinces(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    sub = df[(df["kind"] == "province") & df[metric].notna()].copy()
    sub["province"] = sub["ten"].map(normalise_province_name)
    return sub.rename(columns={metric: "value"})[["province", "value"]]


def run_all(*, root: Path, out_dir: Path) -> dict[str, Any]:
    extracted = root / "extracted" / "extracted.parquet"
    if not extracted.exists():
        raise FileNotFoundError(
            f"missing {extracted}; run `geography-vn curate` first.")

    df = pd.read_parquet(extracted)
    df["macro_region_en"] = df["macro_region"].map(MACRO_REGION_EN).fillna("Other")
    log.info("loaded %d rows from %s", len(df), extracted)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 01 — population choropleth
    pop = _frame_provinces(df, "population")
    fig = build_choropleth(
        pop, title="Vietnam — Population by post-merger province (2024)",
        colorbar_title="people", hover_unit="people")
    save_figure(fig, "01_provinces_population", width=FIG_W_MAP, height=FIG_H_MAP,
                  out_dir=out_dir)

    # 02 — density choropleth
    den = _frame_provinces(df, "density")
    fig = build_choropleth(
        den, title="Vietnam — Population density (people per km²)",
        colorbar_title="people / km²", hover_unit="people / km²")
    save_figure(fig, "02_provinces_density", width=FIG_W_MAP, height=FIG_H_MAP,
                  out_dir=out_dir)

    # 03 — area choropleth
    area = _frame_provinces(df, "area_km2")
    fig = build_choropleth(
        area, title="Vietnam — Land area by post-merger province",
        colorbar_title="km²", hover_unit="km²")
    save_figure(fig, "03_provinces_area", width=FIG_W_MAP, height=FIG_H_MAP,
                  out_dir=out_dir)

    # 04 — commune centroids scatter
    com = df[(df["kind"] == "commune") & df["centroid_lat"].notna()].copy()
    com["parent_ten"] = com["parent_ten"].fillna("")
    fig = build_scatter(
        com, title=f"3,321 communes by centroid (post-merger; n = {len(com):,})",
        size=3, opacity=0.5)
    save_figure(fig, "04_communes_scatter", width=FIG_W_MAP, height=FIG_H_MAP,
                  out_dir=out_dir)

    # 05 — committees scatter
    cmte = df[(df["kind"] == "committee") & df["centroid_lat"].notna()].copy()
    cmte["parent_ten"] = cmte["parent_ten"].fillna("")
    fig = build_scatter(
        cmte, title=f"3,357 commune people's committees by registered location "
                     f"(n = {len(cmte):,})",
        size=4, opacity=0.6)
    save_figure(fig, "05_committees_scatter", width=FIG_W_MAP, height=FIG_H_MAP,
                  out_dir=out_dir)

    n = len(list(out_dir.glob("*.png")))
    log.info("rendered %d map figures into %s", n, out_dir)
    return {"out_dir": str(out_dir), "n_figures": n}


def main() -> None:
    p = argparse.ArgumentParser(description="Render Vietnam map figures with "
                                              "the dual-archipelago declaration.")
    p.add_argument("--config", default="configs/curator.yaml",
                   help="curator config (default: configs/curator.yaml)")
    p.add_argument("--root", default=None, help="curator artefact root override")
    p.add_argument("--out", default=None, help="output directory override")
    args = p.parse_args()

    cfg = load_config(args.config)
    root_str = args.root or str(cfg.dataset.root)
    out_str = args.out or str(cfg.viz.get("maps_dir") or "docs/figures/maps")
    root = REPO_ROOT / root_str if not Path(root_str).is_absolute() else Path(root_str)
    out_dir = REPO_ROOT / out_str if not Path(out_str).is_absolute() else Path(out_str)
    summary = run_all(root=root, out_dir=ensure_dir(out_dir))
    import json
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
