"""Plotly style: NVIDIA brand palette + LaTeX-serif typography on white.

Design contract
---------------
* **White canvas** — every figure ships on ``#FFFFFF``. No off-white, no
  grey, nothing that can collide with a paper / dataset-card background.
* **NVIDIA Green** (``#76B900``) is the primary data-series colour and the
  colorbar high-anchor. We never recolour the brand wordmark.
* **LaTeX-style serif typography** — chart titles, axis labels, legend
  text, hover tooltips all default to a Computer-Modern-style serif font
  fallback chain so the rendered figures read like LaTeX paper figures.
  When a viewer has NVIDIA Sans installed (NVIDIA staff machines) the
  numerics still use it for tabular legibility — see
  :data:`NV_FONT_FAMILY` vs :data:`SERIF_FONT_FAMILY`.

The two-axis font setup mirrors ``matplotlib.usetex=True`` figures: serif
text everywhere, and crisp tabular numerals for ticks. NVIDIA Sans is the
proprietary brand face; we list it first in the body / hover font chain so
NVIDIA viewers see the real thing, and gracefully degrade to Inter and
Helvetica Neue otherwise.

Usage
-----
::

    import plotly.express as px
    from packages.viz.style import apply_nvidia_latex_style, save_figure, NV_GREEN
    fig = px.bar(df, x="region", y="population")
    fig.update_traces(marker_color=NV_GREEN)
    apply_nvidia_latex_style(fig)
    save_figure(fig, "01_population_by_region")
    fig.show()
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from packages.common.paths import REPO_ROOT, ensure_dir

# kaleido + its choreographer (headless-Chrome) backend log a steady stream
# of INFO lines on every ``fig.write_image`` call. Mute them once, here, so
# every consumer of ``save_figure`` gets a quiet kaleido by default.
for _lib in ("kaleido", "choreographer", "logistro"):
    logging.getLogger(_lib).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Brand palette (NVIDIA logo & brand guidelines)
# ---------------------------------------------------------------------------
NV_GREEN       = "#76B900"   # NVIDIA wordmark green — primary series colour
NV_GREEN_DARK  = "#5C9300"   # press-release "deep green" for hovers / focus
NV_GREEN_SOFT  = "#B5D88A"   # 50% tint for fills / area under curve
NV_BLACK       = "#000000"   # body text + axis tick labels
NV_DARK        = "#1A1A1A"   # near-black for chart titles
NV_GREY        = "#666666"   # secondary text / faint chrome
NV_LIGHT_GREY  = "#CCCCCC"   # axis lines, table borders
NV_FAINT       = "#F5F5F5"   # background fill for separators
NV_WHITE       = "#FFFFFF"   # canvas

# Categorical sequence (multi-series plots) — green leads, then a curated
# greyscale ramp + a single accent black. Matches the ``personas-vn``
# palette so figures from both repos can sit side-by-side in a deck.
NV_DISCRETE = [
    NV_GREEN,
    NV_BLACK,
    "#888888",
    NV_GREEN_SOFT,
    "#444444",
    NV_LIGHT_GREY,
]

# Sequential scale for choropleths / heatmaps. Bottom stop is intentionally
# NOT pure white so low-value polygons stay distinguishable from the canvas.
NV_SEQUENTIAL = [
    [0.00, "#ECF5DC"],
    [0.25, "#D4E8B0"],
    [0.50, NV_GREEN_SOFT],
    [0.75, NV_GREEN],
    [1.00, NV_GREEN_DARK],
]

# Font fallbacks — two chains, one serif (LaTeX-feeling), one sans (NVIDIA
# Sans → Inter). Plotly accepts either CSS font-family chain.
SERIF_FONT_FAMILY = (
    "Latin Modern Roman, LM Roman 10, Computer Modern, "
    "CMU Serif, STIX Two Text, STIX, Times New Roman, Times, serif"
)
NV_FONT_FAMILY = (
    "NVIDIA Sans, NVIDIASans, NVIDIA, Inter, "
    "Helvetica Neue, Helvetica, Arial, sans-serif"
)


# ---------------------------------------------------------------------------
# Canonical figure sizes — every figure ships at the SAME pixel dimensions
# so the dataset card / DATAANALYSIS.md pages don't see a viewport jump
# between figures.
# ---------------------------------------------------------------------------
# Analytical pack (bar charts, scatter, donut, UMAP). 1200 × 900 gives
# enough vertical room for the longest category list (the 34-province bar
# charts) without squashing the bars, while remaining a comfortable 4:3
# aspect for non-bar figures.
FIG_W, FIG_H = 1200, 900

# Cartographic pack. Vietnam is taller than wide (lat 8 → 24, lon 102 →
# 117), so a SQUARE canvas pushes the country to fill more of the visible
# area without the wasted side-margins of a 4:3 frame. The colorbar /
# legend column at x=0.87 carves out 13% of the width.
FIG_W_MAP, FIG_H_MAP = 1100, 1100

FIG_SCALE = 2  # 2× retina scaling on PNG export


# ---------------------------------------------------------------------------
# Theme application
# ---------------------------------------------------------------------------
_AXIS_COMMON: dict[str, Any] = dict(
    showgrid=True,
    gridcolor="rgba(0,0,0,0.08)",
    zeroline=True,
    zerolinecolor=NV_LIGHT_GREY,
    linecolor=NV_BLACK,
    linewidth=1,
    ticks="outside",
    tickcolor=NV_BLACK,
    showline=True,
    mirror=False,
    automargin=True,
)


def apply_nvidia_latex_style(fig, *, axes: bool = True) -> Any:
    """Restyle ``fig`` in place: white background, LaTeX-serif typography,
    NVIDIA palette.

    Parameters
    ----------
    fig:
        Any object that supports ``update_layout`` (a Plotly ``Figure``).
    axes:
        ``True`` (default) shows axes with the standard NVIDIA grid + tick
        treatment; ``False`` hides them entirely (graphs / network plots).
    """
    fig.update_layout(
        paper_bgcolor=NV_WHITE,
        plot_bgcolor=NV_WHITE,
        font=dict(family=SERIF_FONT_FAMILY, color=NV_BLACK, size=14),
        title=dict(
            font=dict(family=SERIF_FONT_FAMILY, color=NV_DARK, size=20),
            x=0.02, xanchor="left", y=0.96,
        ),
        legend=dict(
            font=dict(family=SERIF_FONT_FAMILY, color=NV_BLACK, size=12),
            bgcolor="rgba(255,255,255,0.92)",
            bordercolor=NV_LIGHT_GREY,
            borderwidth=1,
        ),
        colorway=NV_DISCRETE,
        margin=dict(l=120, r=80, t=80, b=80),
        hoverlabel=dict(
            font=dict(family=SERIF_FONT_FAMILY, color=NV_WHITE, size=12),
            bgcolor=NV_BLACK,
            bordercolor=NV_GREEN,
        ),
    )
    if axes:
        # Axes inherit the serif body font but use a slightly smaller tick
        # size — same convention as ``\caption`` / ``\label`` in LaTeX.
        axis_kw = dict(
            _AXIS_COMMON,
            tickfont=dict(family=SERIF_FONT_FAMILY, color=NV_BLACK, size=12),
            title_font=dict(family=SERIF_FONT_FAMILY, color=NV_BLACK, size=14),
        )
        fig.update_xaxes(**axis_kw)
        fig.update_yaxes(**axis_kw)
    else:
        for ax in (fig.update_xaxes, fig.update_yaxes):
            ax(showgrid=False, zeroline=False, showticklabels=False,
               showline=False, ticks="")
    # ``textposition='outside'`` on horizontal bars otherwise gets clipped at
    # the chart edge for the longest-value bar — disable axis-clipping for
    # bar traces so the value annotations always render fully.
    fig.update_traces(cliponaxis=False, selector=dict(type="bar"))
    return fig


def register_plotly_template(name: str = "nvidia_latex") -> str:
    """Register an opinionated Plotly template named ``name`` and return it.

    Useful when you want every default ``px.bar(…)`` / ``go.Figure()`` in a
    notebook session to start with the NVIDIA + LaTeX-serif look without
    having to remember to call :func:`apply_nvidia_latex_style` on each
    figure. Callers that DO want fine-grained styling can still call the
    function explicitly afterwards.
    """
    import plotly.graph_objects as go
    import plotly.io as pio

    pio.templates[name] = go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=NV_WHITE,
            plot_bgcolor=NV_WHITE,
            font=dict(family=SERIF_FONT_FAMILY, color=NV_BLACK, size=14),
            colorway=NV_DISCRETE,
            title=dict(
                font=dict(family=SERIF_FONT_FAMILY, color=NV_DARK, size=20),
                x=0.02, xanchor="left", y=0.96,
            ),
        ),
    )
    pio.templates.default = name
    return name


# ---------------------------------------------------------------------------
# Save helpers — mirror personas-vn's ``scripts._nvidia_style.save_figure``
# ---------------------------------------------------------------------------
def _in_jupyter_kernel() -> bool:
    """Return ``True`` iff this code is executing inside a Jupyter kernel.

    Used to gate the Mapbox-GL + kaleido auto-skip heuristic below.
    """
    try:
        from IPython import get_ipython  # type: ignore
    except Exception:
        return False
    ipy = get_ipython()
    if ipy is None:
        return False
    return type(ipy).__name__ == "ZMQInteractiveShell"


_MAPBOX_PREFIXES = (
    "choroplethmapbox", "scattermapbox", "densitymapbox",  # plotly < 5.24 (Mapbox-GL)
    "choroplethmap",   "scattermap",   "densitymap",       # plotly >= 5.24 (MapLibre)
)


def save_figure(
    fig,
    name: str,
    *,
    width: int = FIG_W,
    height: int = FIG_H,
    scale: int = FIG_SCALE,
    write_png: bool = True,
    write_html: bool = True,
    out_dir: str | Path | None = None,
) -> tuple[Path | None, Path | None]:
    """Persist a styled Plotly figure to ``<out_dir>/<name>.{png,html}``.

    The PNG is what ``DATAANALYSIS.md`` / the HF dataset card embed; the
    HTML is the fully-interactive Plotly version for local inspection.

    Defensive against two known kaleido-Mapbox-GL pitfalls:

    1. If ``fig`` carries Mapbox / MapLibre traces AND we're inside a
       Jupyter kernel, PNG export is skipped automatically (kaleido's
       headless Chromium loses its Mapbox-GL worker in nbconvert).
       The standalone process used by ``scripts/render_maps.py`` does
       not have that limitation.
    2. Any other ``write_image`` exception is logged and swallowed so the
       cell still produces an interactive HTML companion via ``fig.show()``.
    """
    from packages.common.logging import get_logger

    log = get_logger(__name__)

    target_dir = ensure_dir(Path(out_dir) if out_dir
                              else REPO_ROOT / "docs" / "figures" / "analysis")

    has_mapbox = any(
        (getattr(t, "type", "") or "").lower().startswith(_MAPBOX_PREFIXES)
        for t in (getattr(fig, "data", None) or ())
    )
    if has_mapbox and write_png and _in_jupyter_kernel():
        log.warning(
            "%s contains Mapbox/MapLibre trace(s) and we're in a Jupyter "
            "kernel — skipping PNG export (kaleido + Mapbox-GL fails here; "
            "use `python -m scripts.render_maps` for static PNGs)", name)
        write_png = False

    # Pin the figure's rendered dimensions so the HTML and the PNG ship at
    # the EXACT same size. Without this the HTML rendering uses whatever
    # the browser viewport happens to be while the PNG uses ``width × height``,
    # producing inconsistent screenshots between the two formats.
    fig.update_layout(width=width, height=height)

    png_path: Path | None = None
    if write_png:
        png_path = target_dir / f"{name}.png"
        try:
            fig.write_image(png_path, width=width, height=height, scale=scale)
        except Exception as exc:
            log.warning("PNG export failed for %s (%s: %s) — keeping HTML only",
                         name, type(exc).__name__, exc)
            png_path = None

    html_path = target_dir / f"{name}.html" if write_html else None
    if html_path is not None:
        fig.write_html(html_path, include_plotlyjs="cdn")
    return png_path, html_path
