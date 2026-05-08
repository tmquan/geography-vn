"""Visualisation package: NVIDIA-styled, LaTeX-serif Plotly theme + Vietnamese
geographic helpers (post-merger 34-province polygons + the two offshore
archipelagos).

The package mirrors ``personas-vn/packages/viz/`` in spirit but is targeted
at the post-2025-merger geography (34 first-level units, 3,321 second-level
units, plus the Hoàng Sa / Trường Sa bounding-outline declarations every
Vietnamese atlas includes).
"""

from packages.viz.archipelago import (
    HOANG_SA,
    HOANG_SA_ISLANDS,
    HOANG_SA_POLYGON,
    SCATTERED_ISLAND_MARKERS,
    TRUONG_SA,
    TRUONG_SA_ISLANDS,
    TRUONG_SA_POLYGON,
    archipelago_features,
)
from packages.viz.style import (
    FIG_H,
    FIG_H_MAP,
    FIG_SCALE,
    FIG_W,
    FIG_W_MAP,
    NV_BLACK,
    NV_DARK,
    NV_DISCRETE,
    NV_FONT_FAMILY,
    NV_GREEN,
    NV_GREEN_DARK,
    NV_GREEN_SOFT,
    NV_LIGHT_GREY,
    NV_SEQUENTIAL,
    NV_WHITE,
    SERIF_FONT_FAMILY,
    apply_nvidia_latex_style,
    register_plotly_template,
    save_figure,
)
from packages.viz.vietnam_geo import (
    NOTABLE_CITIES,
    NOTABLE_ISLANDS,
    diacritic_key,
    load_vietnam_geojson,
    normalise_province_name,
)

__all__ = [
    # archipelago
    "HOANG_SA",
    "TRUONG_SA",
    "HOANG_SA_POLYGON",
    "TRUONG_SA_POLYGON",
    "HOANG_SA_ISLANDS",
    "TRUONG_SA_ISLANDS",
    "SCATTERED_ISLAND_MARKERS",
    "archipelago_features",
    # style
    "NV_GREEN",
    "NV_GREEN_DARK",
    "NV_GREEN_SOFT",
    "NV_BLACK",
    "NV_DARK",
    "NV_WHITE",
    "NV_LIGHT_GREY",
    "NV_DISCRETE",
    "NV_SEQUENTIAL",
    "NV_FONT_FAMILY",
    "SERIF_FONT_FAMILY",
    "FIG_W", "FIG_H", "FIG_W_MAP", "FIG_H_MAP", "FIG_SCALE",
    "apply_nvidia_latex_style",
    "save_figure",
    "register_plotly_template",
    # vietnam_geo
    "NOTABLE_CITIES",
    "NOTABLE_ISLANDS",
    "load_vietnam_geojson",
    "normalise_province_name",
    "diacritic_key",
]
