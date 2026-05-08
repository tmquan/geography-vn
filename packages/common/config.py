"""YAML config loader, backed by **OmegaConf**.

OmegaConf gives us, for free, what we previously hand-rolled in the
``Config`` mapping wrapper, plus three things the in-house version did not:

1. **Variable interpolation.** ``${dataset.root}/raw`` evaluates lazily
   against the loaded config tree, so paths derived from a single root
   cannot drift apart. ``${oc.env:GEOGRAPHY_VN_API_KEY}`` pulls from the
   environment with a default fallback.
2. **Multi-file merge.** ``load_config(path, *overrides)`` deep-merges any
   number of files in CLI order so ``--config-overrides defaults.yaml
   curator.yaml local.yaml`` works.
3. **Structured-config validation** for callers who want it: the
   :class:`Config` type alias is just OmegaConf's :class:`DictConfig`, so
   user code can hand it a typed dataclass with ``OmegaConf.merge`` and
   pick up runtime type checks for free.

The public API is intentionally backward-compatible with the previous
hand-rolled version: ``cfg.dataset.root``, ``cfg["download"]``,
``cfg.get("max_records")``, ``isinstance(cfg, Mapping)`` all still work
(``DictConfig`` implements the ``MutableMapping`` protocol).
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from omegaconf import DictConfig, ListConfig, OmegaConf

from packages.common.paths import resolve

# Re-export so callers can write ``from packages.common.config import Config``
# and treat it as the canonical type.
Config = DictConfig


def load_config(*paths: str | Path) -> Config:
    """Load one or more YAML files into a merged :class:`DictConfig`.

    The first positional argument is the base config; every subsequent
    argument is deep-merged on top in order. Variable interpolation
    (``${section.key}`` and ``${oc.env:VAR,default}``) resolves against
    the merged tree.
    """
    if not paths:
        raise TypeError("load_config() requires at least one path")
    configs: list[DictConfig | ListConfig] = []
    for path in paths:
        full = resolve(path)
        if not full.exists():
            raise FileNotFoundError(f"Config not found: {full}")
        loaded = OmegaConf.load(full)
        if not isinstance(loaded, DictConfig):
            raise ValueError(
                f"Config root must be a mapping, got {type(loaded).__name__}: {full}"
            )
        configs.append(loaded)
    merged = OmegaConf.merge(*configs)
    if not isinstance(merged, DictConfig):
        raise ValueError("merged config is not a mapping")
    return merged


def to_container(cfg: Config | Any) -> dict[str, Any]:
    """Resolve interpolations and return a plain ``dict`` (for json.dump etc.)."""
    if isinstance(cfg, (DictConfig, ListConfig)):
        return OmegaConf.to_container(cfg, resolve=True)  # type: ignore[return-value]
    if isinstance(cfg, Mapping):
        return dict(cfg)
    raise TypeError(f"to_container: unsupported type {type(cfg).__name__}")


__all__ = ["Config", "load_config", "to_container", "OmegaConf"]
