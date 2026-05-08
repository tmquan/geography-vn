"""Curator pipeline orchestrator (NeMo Curator-compatible).

Two execution backends:

1. **Local sequential** (default; what the tests exercise).
2. **NeMo Curator** ``Pipeline`` + ``InProcessExecutor`` when ``nemo_curator``
   is importable. Both backends read / write the same on-disk shape, so the
   visualizer / analysis notebook do not care which one ran.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from packages.common.config import Config
from packages.common.config import load_config as _load_yaml
from packages.common.logging import get_logger
from packages.common.paths import ensure_dir
from packages.curator.stages import (
    DownloadStage,
    EmbedStage,
    ExtractStage,
    ParseStage,
    ReduceStage,
)

log = get_logger(__name__)


@dataclass
class CurationConfig:
    name: str
    root: Path
    raw_dir: Path
    parsed_dir: Path
    extracted_dir: Path
    embedded_dir: Path
    reduced_dir: Path
    download: Config
    parse: Config
    extract: Config
    embed: Config
    reduce: Config

    @classmethod
    def from_yaml(cls, *paths: str | Path) -> CurationConfig:
        """Load (and merge, if multiple paths are given) the curator YAML.

        Sub-paths under ``dataset.*_dir`` use OmegaConf interpolation so a
        single ``dataset.root`` change cascades to every artefact location
        without manual fix-up.
        """
        if not paths:
            paths = ("configs/curator.yaml",)
        cfg = _load_yaml(*paths)
        ds = cfg.dataset
        return cls(
            name=str(ds.name),
            root=ensure_dir(str(ds.root)),
            raw_dir=ensure_dir(str(ds.get("raw_dir") or f"{ds.root}/raw")),
            parsed_dir=ensure_dir(str(ds.get("parsed_dir") or f"{ds.root}/parsed")),
            extracted_dir=ensure_dir(str(ds.get("extracted_dir") or f"{ds.root}/extracted")),
            embedded_dir=ensure_dir(str(ds.get("embedded_dir") or f"{ds.root}/embedded")),
            reduced_dir=ensure_dir(str(ds.get("reduced_dir") or f"{ds.root}/reduced")),
            download=cfg.download,
            parse=cfg.parse,
            extract=cfg.extract,
            embed=cfg.embed,
            reduce=cfg.reduce,
        )


@dataclass
class CurationArtefacts:
    root: Path
    manifest_path: Path
    parsed_path: Path | None = None
    extracted_path: Path | None = None
    embedded_path: Path | None = None
    reduced_path: Path | None = None


class CurationPipeline:
    STAGE_ORDER = ("download", "parse", "extract", "embed", "reduce")

    def __init__(self, config: CurationConfig) -> None:
        self.cfg = config

    def _build_stages(self) -> dict[str, Any]:
        return {
            "download": DownloadStage(self.cfg.download, self.cfg.raw_dir),
            "parse":    ParseStage(self.cfg.parse, self.cfg.raw_dir, self.cfg.parsed_dir),
            "extract":  ExtractStage(self.cfg.extract, self.cfg.parsed_dir, self.cfg.extracted_dir),
            "embed":    EmbedStage(self.cfg.embed, self.cfg.extracted_dir, self.cfg.embedded_dir),
            "reduce":   ReduceStage(self.cfg.reduce, self.cfg.embedded_dir, self.cfg.reduced_dir),
        }

    def run(
        self,
        *,
        only: list[str] | None = None,
        skip: list[str] | None = None,
        backend: str = "local",
    ) -> CurationArtefacts:
        if backend == "nemo_curator":
            return self._run_nemo_curator(only=only, skip=skip)
        return self._run_local(only=only, skip=skip)

    def _run_local(
        self,
        *,
        only: list[str] | None,
        skip: list[str] | None,
    ) -> CurationArtefacts:
        stages = self._build_stages()
        manifest: dict[str, Any] = {
            "dataset":    self.cfg.name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "backend":    "local",
            "stages":     {},
        }
        for name in self.STAGE_ORDER:
            if only and name not in only:
                continue
            if skip and name in skip:
                continue
            stage = stages[name]
            log.info(">>> stage: %s", name)
            stage.setup()
            try:
                summary = stage.run()
            except Exception as exc:
                manifest["stages"][name] = {"status": "error", "error": str(exc)}
                self._write_manifest(manifest)
                raise
            finally:
                stage.teardown()
            manifest["stages"][name] = {"status": "ok", **(summary or {})}

        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        manifest_path = self._write_manifest(manifest)
        return CurationArtefacts(
            root=self.cfg.root,
            manifest_path=manifest_path,
            parsed_path=self.cfg.parsed_dir / "parsed.jsonl",
            extracted_path=self.cfg.extracted_dir / "extracted.parquet",
            embedded_path=self.cfg.embedded_dir / "embedded.parquet",
            reduced_path=self.cfg.reduced_dir / "reduced.parquet",
        )

    def _run_nemo_curator(
        self,
        *,
        only: list[str] | None,
        skip: list[str] | None,
    ) -> CurationArtefacts:
        try:
            from nemo_curator.core.pipeline import Pipeline as NCPipeline
            from nemo_curator.core.stage import ProcessingStage as NCProcessingStage
            from nemo_curator.tasks import DocumentBatch as NCDocumentBatch
        except ImportError as exc:
            log.warning("NeMo Curator not available (%s); falling back to local", exc)
            return self._run_local(only=only, skip=skip)

        stages = self._build_stages()
        manifest: dict[str, Any] = {
            "dataset":    self.cfg.name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "backend":    "nemo_curator",
            "stages":     {},
        }

        nc_pipeline = NCPipeline(name=f"{self.cfg.name}-curator")
        wrappers: list[tuple[str, Any]] = []
        for name in self.STAGE_ORDER:
            if only and name not in only:
                continue
            if skip and name in skip:
                continue
            wrapper = _wrap_for_nemo_curator(name, stages[name],
                                              NCProcessingStage, NCDocumentBatch)
            nc_pipeline.add_stage(wrapper)
            wrappers.append((name, wrapper))

        try:
            from nemo_curator.backends.experimental.in_process import InProcessExecutor

            executor = InProcessExecutor()
            nc_pipeline.run(executor=executor)
        except Exception as exc:
            log.warning("NeMo Curator pipeline crashed (%s); falling back to local", exc)
            return self._run_local(only=only, skip=skip)

        for name, wrapper in wrappers:
            manifest["stages"][name] = {"status": "ok", **(wrapper.summary or {})}
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        manifest_path = self._write_manifest(manifest)
        return CurationArtefacts(
            root=self.cfg.root,
            manifest_path=manifest_path,
            parsed_path=self.cfg.parsed_dir / "parsed.jsonl",
            extracted_path=self.cfg.extracted_dir / "extracted.parquet",
            embedded_path=self.cfg.embedded_dir / "embedded.parquet",
            reduced_path=self.cfg.reduced_dir / "reduced.parquet",
        )

    def _write_manifest(self, manifest: dict[str, Any]) -> Path:
        path = self.cfg.root / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                         encoding="utf-8")
        return path


def _wrap_for_nemo_curator(name: str, local_stage: Any, NCProcessingStage, NCDocumentBatch):
    class _Wrapper(NCProcessingStage):  # type: ignore[misc, valid-type]
        _name = f"sapnhap.{name}"

        def __init__(self) -> None:
            super().__init__()
            self.summary: dict[str, Any] = {}

        def setup(self) -> None:  # type: ignore[override]
            local_stage.setup()

        def teardown(self) -> None:  # type: ignore[override]
            local_stage.teardown()

        def process(self, task):  # type: ignore[override]
            self.summary = local_stage.run() or {}
            try:
                return NCDocumentBatch(documents=[])
            except TypeError:
                return NCDocumentBatch([])

    _Wrapper.__name__ = f"Sapnhap_{name.capitalize()}Stage"
    return _Wrapper()


def load_config(*paths: str | Path) -> CurationConfig:
    """Load (and merge) the curator YAML config.

    With no arguments this loads ``configs/curator.yaml``; pass extra paths
    to layer per-environment overrides on top, OmegaConf-style.
    """
    return CurationConfig.from_yaml(*(paths or ("configs/curator.yaml",)))


def run_curation(
    *config_paths: str | Path,
    only: list[str] | None = None,
    skip: list[str] | None = None,
    backend: str = "local",
) -> CurationArtefacts:
    cfg = load_config(*config_paths)
    return CurationPipeline(cfg).run(only=only, skip=skip, backend=backend)
