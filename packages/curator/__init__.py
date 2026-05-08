"""NeMo Curator-style 5-stage pipeline.

Five lightweight stages, each implementing the same lifecycle as a
``nemo_curator.core.stage.ProcessingStage`` (``setup`` / ``run`` / ``teardown``):

    download -> parse -> extract -> embed -> reduce

Driven by :class:`packages.curator.pipeline.CurationPipeline`, which can run
the stages either via an in-house sequential executor or via NeMo Curator's
``InProcessExecutor`` when the optional dep is installed.
"""

from packages.curator.pipeline import (
    CurationArtefacts,
    CurationConfig,
    CurationPipeline,
    load_config,
    run_curation,
)
from packages.curator.regions import MACRO_REGION_EN, MACRO_REGION_VI, province_to_region
from packages.curator.stages import (
    DownloadStage,
    EmbedStage,
    ExtractStage,
    ParseStage,
    ReduceStage,
)

__all__ = [
    "CurationArtefacts",
    "CurationConfig",
    "CurationPipeline",
    "load_config",
    "run_curation",
    "DownloadStage",
    "ParseStage",
    "ExtractStage",
    "EmbedStage",
    "ReduceStage",
    "MACRO_REGION_VI",
    "MACRO_REGION_EN",
    "province_to_region",
]
