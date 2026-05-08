"""Command-line entry point for the ``geography-vn`` curator.

Usage
-----
::

    # Full run: download → parse → extract → embed → reduce.
    python -m packages.pipeline.cli curate

    # Only the download stage (full crawl, ~12 min on first run, then cached).
    python -m packages.pipeline.cli curate --only download

    # Skip the download stage (everything else, ~3 min).
    python -m packages.pipeline.cli curate --skip download

    # Use the NeMo Curator InProcessExecutor instead of the in-house executor.
    python -m packages.pipeline.cli curate --backend nemo_curator
"""

from __future__ import annotations

import argparse
import sys

from packages.common.logging import get_logger
from packages.curator.pipeline import run_curation

log = get_logger(__name__)


def _add_curate_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--config", nargs="+", default=["configs/curator.yaml"],
                   help="curator config(s) to load (deep-merged in order; "
                        "OmegaConf ${var} interpolation supported)")
    p.add_argument("--only", nargs="+", default=None,
                   help="run only these stages (e.g. --only download parse)")
    p.add_argument("--skip", nargs="+", default=None,
                   help="skip these stages (e.g. --skip embed reduce)")
    p.add_argument("--backend", default="local",
                   choices=("local", "nemo_curator"),
                   help="execution backend (default: local)")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="geography-vn",
        description="5-stage NeMo-Curator-compatible scraper for "
                    "https://sapnhap.bando.com.vn/",
    )
    sub = p.add_subparsers(dest="command", required=True)

    curate = sub.add_parser(
        "curate",
        help="Run the 5-stage download/parse/extract/embed/reduce pipeline.",
    )
    _add_curate_args(curate)

    args = p.parse_args(argv)
    if args.command == "curate":
        artefacts = run_curation(
            *args.config,
            only=args.only,
            skip=args.skip,
            backend=args.backend,
        )
        log.info("done. manifest=%s", artefacts.manifest_path)
        return 0
    p.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
