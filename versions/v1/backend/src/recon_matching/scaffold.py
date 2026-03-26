"""
Phase 1 — Run directory scaffolding.

Creates RUN_DIR on disk and validates KB_DIR before boot proceeds.
"""

import shutil
from pathlib import Path

from loguru import logger

from .paths import validate_kb_dir


class ScaffoldError(Exception):
    pass


def create_run_dir(run_dir: Path) -> Path:
    """Create RUN_DIR on disk. Returns the path."""
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Created RUN_DIR: {}", run_dir)
    return run_dir


def validate_and_prepare(kb_dir: Path, run_dir: Path) -> None:
    """
    Validate KB_DIR has all required files, then create RUN_DIR.
    Raises ScaffoldError on validation failure.
    """
    missing = validate_kb_dir(kb_dir)
    if missing:
        raise ScaffoldError(f"KB validation failed. Missing: {missing}")

    create_run_dir(run_dir)
    logger.info("Scaffold complete: KB_DIR={}, RUN_DIR={}", kb_dir, run_dir)
