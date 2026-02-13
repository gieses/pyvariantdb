"""Define constants used in pyvariantdb."""

import os
from loguru import logger
from pathlib import Path


def get_cache_dir() -> None:
    """Return the root directory of the project."""
    # check if env var PYVARIANTDB_HOME exists, otherwise ~/.cache/pyvariantdb
    cache_dir = os.getenv("PYVARIANTDB_HOME", Path().home() / ".cache" / "pyvariantdb")
    cache_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Cache directory: {cache_dir}")
    return cache_dir
