"""Load step of the ETL pipeline.

This module persists the transformed dataset to disk (processed zone).
"""

import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def load(
    data: Tuple[pd.DataFrame, Path],
    destination: Path = Path("data/processed/"),
) -> Path:
    """Persist the transformed dataframe to the processed data directory.

    The output file name is derived from the original raw file name:
    `prcsd_<raw_filename>.gzip`.

    Args:
        data: A tuple `(df, raw_path)` where `df` is the transformed dataset and
            `raw_path` is the path of the raw source file used to build it.
        destination: Output directory where the processed file will be written.

    Raises:
        OSError: If the destination cannot be created or the file cannot be written.
    """
    logger.info("Loading transformed data...")

    df, raw_path = data

    output_dir = Path(destination)

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        logger.exception("Failed to prepare destination path: %s", output_dir)
        raise

    load_path = output_dir / f"prcsd_{raw_path.name}.gzip"

    try:
        df.to_parquet(load_path, compression="gzip")
    except OSError:
        logger.exception("Failed writing processed file to %s", load_path)
        raise

    logger.info(
        "File saved: %s (%.1f MB)",
        load_path,
        load_path.stat().st_size / 1e6,
    )

    return load_path
