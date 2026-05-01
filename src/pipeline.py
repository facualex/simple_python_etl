"""ETL pipeline entrypoint.

Running this module executes the end-to-end workflow:
extract (download) → transform → load.
"""

import logging

from dotenv import load_dotenv

from extract import extract
from load import load
from logging_config import configure_logging
from transform import transform

load_dotenv()
configure_logging()

logger = logging.getLogger(__name__)


def pipeline():
    """Run the full ETL pipeline once."""
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


if __name__ == "__main__":
    pipeline()
    logger.info("Pipeline completed")
