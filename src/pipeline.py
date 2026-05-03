"""ETL pipeline entrypoint.

Running this module executes the end-to-end workflow:
extract (download) → transform → load.
"""

import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

from extract import extract
from load import load
from logging_config import configure_logging
from run_context import RunContext
from transform import transform

load_dotenv()
configure_logging()

logger = logging.getLogger(__name__)


def pipeline():
    """Run the full ETL pipeline once."""
    ctx = RunContext()

    try:
        raw_path, source_url = extract()
        ctx.input_url = source_url
        ctx.raw_path = str(raw_path)

        transformed_data, _, rows_in, dropped = transform(raw_path)
        ctx.rows_in = rows_in
        ctx.rows_out = len(transformed_data)
        ctx.dropped_by_reason = dropped
        ctx.pickup_min = str(transformed_data["tpep_pickup_datetime"].min())
        ctx.pickup_max = str(transformed_data["tpep_pickup_datetime"].max())

        processed_path = load((transformed_data, raw_path))
        ctx.processed_path = str(processed_path)
        ctx.status = "success"
    except Exception as e:
        ctx.error = str(e)
        raise
    finally:
        ctx.ended_at = datetime.now(timezone.utc)
        ctx.write_to_logs()


if __name__ == "__main__":
    pipeline()
    logger.info("Pipeline completed")
