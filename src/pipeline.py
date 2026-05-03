"""ETL pipeline entrypoint.

Running this module executes the end-to-end workflow:
extract (download) → transform → load.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from extract import extract, get_latest_data_url
from load import load
from logging_config import configure_logging
from pipeline_state import get_remote_etag, load_state, save_state
from run_context import RunContext
from transform import transform

load_dotenv()
_log_file = configure_logging()

logger = logging.getLogger(__name__)


def pipeline():
    """Run the full ETL pipeline once."""
    ctx = RunContext(log_file=str(_log_file))
    current_etag = None
    processed_path = None

    try:
        # Resolve URL first so it can be used for the skip check before downloading.
        url = get_latest_data_url()
        month_key = Path(url).stem.split("_")[-1]  # e.g. "2026-03"

        # Fetch the remote ETag once — used for both the skip check and state saving.
        current_etag = get_remote_etag(url)

        # Skip if already processed and the source file hasn't changed.
        state = load_state()
        if month_key in state:
            stored_etag = state[month_key].get("etag")
            output_exists = Path(state[month_key].get("processed_path", "")).exists()

            if current_etag is not None and current_etag == stored_etag and output_exists:
                logger.info("Skipping %s: already processed and source unchanged.", month_key)
                ctx.status = "success"
                return

        raw_path, source_url = extract(url=url)
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

    # Save state only after a confirmed successful run.
    # Outside the try/finally so a state write failure doesn't mask a pipeline success.
    if current_etag is not None and processed_path is not None:
        try:
            save_state(month_key, current_etag, str(processed_path))
        except Exception:
            logger.warning("Failed to save pipeline state for %s.", month_key, exc_info=True)


if __name__ == "__main__":
    pipeline()
    logger.info("Pipeline completed")
