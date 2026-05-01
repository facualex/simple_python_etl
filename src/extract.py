import logging
import os
from datetime import datetime
from pathlib import Path

import requests
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S = (
    30  # Timeout (seconds) for the HTTP request that downloads the parquet file
)


def build_url(year: int, month: int) -> str:
    from dotenv import load_dotenv

    load_dotenv()

    BASE_URL = os.getenv("BASE_URL")

    if not BASE_URL:
        raise ValueError(
            "Missing BASE_URL. Set it in your environment or a .env file (e.g. BASE_URL=https://.../yellow_tripdata)"
        )

    return f"{BASE_URL}_{year}-{month:02d}.parquet"


def get_latest_data_url(max_backwards_threshold: int = 6) -> str:
    """Resolve the most recent available NYC Taxi dataset URL/path.

    The NYC TLC parquet files are published with a lag and can be missing for
    the current month. This helper encapsulates the logic for selecting the
    newest *existing* dataset (e.g., by probing months backwards from "now")
    so the pipeline can run deterministically without manual date updates.

    Args:
        max_backwards_threshold (int): Maximum number of months to step
            backwards when probing for an available dataset. This bounds the
            search so the pipeline fails fast if the source is stale or the
            URL pattern is wrong.

    Returns:
        str: A fully-qualified URL for the latest available data.
    """

    now = datetime.now()

    for month_back in range(max_backwards_threshold):
        date = now - relativedelta(months=month_back)

        YYYY = date.year
        MM = date.month

        try:
            url = build_url(YYYY, MM)
        except ValueError:
            logger.exception(
                "Failed to build data URL (missing/invalid configuration)."
            )
            raise

        logger.info(f"Checking for available data ({YYYY}-{MM}): {url}")

        try:
            # HEAD, no GET. Doesn't download the file.
            response = requests.head(url, timeout=DEFAULT_TIMEOUT_S)
        except requests.exceptions.RequestException:
            logger.warning(
                "Failed checking availability for %s-%02d (%s). Will retry previous months.",
                YYYY,
                MM,
                url,
                exc_info=True,
            )
            continue

        if response.status_code == 200:
            logger.info(f"Data found for {YYYY}-{MM}: {url}")
            return url

        logger.warning(f"No data found for {YYYY}-{MM}")

    raise RuntimeError(
        f"Could not find an available dataset within the last {max_backwards_threshold} months. "
        "Check BASE_URL and the upstream dataset availability."
    )


def extract(
    year: int | None = None,
    month: int | None = None,
    destination: Path = Path("data/raw/"),
) -> Path:
    """Download the NYC Taxi parquet file and persist it locally.

    If `year` and `month` are not provided, the function probes the most recent
    available dataset by checking (via HTTP HEAD) the current month and then
    stepping backwards up to a configured threshold.

    The `destination` argument is flexible:
    - If it is a directory path (default: `data/raw/`), the file is written to
      `<destination>/<remote_filename>.parquet`.
    - If it is an explicit file path (i.e. it has a suffix like `.parquet`),
      the file is written exactly to that path.

    Args:
        year: Dataset year (e.g. 2026). If omitted, the latest available month
            is resolved automatically.
        month: Dataset month (1-12). If omitted, the latest available month is
            resolved automatically.
        destination: Output directory or output file path where the downloaded
            parquet will be saved.

    Returns:
        Path to the saved parquet file.

    Raises:
        ValueError: If required configuration is missing (e.g. `BASE_URL`).
        RuntimeError: If no dataset can be found within the lookback window.
        requests.exceptions.RequestException: For network-related failures.
        OSError: For filesystem-related failures (permissions, disk full, etc.).
    """
    try:
        if year is None or month is None:
            data_url = get_latest_data_url()
        else:
            data_url = build_url(year, month)
    except ValueError:
        logger.exception("Failed to resolve data URL (missing/invalid configuration).")
        raise
    except Exception:
        logger.exception("Unexpected error while resolving the data URL.")
        raise

    logger.info(f"Downloading {data_url}...")

    try:
        response = requests.get(data_url, stream=True, timeout=DEFAULT_TIMEOUT_S)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        logger.exception("HTTP error downloading %s (status=%s).", data_url, status)
        raise
    except requests.exceptions.RequestException:
        logger.exception("Network error downloading %s.", data_url)
        raise

    # 'destination' can be either:
    # - a directory path (default): we write to '<destination>/<remote_filename>'
    # - an explicit file path (has suffix): we write exactly there
    try:
        if destination.suffix:
            output_path = destination
            output_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            destination.mkdir(parents=True, exist_ok=True)
            remote_file_name = Path(data_url).name
            output_path = destination / remote_file_name
    except OSError:
        logger.exception("Failed to prepare destination path: %s", destination)
        raise

    try:
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                f.write(chunk)
    except OSError:
        logger.exception("Failed writing downloaded file to %s", output_path)
        raise

    logger.info(
        f"Filed saved: {output_path} ({output_path.stat().st_size / 1e6:.1f} MB)"
    )

    return output_path
