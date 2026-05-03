import json
from pathlib import Path

import requests

# Resolved once at module load. All functions in this file use it.
_STATE_FILE = Path(__file__).resolve().parent.parent / "pipeline_state.json"


def load_state() -> dict:
    """Read the pipeline state file. Returns {} if the file doesn't exist yet."""
    if not _STATE_FILE.exists():
        return {}

    with open(_STATE_FILE) as f:
        return json.load(f)


def save_state(month_key: str, etag: str, processed_path: str) -> None:
    """Upsert the state entry for a given month and persist the file."""
    state = load_state()

    state[month_key] = {
        "etag": etag,
        "processed_path": processed_path,
    }

    with open(_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_remote_etag(url: str, timeout: int = 30) -> str | None:
    """Fetch the ETag header of a remote file via HTTP HEAD without downloading it.

    Returns None if the request fails or the server does not expose an ETag.
    """
    try:
        response = requests.head(url, timeout=timeout)
        response.raise_for_status()
        return response.headers.get("ETag")
    except requests.exceptions.RequestException:
        return None
