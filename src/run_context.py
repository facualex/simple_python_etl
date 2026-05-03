"""Run metadata container for the ETL pipeline.

A RunContext is created at the start of each pipeline execution and populated
incrementally as each step completes. It is written to disk as a JSON summary
at the end of the run — whether the run succeeded or failed — providing a
structured audit trail alongside the log file.
"""

import json
import subprocess
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal


def _git_sha() -> str | None:
    # Module-level so it can be passed as default_factory to field() below.
    # Inside the class body RunContext doesn't exist yet, so static helpers
    # that are referenced in field() defaults must live outside the class.
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except Exception:
        return None


@dataclass
class RunContext:
    """Holds all metadata and metrics for a single pipeline execution.

    Fields are intentionally nullable so the object can be written even when
    the pipeline fails mid-way and later fields were never populated.

    Attributes:
        run_id: Unique identifier for this execution (UUID4).
        started_at: UTC timestamp recorded at pipeline start.
        ended_at: UTC timestamp recorded at pipeline end, success or failure.
        status: Final outcome; defaults to "failed" so an incomplete run is
            never silently recorded as successful.
        error: Exception message if the run failed, None otherwise.

        input_url: Remote URL the raw file was downloaded from.
        raw_path: Local path of the downloaded raw parquet file.
        processed_path: Local path of the written processed parquet file.

        rows_in: Row count of the raw dataset before any cleaning.
        rows_out: Row count of the processed dataset after cleaning and transforms.
        dropped_by_reason: Counts of rows removed at each cleaning step, keyed
            by the name of the rule (e.g. "invalid_passengers").

        pickup_min: Earliest pickup timestamp in the processed dataset (ISO string).
            Useful to quickly identify the processed time period.
        pickup_max: Latest pickup timestamp in the processed dataset (ISO string).
            Useful to quickly identify the processed time period.

        git_sha: Git commit SHA of the codebase at the time of execution. Useful
            when the pipeline runs on a schedule to correlate output with code version.
    """

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: datetime | None = None
    status: Literal["success", "failed"] = "failed"
    error: str | None = None

    input_url: str | None = None
    raw_path: str | None = None
    processed_path: str | None = None

    rows_in: int | None = None
    rows_out: int | None = None
    dropped_by_reason: dict[str, int] = field(default_factory=dict)

    pickup_min: str | None = None
    pickup_max: str | None = None

    git_sha: str | None = field(default_factory=_git_sha)

    def write_to_logs(self) -> None:
        """Serialize this RunContext to a JSON file under logs/YYYYMMDD/."""
        project_root = Path(__file__).resolve().parent.parent
        logs_dir = project_root / "logs" / self.started_at.strftime("%Y%m%d")
        logs_dir.mkdir(parents=True, exist_ok=True)

        output_path = logs_dir / f"run_summary_{self.run_id}.json"

        with open(output_path, "w") as f:
            # default=str converts datetimes (and any other non-serializable types)
            # to their string representation instead of raising TypeError.
            json.dump(asdict(self), f, default=str, indent=2)
