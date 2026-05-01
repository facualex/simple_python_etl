import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Literal

LogLevelName = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]


def configure_logging(minimum_level: int | LogLevelName | None = None) -> None:
    """Configure application logging once at startup.

    Keep this as a function (instead of module-level configuration) so importing
    modules doesn't have side effects. Call it from the entrypoint a single time.
    """

    if minimum_level is None:
        minimum_level = os.getenv("LOG_LEVEL", "INFO").upper()

    resolved_level = (
        minimum_level
        if isinstance(minimum_level, int)
        else logging.getLevelName(minimum_level)
    )

    if not isinstance(resolved_level, int):
        raise ValueError(
            f"Invalid log level: {minimum_level!r}. Use one of "
            "'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'."
        )

    root_logger = logging.getLogger()
    root_logger.setLevel(resolved_level)

    # Clear existing handlers to avoid duplicated logs if configure_logging()
    # is called more than once in the same process.
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s:%(lineno)d] - %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(resolved_level)
    console_handler.setFormatter(formatter)

    project_root = Path(__file__).resolve().parent.parent
    logs_root_dir = project_root / "logs"
    day_dir = logs_root_dir / datetime.now().strftime("%Y%m%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    log_filename = datetime.now().strftime("%H%M%S-%f") + ".log"

    file_handler = logging.FileHandler(day_dir / log_filename, encoding="utf-8")
    file_handler.setLevel(resolved_level)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
