import json
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
APP_LOG_FILE = LOG_DIR / "error_log.txt"
INVALID_RECORDS_FILE = LOG_DIR / "invalid_records.jsonl"


def _json_default(value: Any):
    if isinstance(value, (datetime, Path)):
        return str(value)
    return repr(value)


def _serialize_context(context: dict[str, Any]) -> str:
    return json.dumps(context, default=_json_default, ensure_ascii=True, sort_keys=True)


def ensure_log_directory() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR


def configure_logging() -> None:
    ensure_log_directory()
    root_logger = logging.getLogger()

    if any(
        isinstance(handler, RotatingFileHandler) and Path(getattr(handler, "baseFilename", "")) == APP_LOG_FILE
        for handler in root_logger.handlers
    ):
        return

    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = RotatingFileHandler(
        APP_LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


def log_with_context(logger: logging.Logger, level: int, message: str, **context: Any) -> None:
    if context:
        logger.log(level, "%s | context=%s", message, _serialize_context(context))
        return
    logger.log(level, message)


def log_exception(logger: logging.Logger, message: str, exc: Exception, **context: Any) -> None:
    error_context = {
        "exception_type": type(exc).__name__,
        "exception_message": str(exc),
    }
    error_context.update(context)
    logger.exception("%s | context=%s", message, _serialize_context(error_context))


def append_invalid_record(reason: str, **context: Any) -> None:
    ensure_log_directory()
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        **context,
    }
    with INVALID_RECORDS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, default=_json_default, ensure_ascii=True) + "\n")
