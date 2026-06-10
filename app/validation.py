import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable

from fastapi import HTTPException

from .config import DB_HOST, DB_PASSWORD, DB_USER, ES_API_KEY, ES_CLOUD_ID
from .logging_utils import append_invalid_record, get_logger, log_exception, log_with_context


logger = get_logger(__name__)
SUPPORTED_DATE_FORMATS = ("%d-%m-%Y", "%Y-%m-%d")


def validate_non_empty_text(value: str, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be blank.")
    return normalized


def normalize_path_text(value: str, field_name: str) -> str:
    normalized = validate_non_empty_text(value, field_name)
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
        normalized = normalized[1:-1].strip()
    return normalized


def validate_date_text(value: str, field_name: str = "date") -> str:
    from datetime import datetime

    normalized = validate_non_empty_text(value, field_name)
    for date_format in SUPPORTED_DATE_FORMATS:
        try:
            datetime.strptime(normalized, date_format)
            return normalized
        except ValueError:
            continue
    raise ValueError(f"{field_name} must be in dd-mm-yyyy or yyyy-mm-dd format.")


def validate_directory_path_value(value: str) -> str:
    normalized = normalize_path_text(value, "directory_path")
    if not os.path.isdir(normalized):
        raise ValueError(f"directory_path does not exist: {normalized}")
    return normalized


def validate_file_path_value(value: str) -> str:
    normalized = normalize_path_text(value, "file_path")
    if not os.path.isfile(normalized):
        raise ValueError(f"file_path does not exist: {normalized}")
    return normalized


def validate_download_dir_value(value: str) -> str:
    normalized = normalize_path_text(value, "download_dir")
    path = Path(normalized)
    if not path.is_absolute():
        path = Path.cwd() / path
    return str(path)


def parse_cdf_xml(file_path: str):
    try:
        return ET.parse(file_path)
    except (ET.ParseError, OSError, UnicodeDecodeError) as exc:
        append_invalid_record(
            "File Read Error",
            file_path=file_path,
            operation="parse_cdf_xml",
            details=str(exc),
        )
        raise HTTPException(status_code=400, detail=f"Unable to read file: {str(exc)}") from exc


def is_expected_file_processing_issue(exc: Exception) -> bool:
    if not isinstance(exc, HTTPException) or exc.status_code != 400:
        return False

    detail = str(exc.detail or "")
    expected_prefixes = (
        "Unable to read file:",
        "File Read Error:",
        "Missing required XML section:",
        "Missing required attribute",
        "Missing meter number in D1/G1",
        "Invalid billing DATETIME format.",
        "Invalid event TIME format.",
        "Invalid day profile DATETIME format.",
        "Missing or invalid D4.INTERVALPERIOD value.",
    )

    if detail.startswith(expected_prefixes):
        return True

    if detail.startswith("No ") and "records found" in detail:
        return True

    return False


def require_xml_section(root, section_tag: str, operation: str, file_path: str = ""):
    section = root.find(f".//{section_tag}")
    if section is None:
        message = f"Missing required XML section: {section_tag}"
        append_invalid_record(
            message,
            file_path=file_path,
            operation=operation,
            section=section_tag,
        )
        raise HTTPException(status_code=400, detail=message)
    return section


def require_xml_attribute(element, attribute_name: str, operation: str, file_path: str = "", element_name: str = ""):
    value = element.get(attribute_name) if element is not None else None
    if value in (None, ""):
        target = element_name or getattr(element, "tag", "element")
        message = f"Missing required attribute '{attribute_name}' in {target}"
        append_invalid_record(
            message,
            file_path=file_path,
            operation=operation,
            element=target,
            attribute=attribute_name,
        )
        raise HTTPException(status_code=400, detail=message)
    return value


def require_meter_no(root, file_path: str = "", operation: str = "extract_meter_no") -> str:
    d1 = require_xml_section(root, "D1", operation=operation, file_path=file_path)
    g1 = d1.find("G1")
    if g1 is None or not str(g1.text or "").strip():
        message = "Missing meter number in D1/G1"
        append_invalid_record(
            message,
            file_path=file_path,
            operation=operation,
            section="D1",
            element="G1",
        )
        raise HTTPException(status_code=400, detail=message)
    return str(g1.text).strip()


def ensure_records_present(records: list, operation: str, record_type: str, file_path: str = "") -> None:
    if records:
        return
    message = f"No {record_type} records found in the CDF file."
    append_invalid_record(
        message,
        file_path=file_path,
        operation=operation,
        record_type=record_type,
    )
    raise HTTPException(status_code=400, detail=message)


def build_failure_details(file_path: str, operation: str, exc: Exception) -> dict:
    detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
    return {
        "file_path": file_path,
        "operation": operation,
        "error_type": type(exc).__name__,
        "detail": detail,
    }


def log_processing_failure(file_path: str, operation: str, exc: Exception) -> dict:
    failure = build_failure_details(file_path, operation, exc)
    if isinstance(exc, HTTPException):
        log_with_context(logger, 40, "Processing failure", **failure)
    else:
        append_invalid_record(reason=failure["detail"], **failure)
        log_exception(logger, "Processing failure", exc, **failure)
    return failure


def validate_runtime_configuration() -> dict[str, list[str]]:
    config_groups = {
        "elasticsearch": [
            name
            for name, value in {"ES_CLOUD_ID": ES_CLOUD_ID, "ES_API_KEY": ES_API_KEY}.items()
            if not str(value or "").strip()
        ],
        "database": [
            name
            for name, value in {"DB_HOST": DB_HOST, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}.items()
            if not str(value or "").strip()
        ],
    }

    for group_name, missing_values in config_groups.items():
        if missing_values:
            log_with_context(
                logger,
                30,
                "Missing runtime configuration for feature group",
                feature_group=group_name,
                missing_values=missing_values,
            )
        else:
            log_with_context(
                logger,
                20,
                "Runtime configuration validated",
                feature_group=group_name,
            )

    return config_groups


def summarize_failures(failures: Iterable[dict]) -> list[dict]:
    return list(failures)
