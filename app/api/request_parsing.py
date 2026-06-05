import json
import os
import re
from typing import Any

from fastapi import HTTPException, Request

from ..validation import normalize_path_text


async def read_request_text(request: Request) -> str:
    body = await request.body()
    return body.decode("utf-8", errors="ignore").strip()


def _extract_json_object(body_text: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(body_text)
    except json.JSONDecodeError:
        return None

    return parsed if isinstance(parsed, dict) else None


def _extract_field_from_raw_body(body_text: str, field_name: str) -> str | None:
    patterns = (
        rf'"{re.escape(field_name)}"\s*:\s*"((?:\\.|[^"])*)"',
        rf"'{re.escape(field_name)}'\s*:\s*'([^']*)'",
    )
    for pattern in patterns:
        match = re.search(pattern, body_text, flags=re.DOTALL)
        if match:
            return match.group(1).strip()
    return None


def _build_plain_text_path(body_text: str, field_name: str) -> str | None:
    cleaned = body_text.strip().strip(",")
    if not cleaned:
        return None

    if cleaned.startswith("{") and cleaned.endswith("}"):
        return None

    return normalize_path_text(cleaned, field_name)


async def extract_single_path(request: Request, field_name: str) -> str:
    body_text = await read_request_text(request)
    if not body_text:
        raise HTTPException(status_code=400, detail=f"{field_name} is required.")

    parsed_json = _extract_json_object(body_text)
    if parsed_json is not None:
        value = parsed_json.get(field_name)
        if value not in (None, ""):
            return normalize_path_text(str(value), field_name)

    raw_value = _extract_field_from_raw_body(body_text, field_name)
    if raw_value not in (None, ""):
        return normalize_path_text(raw_value, field_name)

    plain_text_value = _build_plain_text_path(body_text, field_name)
    if plain_text_value:
        return plain_text_value

    raise HTTPException(status_code=400, detail=f"{field_name} is required.")


async def extract_all_data_paths(request: Request) -> dict[str, str]:
    body_text = await read_request_text(request)
    if not body_text:
        raise HTTPException(status_code=400, detail="Provide either file_path or directory_path.")

    parsed_json = _extract_json_object(body_text)
    if parsed_json is not None:
        file_path = parsed_json.get("file_path")
        directory_path = parsed_json.get("directory_path")
        if file_path not in (None, "") and directory_path not in (None, ""):
            raise HTTPException(status_code=400, detail="Provide exactly one of file_path or directory_path.")
        if file_path not in (None, ""):
            return {"file_path": normalize_path_text(str(file_path), "file_path")}
        if directory_path not in (None, ""):
            return {"directory_path": normalize_path_text(str(directory_path), "directory_path")}

    file_path = _extract_field_from_raw_body(body_text, "file_path")
    directory_path = _extract_field_from_raw_body(body_text, "directory_path")
    if file_path not in (None, "") and directory_path not in (None, ""):
        raise HTTPException(status_code=400, detail="Provide exactly one of file_path or directory_path.")
    if file_path not in (None, ""):
        return {"file_path": normalize_path_text(file_path, "file_path")}
    if directory_path not in (None, ""):
        return {"directory_path": normalize_path_text(directory_path, "directory_path")}

    plain_text_value = _build_plain_text_path(body_text, "file_path")
    if plain_text_value:
        if os.path.isdir(plain_text_value):
            return {"directory_path": plain_text_value}
        return {"file_path": plain_text_value}

    raise HTTPException(status_code=400, detail="Provide either file_path or directory_path.")


async def extract_s3_request(request: Request) -> dict[str, str]:
    body_text = await read_request_text(request)
    if not body_text:
        raise HTTPException(status_code=400, detail="bucket_name and object_key are required.")

    parsed_json = _extract_json_object(body_text)
    if parsed_json is not None:
        bucket_name = parsed_json.get("bucket_name")
        object_key = parsed_json.get("object_key")
        download_dir = parsed_json.get("download_dir", "S3files")
        if bucket_name in (None, "") or object_key in (None, ""):
            raise HTTPException(status_code=400, detail="bucket_name and object_key are required.")
        return {
            "bucket_name": str(bucket_name).strip(),
            "object_key": str(object_key).strip(),
            "download_dir": normalize_path_text(str(download_dir), "download_dir"),
        }

    bucket_name = _extract_field_from_raw_body(body_text, "bucket_name")
    object_key = _extract_field_from_raw_body(body_text, "object_key")
    download_dir = _extract_field_from_raw_body(body_text, "download_dir") or "S3files"
    if bucket_name in (None, "") or object_key in (None, ""):
        raise HTTPException(status_code=400, detail="bucket_name and object_key are required.")
    return {
        "bucket_name": bucket_name.strip(),
        "object_key": object_key.strip(),
        "download_dir": normalize_path_text(download_dir, "download_dir"),
    }
