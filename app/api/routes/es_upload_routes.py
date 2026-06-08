import csv
import os
import re
import shutil
import tempfile
from io import StringIO
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from ... import parser
from ...config import (
    BILLING_INDEX,
    DAY_PROFILE_INDEX,
    EVENT_INDEX,
    LOAD_PROFILE_INDEX,
)
from ...models import LoadProfileExportRequest
from ...services.elasticsearch_service import (
    build_billing_es_documents,
    build_day_profile_es_documents,
    build_event_es_documents,
    build_load_profile_es_documents,
    build_load_profile_export_rows,
    fetch_load_profile_docs_from_es,
    publish_all_data_to_es,
    publish_directory_data_to_es,
    publish_to_es_helper,
)
from ...services.sql_service import save_upload_history_rows
from ...validation import is_expected_file_processing_issue, normalize_path_text, parse_cdf_xml, require_meter_no
from ..request_parsing import extract_all_data_paths, extract_single_path


router = APIRouter()


def _safe_upload_name(filename: str | None) -> str:
    raw_name = str(filename or "uploaded.cdf").replace("\\", "/").split("/")[-1]
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", raw_name).strip()
    return name or "uploaded.cdf"


def _upload_display_name(filename: str | None) -> str:
    raw_name = str(filename or "uploaded.cdf").replace("\\", "/").strip()
    raw_name = re.sub(r"[\x00-\x1f]", "_", raw_name)
    return raw_name.strip("/") or "uploaded.cdf"


def _build_upload_history_row(filename: str | None, file_size_bytes: int, upload_source: str) -> dict:
    return {
        "file_name": _upload_display_name(filename),
        "file_size_bytes": file_size_bytes,
        "upload_source": upload_source,
    }


def _unique_upload_path(upload_dir: str, filename: str | None) -> str:
    safe_name = _safe_upload_name(filename)
    candidate = Path(upload_dir) / safe_name
    if not candidate.exists():
        return str(candidate)

    stem = candidate.stem or "uploaded"
    suffix = candidate.suffix
    counter = 2
    while True:
        candidate = Path(upload_dir) / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return str(candidate)
        counter += 1


def _publish_uploaded_directory(temp_dir: str, uploaded_names: list[str]) -> dict:
    result = publish_all_data_to_es(directory_path=temp_dir)
    result["uploaded_files"] = uploaded_names
    result["uploaded_files_count"] = len(uploaded_names)
    return result


def _publish_single_file(file_path: str, extractor, index_name: str, transformer=None):
    operation_name = f"publish_single_file_{index_name}"
    try:
        tree = parse_cdf_xml(file_path)
        root = tree.getroot()
        meter_no = require_meter_no(root, file_path=file_path, operation=operation_name)
        data = extractor(root, meter_no, file_path=file_path)
        if transformer:
            data = transformer(data)
        return publish_to_es_helper(data, index_name=index_name)
    except HTTPException as exc:
        if is_expected_file_processing_issue(exc):
            return {
                "status": "skipped",
                "file_path": file_path,
                "index_name": index_name,
                "reason": str(getattr(exc, "detail", exc)),
                "published_records": 0,
            }
        return {
            "status": "failed",
            "file_path": file_path,
            "index_name": index_name,
            "reason": str(getattr(exc, "detail", exc)),
            "published_records": 0,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "file_path": file_path,
            "index_name": index_name,
            "reason": str(exc),
            "published_records": 0,
        }


@router.post("/api/elasticsearch/instantaneous")
async def es_push_instantaneous(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return _publish_single_file(
        file_path=file_path,
        extractor=parser.extract_instantaneous,
        index_name="meter-instantaneous-data",
    )


@router.post("/api/elasticsearch/dir/instantaneous")
async def es_push_dir_instantaneous(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return publish_directory_data_to_es(
        directory_path=directory_path,
        extractor=parser.extract_instantaneous,
        index_name="meter-instantaneous-data",
    )


@router.post("/api/elasticsearch/load-profile")
async def es_push_load_profile(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return _publish_single_file(
        file_path=file_path,
        extractor=parser.extract_load_profile,
        index_name=LOAD_PROFILE_INDEX,
        transformer=build_load_profile_es_documents,
    )


@router.post("/api/elasticsearch/dir/load-profile")
async def es_push_dir_load_profile(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return publish_directory_data_to_es(
        directory_path=directory_path,
        extractor=parser.extract_load_profile,
        index_name=LOAD_PROFILE_INDEX,
        transformer=build_load_profile_es_documents,
    )


@router.post("/api/elasticsearch/load-profile/export")
def export_load_profile_from_es(req: LoadProfileExportRequest):
    hits = fetch_load_profile_docs_from_es(req.meter_no, req.date)
    rows, csv_headers = build_load_profile_export_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No load profile data found for the given meter no and date.",
        )

    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_headers)
    writer.writeheader()
    writer.writerows(rows)

    safe_date = req.date.replace("/", "-").replace(":", "-")
    filename = f"{req.meter_no}_{safe_date}_load_profile_export.csv"

    return StreamingResponse(
        iter([csv_buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/api/elasticsearch/billing")
async def es_push_billing(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return _publish_single_file(
        file_path=file_path,
        extractor=parser.extract_billing,
        index_name=BILLING_INDEX,
        transformer=build_billing_es_documents,
    )


@router.post("/api/elasticsearch/dir/billing")
async def es_push_dir_billing(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return publish_directory_data_to_es(
        directory_path=directory_path,
        extractor=parser.extract_billing,
        index_name=BILLING_INDEX,
        transformer=build_billing_es_documents,
    )


@router.post("/api/elasticsearch/event")
async def es_push_event(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return _publish_single_file(
        file_path=file_path,
        extractor=parser.extract_events,
        index_name=EVENT_INDEX,
        transformer=build_event_es_documents,
    )


@router.post("/api/elasticsearch/dir/event")
async def es_push_dir_event(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return publish_directory_data_to_es(
        directory_path=directory_path,
        extractor=parser.extract_events,
        index_name=EVENT_INDEX,
        transformer=build_event_es_documents,
    )


@router.post("/api/elasticsearch/day-profile")
async def es_push_day_profile(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return _publish_single_file(
        file_path=file_path,
        extractor=parser.extract_day_profile,
        index_name=DAY_PROFILE_INDEX,
        transformer=build_day_profile_es_documents,
    )


@router.post("/api/elasticsearch/dir/day-profile")
async def es_push_dir_day_profile(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return publish_directory_data_to_es(
        directory_path=directory_path,
        extractor=parser.extract_day_profile,
        index_name=DAY_PROFILE_INDEX,
        transformer=build_day_profile_es_documents,
    )


@router.post("/api/elasticsearch/all-data")
async def es_push_all_data(
    request: Request,
    file_path: str | None = Query(
        default=None,
        description="Paste a file path exactly as copied from Windows.",
    ),
    folder_path: str | None = Query(
        default=None,
        description="Paste a folder path exactly as copied from Windows.",
    ),
    directory_path: str | None = Query(default=None, include_in_schema=False),
):
    supplied_paths = [
        path
        for path in (file_path, folder_path, directory_path)
        if str(path or "").strip()
    ]
    if len(supplied_paths) > 1:
        raise HTTPException(status_code=400, detail="Provide exactly one of file_path or folder_path.")

    if file_path:
        payload = {"file_path": normalize_path_text(file_path, "file_path")}
    elif folder_path or directory_path:
        payload = {"directory_path": normalize_path_text(folder_path or directory_path, "folder_path")}
    else:
        payload = await extract_all_data_paths(request)

    return await run_in_threadpool(
        publish_all_data_to_es,
        file_path=payload.get("file_path"),
        directory_path=payload.get("directory_path"),
    )


@router.post("/api/elasticsearch/all-data/upload")
async def es_push_all_data_upload(file: UploadFile = File(...)):
    suffix = Path(_safe_upload_name(file.filename)).suffix or ".cdf"
    temp_file_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            temp_file_path = temp_file.name
            shutil.copyfileobj(file.file, temp_file)

        result = await run_in_threadpool(publish_all_data_to_es, file_path=temp_file_path)
        result["uploaded_filename"] = _safe_upload_name(file.filename)
        result["upload_history_rows"] = await run_in_threadpool(
            save_upload_history_rows,
            [_build_upload_history_row(file.filename, os.path.getsize(temp_file_path), "file")]
        )
        return result
    finally:
        await file.close()
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)


@router.post("/api/elasticsearch/all-data/upload-folder")
async def es_push_all_data_upload_folder(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="Upload at least one file.")

    temp_dir = tempfile.mkdtemp(prefix="cdf_upload_")
    uploaded_names = []

    try:
        upload_history_rows = []
        for upload in files:
            destination = _unique_upload_path(temp_dir, upload.filename)
            with open(destination, "wb") as output_file:
                shutil.copyfileobj(upload.file, output_file)
            uploaded_names.append(_safe_upload_name(upload.filename))
            upload_history_rows.append(
                _build_upload_history_row(upload.filename, os.path.getsize(destination), "folder")
            )

        result = await run_in_threadpool(_publish_uploaded_directory, temp_dir, uploaded_names)
        result["upload_history_rows"] = await run_in_threadpool(save_upload_history_rows, upload_history_rows)
        return result
    finally:
        for upload in files:
            await upload.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


@router.get("/upload", response_class=HTMLResponse, include_in_schema=False)
async def upload_page():
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CDF Upload</title>
  <style>
    body { margin: 0; font-family: Arial, sans-serif; background: #f6f8fb; color: #172033; }
    main { max-width: 920px; margin: 40px auto; padding: 0 20px; }
    section { background: #fff; border: 1px solid #d9e0ea; border-radius: 8px; padding: 24px; margin-bottom: 18px; }
    h1 { font-size: 28px; margin: 0 0 18px; }
    h2 { font-size: 18px; margin: 0 0 14px; }
    input { display: block; margin: 12px 0; }
    button { background: #1464f4; border: 0; border-radius: 6px; color: #fff; cursor: pointer; font-size: 15px; padding: 10px 14px; }
    button:disabled { background: #94a3b8; cursor: wait; }
    pre { background: #111827; border-radius: 8px; color: #d1fae5; min-height: 180px; overflow: auto; padding: 16px; white-space: pre-wrap; }
  </style>
</head>
<body>
  <main>
    <h1>CDF Upload</h1>

    <section>
      <h2>Single file</h2>
      <input id="singleFile" type="file" />
      <button id="singleButton" onclick="uploadSingle()">Upload file</button>
    </section>

    <section>
      <h2>Folder</h2>
      <input id="folderFiles" type="file" webkitdirectory directory multiple />
      <button id="folderButton" onclick="uploadFolder()">Upload folder</button>
    </section>

    <pre id="result">Ready.</pre>
  </main>

  <script>
    const result = document.getElementById("result");

    async function postForm(url, formData, buttonId) {
      const button = document.getElementById(buttonId);
      button.disabled = true;
      result.textContent = "Uploading and processing...";
      try {
        const response = await fetch(url, { method: "POST", body: formData });
        const data = await response.json();
        result.textContent = JSON.stringify(data, null, 2);
      } catch (error) {
        result.textContent = String(error);
      } finally {
        button.disabled = false;
      }
    }

    function uploadSingle() {
      const input = document.getElementById("singleFile");
      if (!input.files.length) {
        result.textContent = "Choose one file first.";
        return;
      }
      const formData = new FormData();
      formData.append("file", input.files[0], input.files[0].name);
      postForm("/api/elasticsearch/all-data/upload", formData, "singleButton");
    }

    function uploadFolder() {
      const input = document.getElementById("folderFiles");
      if (!input.files.length) {
        result.textContent = "Choose one folder first.";
        return;
      }
      const formData = new FormData();
      for (const file of input.files) {
        formData.append("files", file, file.webkitRelativePath || file.name);
      }
      postForm("/api/elasticsearch/all-data/upload-folder", formData, "folderButton");
    }
  </script>
</body>
</html>
"""
