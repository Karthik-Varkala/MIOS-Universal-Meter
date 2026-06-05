import csv
from io import StringIO

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

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
from ...validation import is_expected_file_processing_issue, normalize_path_text, parse_cdf_xml, require_meter_no
from ..request_parsing import extract_all_data_paths, extract_single_path


router = APIRouter()


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

    return publish_all_data_to_es(
        file_path=payload.get("file_path"),
        directory_path=payload.get("directory_path"),
    )
