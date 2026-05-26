import csv
from io import StringIO

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ... import parser
from ...config import (
    BILLING_INDEX,
    DAY_PROFILE_INDEX,
    EVENT_INDEX,
    LOAD_PROFILE_INDEX,
)
from ...models import DirectoryRequest, FileRequest, LoadProfileExportRequest
from ...services.elasticsearch_service import (
    build_billing_es_documents,
    build_day_profile_es_documents,
    build_event_es_documents,
    build_load_profile_es_documents,
    build_load_profile_export_rows,
    fetch_load_profile_docs_from_es,
    publish_directory_data_to_es,
    publish_to_es_helper,
)
from ...validation import parse_cdf_xml, require_meter_no


router = APIRouter()


def _publish_single_file(file_path: str, extractor, index_name: str, transformer=None):
    tree = parse_cdf_xml(file_path)
    root = tree.getroot()
    meter_no = require_meter_no(root, file_path=file_path, operation=f"publish_single_file_{index_name}")
    data = extractor(root, meter_no, file_path=file_path)
    if transformer:
        data = transformer(data)
    return publish_to_es_helper(data, index_name=index_name)


@router.post("/api/elasticsearch/instantaneous")
def es_push_instantaneous(req: FileRequest):
    return _publish_single_file(
        file_path=req.file_path,
        extractor=parser.extract_instantaneous,
        index_name="meter-instantaneous-data",
    )


@router.post("/api/elasticsearch/dir/instantaneous")
def es_push_dir_instantaneous(req: DirectoryRequest):
    return publish_directory_data_to_es(
        directory_path=req.directory_path,
        extractor=parser.extract_instantaneous,
        index_name="meter-instantaneous-data",
    )


@router.post("/api/elasticsearch/load-profile")
def es_push_load_profile(req: FileRequest):
    return _publish_single_file(
        file_path=req.file_path,
        extractor=parser.extract_load_profile,
        index_name=LOAD_PROFILE_INDEX,
        transformer=build_load_profile_es_documents,
    )


@router.post("/api/elasticsearch/dir/load-profile")
def es_push_dir_load_profile(req: DirectoryRequest):
    return publish_directory_data_to_es(
        directory_path=req.directory_path,
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
def es_push_billing(req: FileRequest):
    return _publish_single_file(
        file_path=req.file_path,
        extractor=parser.extract_billing,
        index_name=BILLING_INDEX,
        transformer=build_billing_es_documents,
    )


@router.post("/api/elasticsearch/dir/billing")
def es_push_dir_billing(req: DirectoryRequest):
    return publish_directory_data_to_es(
        directory_path=req.directory_path,
        extractor=parser.extract_billing,
        index_name=BILLING_INDEX,
        transformer=build_billing_es_documents,
    )


@router.post("/api/elasticsearch/event")
def es_push_event(req: FileRequest):
    return _publish_single_file(
        file_path=req.file_path,
        extractor=parser.extract_events,
        index_name=EVENT_INDEX,
        transformer=build_event_es_documents,
    )


@router.post("/api/elasticsearch/dir/event")
def es_push_dir_event(req: DirectoryRequest):
    return publish_directory_data_to_es(
        directory_path=req.directory_path,
        extractor=parser.extract_events,
        index_name=EVENT_INDEX,
        transformer=build_event_es_documents,
    )


@router.post("/api/elasticsearch/day-profile")
def es_push_day_profile(req: FileRequest):
    return _publish_single_file(
        file_path=req.file_path,
        extractor=parser.extract_day_profile,
        index_name=DAY_PROFILE_INDEX,
        transformer=build_day_profile_es_documents,
    )


@router.post("/api/elasticsearch/dir/day-profile")
def es_push_dir_day_profile(req: DirectoryRequest):
    return publish_directory_data_to_es(
        directory_path=req.directory_path,
        extractor=parser.extract_day_profile,
        index_name=DAY_PROFILE_INDEX,
        transformer=build_day_profile_es_documents,
    )
