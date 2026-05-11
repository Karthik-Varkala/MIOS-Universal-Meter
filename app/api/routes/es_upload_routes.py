import csv
import os
import xml.etree.ElementTree as ET
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


router = APIRouter()


@router.post("/api/elasticsearch/instantaneous")
def es_push_instantaneous(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_instantaneous(tree.getroot(), meter_no)
        return publish_to_es_helper(data, index_name="meter-instantaneous-data")
    except ET.ParseError as exc:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(exc)}")


@router.post("/api/elasticsearch/dir/instantaneous")
def es_push_dir_instantaneous(req: DirectoryRequest):
    return publish_directory_data_to_es(
        directory_path=req.directory_path,
        extractor=parser.extract_instantaneous,
        index_name="meter-instantaneous-data",
    )


@router.post("/api/elasticsearch/load-profile")
def es_push_load_profile(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        flat_data = parser.extract_load_profile(tree.getroot(), meter_no)
        transformed_data = build_load_profile_es_documents(flat_data)
        return publish_to_es_helper(transformed_data, index_name=LOAD_PROFILE_INDEX)
    except ET.ParseError as exc:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(exc)}")


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
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        flat_data = parser.extract_billing(tree.getroot(), meter_no)
        transformed_data = build_billing_es_documents(flat_data)
        return publish_to_es_helper(transformed_data, index_name=BILLING_INDEX)
    except ET.ParseError as exc:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(exc)}")


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
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        event_data = parser.extract_events(tree.getroot(), meter_no)
        transformed_data = build_event_es_documents(event_data)
        return publish_to_es_helper(transformed_data, index_name=EVENT_INDEX)
    except ET.ParseError as exc:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(exc)}")


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
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        day_profile_data = parser.extract_day_profile(tree.getroot(), meter_no)
        transformed_data = build_day_profile_es_documents(day_profile_data)
        return publish_to_es_helper(transformed_data, index_name=DAY_PROFILE_INDEX)
    except ET.ParseError as exc:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(exc)}")


@router.post("/api/elasticsearch/dir/day-profile")
def es_push_dir_day_profile(req: DirectoryRequest):
    return publish_directory_data_to_es(
        directory_path=req.directory_path,
        extractor=parser.extract_day_profile,
        index_name=DAY_PROFILE_INDEX,
        transformer=build_day_profile_es_documents,
    )
