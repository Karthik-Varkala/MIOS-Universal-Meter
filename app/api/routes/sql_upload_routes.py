from fastapi import APIRouter, HTTPException

from ...config import (
    DB_DAY_PROFILE_TABLE,
    DB_EVENT_PARAMETER_TABLE,
    DB_EVENT_TABLE,
    DB_LOAD_PROFILE_TABLE,
    DB_NAME,
)
from ...models import LoadProfileExportRequest, MeterDateRequest
from ...services.elasticsearch_service import (
    fetch_day_profile_month_docs_from_es,
    fetch_event_docs_from_es,
    fetch_event_month_docs_from_es,
    fetch_load_profile_docs_from_es,
    fetch_load_profile_month_docs_from_es,
    parse_request_date,
)
from ...services.sql_service import (
    build_day_profile_sql_rows,
    build_event_sql_rows,
    build_load_profile_sql_rows,
    save_day_profile_rows_to_sql,
    save_event_rows_to_sql,
    save_load_profile_rows_to_sql,
)


router = APIRouter()


@router.post("/api/elasticsearch/load-profile/save-to-sql")
def save_load_profile_from_es_to_sql(req: LoadProfileExportRequest):
    hits = fetch_load_profile_docs_from_es(req.meter_no, req.date)
    rows = build_load_profile_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No load profile data found for the given meter no and date.",
        )

    affected_rows = save_load_profile_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "table": DB_LOAD_PROFILE_TABLE,
        "meter_no": req.meter_no,
        "date": req.date,
        "processed_rows": len(rows),
        "affected_rows": affected_rows,
    }


@router.post("/api/elasticsearch/load-profile/save-month-to-sql")
def save_load_profile_month_from_es_to_sql(req: LoadProfileExportRequest):
    target_date = parse_request_date(req.date)
    hits = fetch_load_profile_month_docs_from_es(req.meter_no, req.date)
    rows = build_load_profile_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No load profile data found for the given meter no and month.",
        )

    affected_rows = save_load_profile_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "table": DB_LOAD_PROFILE_TABLE,
        "meter_no": req.meter_no,
        "month": target_date.strftime("%m-%Y"),
        "processed_rows": len(rows),
        "affected_rows": affected_rows,
    }


@router.post("/api/elasticsearch/event/save-to-sql")
def save_event_from_es_to_sql(req: MeterDateRequest):
    hits = fetch_event_docs_from_es(req.meter_no, req.date)
    rows = build_event_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No event data found for the given meter no and date.",
        )

    save_result = save_event_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "event_table": DB_EVENT_TABLE,
        "event_parameter_table": DB_EVENT_PARAMETER_TABLE,
        "meter_no": req.meter_no,
        "date": req.date,
        "processed_event_rows": save_result["event_rows"],
        "processed_parameter_rows": save_result["parameter_rows"],
        "affected_rows": save_result["affected_rows"],
    }


@router.post("/api/elasticsearch/event/save-month-to-sql")
def save_event_month_from_es_to_sql(req: MeterDateRequest):
    target_date = parse_request_date(req.date)
    hits = fetch_event_month_docs_from_es(req.meter_no, req.date)
    rows = build_event_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No event data found for the given meter no and month.",
        )

    save_result = save_event_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "event_table": DB_EVENT_TABLE,
        "event_parameter_table": DB_EVENT_PARAMETER_TABLE,
        "meter_no": req.meter_no,
        "month": target_date.strftime("%m-%Y"),
        "processed_event_rows": save_result["event_rows"],
        "processed_parameter_rows": save_result["parameter_rows"],
        "affected_rows": save_result["affected_rows"],
    }


@router.post("/api/elasticsearch/day-profile/save-month-to-sql")
def save_day_profile_month_from_es_to_sql(req: MeterDateRequest):
    target_date = parse_request_date(req.date)
    hits = fetch_day_profile_month_docs_from_es(req.meter_no, req.date)
    rows = build_day_profile_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No day profile data found for the given meter no and month.",
        )

    affected_rows = save_day_profile_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "table": DB_DAY_PROFILE_TABLE,
        "meter_no": req.meter_no,
        "month": target_date.strftime("%m-%Y"),
        "processed_rows": len(rows),
        "affected_rows": affected_rows,
    }
