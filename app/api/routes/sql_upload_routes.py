from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from ...config import (
    DB_BILLING_TABLE,
    DB_DAY_PROFILE_TABLE,
    DB_EVENT_PARAMETER_TABLE,
    DB_EVENT_TABLE,
    DB_LOAD_PROFILE_TABLE,
    DB_NAME,
    DB_UPLOAD_HISTORY_TABLE,
)
from ...models import EsToSqlAllDataRequest, LoadProfileExportRequest, MeterDateRequest, MeterRequest
from ...services.elasticsearch_service import (
    fetch_all_billing_docs_from_es,
    fetch_all_sql_export_docs_from_es,
    fetch_billing_docs_from_es,
    fetch_billing_month_docs_from_es,
    fetch_day_profile_month_docs_from_es,
    fetch_event_docs_from_es,
    fetch_event_month_docs_from_es,
    fetch_load_profile_docs_from_es,
    fetch_load_profile_month_docs_from_es,
    parse_request_date,
)
from ...services.sql_service import (
    build_billing_sql_rows,
    build_day_profile_sql_rows,
    build_event_sql_rows,
    build_load_profile_sql_rows,
    delete_meter_data_from_sql,
    get_distinct_meter_numbers_from_sql,
    get_upload_history_from_sql,
    save_billing_rows_to_sql,
    save_day_profile_rows_to_sql,
    save_event_rows_to_sql,
    save_load_profile_rows_to_sql,
)
from ...validation import validate_non_empty_text


router = APIRouter()


SQL_EXPORT_DATASET_HANDLERS = {
    "load_profile": {
        "table": DB_LOAD_PROFILE_TABLE,
        "builder": build_load_profile_sql_rows,
        "saver": save_load_profile_rows_to_sql,
    },
    "billing": {
        "table": DB_BILLING_TABLE,
        "builder": build_billing_sql_rows,
        "saver": save_billing_rows_to_sql,
    },
    "event": {
        "table": DB_EVENT_TABLE,
        "parameter_table": DB_EVENT_PARAMETER_TABLE,
        "builder": build_event_sql_rows,
        "saver": save_event_rows_to_sql,
    },
    "day_profile": {
        "table": DB_DAY_PROFILE_TABLE,
        "builder": build_day_profile_sql_rows,
        "saver": save_day_profile_rows_to_sql,
    },
}


@router.get("/api/sql/meters")
def get_sql_meter_numbers():
    result = get_distinct_meter_numbers_from_sql()
    return {
        "status": "success",
        "database": DB_NAME,
        **result,
    }


@router.delete("/api/sql/meters/{meter_no}")
def delete_sql_meter_data(meter_no: str):
    normalized_meter_no = validate_non_empty_text(meter_no, "meter_no")
    result = delete_meter_data_from_sql(normalized_meter_no)
    return {
        "status": "success",
        "database": DB_NAME,
        **result,
    }


@router.get("/api/sql/upload-history")
def get_sql_upload_history(limit: int = Query(default=100, ge=1, le=1000)):
    result = get_upload_history_from_sql(limit=limit)
    return {
        "status": "success",
        "database": DB_NAME,
        "table": DB_UPLOAD_HISTORY_TABLE,
        **result,
    }


def _resolve_meter_scope(req: EsToSqlAllDataRequest):
    if req.all_meters:
        return "all_meters", None
    if req.meter_no:
        return "single_meter", [req.meter_no]
    return "multiple_meters", req.meter_nos


def _save_dataset_from_es_hits(dataset_name: str, hits: list):
    handler = SQL_EXPORT_DATASET_HANDLERS[dataset_name]
    rows = handler["builder"](hits)

    if not rows:
        return {
            "status": "no_data",
            "index_records": len(hits),
            "processed_rows": 0,
            "affected_rows": 0,
        }

    save_result = handler["saver"](rows)
    result = {
        "status": "success",
        "index_records": len(hits),
        "processed_rows": len(rows),
        "table": handler["table"],
    }

    if dataset_name == "event":
        result.update(
            {
                "event_table": DB_EVENT_TABLE,
                "event_parameter_table": DB_EVENT_PARAMETER_TABLE,
                "processed_event_rows": save_result["event_rows"],
                "processed_parameter_rows": save_result["parameter_rows"],
                "affected_rows": save_result["affected_rows"],
            }
        )
        return result

    result["affected_rows"] = save_result
    return result


def _extract_billing_month_label(source: dict) -> str:
    timestamp = str(source.get("timestamp", "") or "").strip()
    if timestamp:
        try:
            return parse_request_date(timestamp[:10]).strftime("%m-%Y")
        except HTTPException:
            pass
        except Exception:
            pass

    date_time = str(source.get("date_time", "") or "").strip()
    for datetime_format in (
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S:%f",
        "%d/%m/%Y %H:%M:%S:%f",
    ):
        try:
            return datetime.strptime(date_time, datetime_format).strftime("%m-%Y")
        except ValueError:
            continue
    return ""


@router.post("/api/elasticsearch/all-data/save-to-sql")
def save_all_data_from_es_to_sql(req: EsToSqlAllDataRequest):
    meter_scope, meter_nos = _resolve_meter_scope(req)
    date_filter = (
        {"mode": "date_range", "start_date": req.start_date, "end_date": req.end_date}
        if req.start_date and req.end_date
        else {"mode": "total_data"}
    )
    hits_by_dataset = fetch_all_sql_export_docs_from_es(
        meter_nos=meter_nos,
        start_date=req.start_date,
        end_date=req.end_date,
    )

    dataset_results = {}
    failed_datasets = []
    successful_datasets = []

    for dataset_name, hits in hits_by_dataset.items():
        try:
            result = _save_dataset_from_es_hits(dataset_name, hits)
            dataset_results[dataset_name] = result
            if result["status"] == "success":
                successful_datasets.append(dataset_name)
        except HTTPException as exc:
            failed_datasets.append(dataset_name)
            dataset_results[dataset_name] = {
                "status": "failed",
                "index_records": len(hits),
                "processed_rows": 0,
                "affected_rows": 0,
                "error": exc.detail,
            }
        except Exception as exc:
            failed_datasets.append(dataset_name)
            dataset_results[dataset_name] = {
                "status": "failed",
                "index_records": len(hits),
                "processed_rows": 0,
                "affected_rows": 0,
                "error": str(exc),
            }

    total_processed_rows = sum(result.get("processed_rows", 0) for result in dataset_results.values())
    total_affected_rows = sum(result.get("affected_rows", 0) for result in dataset_results.values())
    if failed_datasets and successful_datasets:
        status = "partial_success"
    elif failed_datasets:
        status = "failed"
    else:
        status = "success"

    return {
        "status": status,
        "database": DB_NAME,
        "meter_scope": meter_scope,
        "meter_nos": meter_nos,
        "date_filter": date_filter,
        "ignored_datasets": ["instantaneous"],
        "processed_rows": total_processed_rows,
        "affected_rows": total_affected_rows,
        "failed_datasets": failed_datasets,
        "datasets": dataset_results,
    }


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


@router.post("/api/elasticsearch/billing/save-to-sql")
def save_billing_from_es_to_sql(req: MeterDateRequest):
    hits = fetch_billing_docs_from_es(req.meter_no, req.date)
    rows = build_billing_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No billing data found for the given meter no and date.",
        )

    affected_rows = save_billing_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "table": DB_BILLING_TABLE,
        "meter_no": req.meter_no,
        "date": req.date,
        "processed_rows": len(rows),
        "affected_rows": affected_rows,
    }


@router.post("/api/elasticsearch/billing/save-month-to-sql")
def save_billing_month_from_es_to_sql(req: MeterDateRequest):
    target_date = parse_request_date(req.date)
    hits = fetch_billing_month_docs_from_es(req.meter_no, req.date)
    rows = build_billing_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No billing data found for the given meter no and month.",
        )

    affected_rows = save_billing_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "table": DB_BILLING_TABLE,
        "meter_no": req.meter_no,
        "month": target_date.strftime("%m-%Y"),
        "processed_rows": len(rows),
        "affected_rows": affected_rows,
    }


@router.post("/api/elasticsearch/billing/save-all-months-to-sql")
def save_billing_all_months_from_es_to_sql(req: MeterRequest):
    hits = fetch_all_billing_docs_from_es(req.meter_no)
    rows = build_billing_sql_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No billing data found for the given meter no.",
        )

    available_months = sorted(
        {
            month_label
            for hit in hits
            for month_label in [_extract_billing_month_label(hit.get("_source", {}))]
            if month_label
        }
    )
    affected_rows = save_billing_rows_to_sql(rows)
    return {
        "status": "success",
        "database": DB_NAME,
        "table": DB_BILLING_TABLE,
        "meter_no": req.meter_no,
        "available_months": available_months,
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
