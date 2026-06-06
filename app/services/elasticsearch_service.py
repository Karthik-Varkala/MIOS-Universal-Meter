import os
from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from fastapi import HTTPException

from .. import parser
from ..config import (
    BILLING_INDEX,
    DB_LOAD_PROFILE_TABLE,
    ES_API_KEY,
    ES_ENDPOINT,
    ES_CLOUD_ID,
    DAY_PROFILE_INDEX,
    EVENT_INDEX,
    INSTANTANEOUS_INDEX,
    LOAD_PROFILE_CORE_HEADERS,
    LOAD_PROFILE_INDEX,
)
from ..logging_utils import get_logger, log_exception
from ..validation import (
    is_expected_file_processing_issue,
    log_processing_failure,
    parse_cdf_xml,
    require_meter_no,
)
from .sql_service import get_parameter_mappings_for_table

es_client = Elasticsearch(
   cloud_id=ES_CLOUD_ID,
    api_key=ES_API_KEY,

)
logger = get_logger(__name__)


def _build_skipped_file_result(file_path: str, operation_name: str, exc: Exception) -> dict:
    detail = str(getattr(exc, "detail", exc))
    return {
        "file_path": file_path,
        "operation": operation_name,
        "error_type": type(exc).__name__,
        "reason": detail,
    }


def publish_to_es_helper(data: list, index_name: str):
    try:
        actions = []
        for record in data:
            action = {
                "_index": index_name,
                "_source": record,
            }

            if "_id" in record:
                action["_id"] = record.pop("_id")

            actions.append(action)

        success, _ = helpers.bulk(es_client, actions)
        return {"message": f"Successfully inserted {success} records into {index_name}"}
    except Exception as exc:
        log_exception(logger, "Elasticsearch bulk insert failed", exc, index_name=index_name, record_count=len(data))
        raise HTTPException(status_code=500, detail=f"Elasticsearch Bulk Insert Error: {str(exc)}")


def parse_request_date(date_str: str) -> datetime:
    for date_format in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            continue

    raise HTTPException(
        status_code=400,
        detail="Invalid date format. Use either dd-mm-yyyy or yyyy-mm-dd.",
    )


def fetch_load_profile_docs_from_es(meter_no: str, date: str):
    source_fields = ["meter_no", "date", "interval", "timestamp", "parameters"]
    query = {
        "bool": {
            "filter": [
                {"term": {"meter_no.keyword": meter_no}},
                {"term": {"date.keyword": date}},
            ]
        }
    }

    try:
        response = es_client.search(
            index=LOAD_PROFILE_INDEX,
            body={"size": 5000, "_source": source_fields, "query": query},
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        log_exception(logger, "Elasticsearch search failed", exc, index_name=LOAD_PROFILE_INDEX, meter_no=meter_no, date=date)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_load_profile_month_docs_from_es(meter_no: str, date: str):
    target_date = parse_request_date(date)
    month_year_pattern = f"*-{target_date.strftime('%m-%Y')}"
    source_fields = ["meter_no", "date", "interval", "timestamp", "parameters"]
    query = {
        "bool": {
            "filter": [
                {"term": {"meter_no.keyword": meter_no}},
                {"wildcard": {"date.keyword": {"value": month_year_pattern}}},
            ]
        }
    }

    try:
        response = es_client.search(
            index=LOAD_PROFILE_INDEX,
            body={"size": 10000, "_source": source_fields, "query": query},
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        log_exception(logger, "Elasticsearch search failed", exc, index_name=LOAD_PROFILE_INDEX, meter_no=meter_no, date=date)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_day_profile_month_docs_from_es(meter_no: str, date: str):
    target_date = parse_request_date(date)
    hyphen_month_pattern = f"*-{target_date.strftime('%m-%Y')}*"
    slash_month_pattern = f"*/{target_date.strftime('%m/%Y')}*"
    source_fields = ["meter_no", "datetime", "timestamp", "parameters"]
    query = {
        "bool": {
            "filter": [
                {"term": {"meter_no.keyword": meter_no}},
                {
                    "bool": {
                        "should": [
                            {"wildcard": {"datetime.keyword": {"value": hyphen_month_pattern}}},
                            {"wildcard": {"datetime.keyword": {"value": slash_month_pattern}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
            ]
        }
    }

    try:
        response = es_client.search(
            index=DAY_PROFILE_INDEX,
            body={"size": 10000, "_source": source_fields, "query": query},
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        log_exception(logger, "Elasticsearch search failed", exc, index_name=DAY_PROFILE_INDEX, meter_no=meter_no, date=date)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_event_docs_from_es(meter_no: str, date: str):
    target_date = parse_request_date(date)
    hyphen_date_pattern = f"{target_date.strftime('%d-%m-%Y')}*"
    slash_date_pattern = f"{target_date.strftime('%d/%m/%Y')}*"
    source_fields = ["meter_no", "event_index", "code", "status", "logid", "time", "timestamp", "parameters"]
    query = {
        "bool": {
            "filter": [
                {"term": {"meter_no.keyword": meter_no}},
                {
                    "bool": {
                        "should": [
                            {"wildcard": {"time.keyword": {"value": hyphen_date_pattern}}},
                            {"wildcard": {"time.keyword": {"value": slash_date_pattern}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
            ]
        }
    }

    try:
        response = es_client.search(
            index=EVENT_INDEX,
            body={"size": 5000, "_source": source_fields, "query": query},
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        log_exception(logger, "Elasticsearch search failed", exc, index_name=EVENT_INDEX, meter_no=meter_no, date=date)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_event_month_docs_from_es(meter_no: str, date: str):
    target_date = parse_request_date(date)
    hyphen_month_pattern = f"*-{target_date.strftime('%m-%Y')}*"
    slash_month_pattern = f"*/{target_date.strftime('%m/%Y')}*"
    source_fields = ["meter_no", "event_index", "code", "status", "logid", "time", "timestamp", "parameters"]
    query = {
        "bool": {
            "filter": [
                {"term": {"meter_no.keyword": meter_no}},
                {
                    "bool": {
                        "should": [
                            {"wildcard": {"time.keyword": {"value": hyphen_month_pattern}}},
                            {"wildcard": {"time.keyword": {"value": slash_month_pattern}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
            ]
        }
    }

    try:
        response = es_client.search(
            index=EVENT_INDEX,
            body={"size": 10000, "_source": source_fields, "query": query},
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        log_exception(logger, "Elasticsearch search failed", exc, index_name=EVENT_INDEX, meter_no=meter_no, date=date)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_billing_docs_from_es(meter_no: str, date: str):
    target_date = parse_request_date(date)
    hyphen_date_pattern = f"{target_date.strftime('%d-%m-%Y')}*"
    slash_date_pattern = f"{target_date.strftime('%d/%m/%Y')}*"
    source_fields = [
        "meter_no",
        "section",
        "date_time",
        "timestamp",
        "reset_method",
        "power_on_duration",
        "power_off_duration",
        "cumulative_tamper_count",
        "parameters",
    ]
    query = {
        "bool": {
            "filter": [
                {"term": {"meter_no.keyword": meter_no}},
                {
                    "bool": {
                        "should": [
                            {"wildcard": {"date_time.keyword": {"value": hyphen_date_pattern}}},
                            {"wildcard": {"date_time.keyword": {"value": slash_date_pattern}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
            ]
        }
    }

    try:
        response = es_client.search(
            index=BILLING_INDEX,
            body={"size": 5000, "_source": source_fields, "query": query},
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        log_exception(logger, "Elasticsearch search failed", exc, index_name=BILLING_INDEX, meter_no=meter_no, date=date)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_billing_month_docs_from_es(meter_no: str, date: str):
    target_date = parse_request_date(date)
    hyphen_month_pattern = f"*-{target_date.strftime('%m-%Y')}*"
    slash_month_pattern = f"*/{target_date.strftime('%m/%Y')}*"
    source_fields = [
        "meter_no",
        "section",
        "date_time",
        "timestamp",
        "reset_method",
        "power_on_duration",
        "power_off_duration",
        "cumulative_tamper_count",
        "parameters",
    ]
    query = {
        "bool": {
            "filter": [
                {"term": {"meter_no.keyword": meter_no}},
                {
                    "bool": {
                        "should": [
                            {"wildcard": {"date_time.keyword": {"value": hyphen_month_pattern}}},
                            {"wildcard": {"date_time.keyword": {"value": slash_month_pattern}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
            ]
        }
    }

    try:
        response = es_client.search(
            index=BILLING_INDEX,
            body={"size": 10000, "_source": source_fields, "query": query},
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        log_exception(logger, "Elasticsearch search failed", exc, index_name=BILLING_INDEX, meter_no=meter_no, date=date)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_all_billing_docs_from_es(meter_no: str):
    source_fields = [
        "meter_no",
        "section",
        "date_time",
        "timestamp",
        "reset_method",
        "power_on_duration",
        "power_off_duration",
        "cumulative_tamper_count",
        "parameters",
    ]
    query = {"bool": {"filter": [{"term": {"meter_no.keyword": meter_no}}]}}

    scroll_id = None
    all_hits = []
    try:
        response = es_client.search(
            index=BILLING_INDEX,
            body={"size": 1000, "_source": source_fields, "query": query},
            scroll="2m",
        )
        scroll_id = response.get("_scroll_id")
        hits = response.get("hits", {}).get("hits", [])
        all_hits.extend(hits)

        while hits:
            response = es_client.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id", scroll_id)
            hits = response.get("hits", {}).get("hits", [])
            all_hits.extend(hits)

        return all_hits
    except Exception as exc:
        log_exception(logger, "Elasticsearch scroll search failed", exc, index_name=BILLING_INDEX, meter_no=meter_no)
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")
    finally:
        if scroll_id:
            try:
                es_client.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


SQL_EXPORT_DATASETS = {
    "load_profile": {
        "index_name": LOAD_PROFILE_INDEX,
        "source_fields": ["meter_no", "date", "interval", "timestamp", "parameters"],
    },
    "billing": {
        "index_name": BILLING_INDEX,
        "source_fields": [
            "meter_no",
            "section",
            "date_time",
            "timestamp",
            "reset_method",
            "power_on_duration",
            "power_off_duration",
            "cumulative_tamper_count",
            "parameters",
        ],
    },
    "event": {
        "index_name": EVENT_INDEX,
        "source_fields": ["meter_no", "event_index", "code", "status", "logid", "time", "timestamp", "parameters"],
    },
    "day_profile": {
        "index_name": DAY_PROFILE_INDEX,
        "source_fields": ["meter_no", "datetime", "timestamp", "parameters"],
    },
}


def _build_sql_export_query(meter_nos: list[str] = None, start_date: str = None, end_date: str = None):
    filters = []

    if meter_nos:
        if len(meter_nos) == 1:
            filters.append({"term": {"meter_no.keyword": meter_nos[0]}})
        else:
            filters.append({"terms": {"meter_no.keyword": meter_nos}})

    if start_date and end_date:
        start_datetime = parse_request_date(start_date).replace(hour=0, minute=0, second=0)
        end_datetime = parse_request_date(end_date).replace(hour=23, minute=59, second=59)
        filters.append(
            {
                "range": {
                    "timestamp": {
                        "gte": start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                        "lte": end_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                }
            }
        )

    if not filters:
        return {"match_all": {}}

    return {"bool": {"filter": filters}}


def fetch_sql_export_docs_from_es(
    index_name: str,
    source_fields: list[str],
    meter_nos: list[str] = None,
    start_date: str = None,
    end_date: str = None,
):
    query = _build_sql_export_query(meter_nos=meter_nos, start_date=start_date, end_date=end_date)
    scroll_id = None
    all_hits = []

    try:
        response = es_client.search(
            index=index_name,
            body={"size": 1000, "_source": source_fields, "query": query},
            scroll="2m",
        )
        scroll_id = response.get("_scroll_id")
        hits = response.get("hits", {}).get("hits", [])
        all_hits.extend(hits)

        while hits:
            response = es_client.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id", scroll_id)
            hits = response.get("hits", {}).get("hits", [])
            all_hits.extend(hits)

        return all_hits
    except Exception as exc:
        log_exception(
            logger,
            "Elasticsearch SQL export search failed",
            exc,
            index_name=index_name,
            meter_nos=meter_nos or [],
            start_date=start_date,
            end_date=end_date,
        )
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")
    finally:
        if scroll_id:
            try:
                es_client.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


def fetch_all_sql_export_docs_from_es(meter_nos: list[str] = None, start_date: str = None, end_date: str = None):
    return {
        dataset_name: fetch_sql_export_docs_from_es(
            index_name=config["index_name"],
            source_fields=config["source_fields"],
            meter_nos=meter_nos,
            start_date=start_date,
            end_date=end_date,
        )
        for dataset_name, config in SQL_EXPORT_DATASETS.items()
    }


def build_load_profile_export_rows(hits: list):
    parameter_codes = set()
    for hit in hits:
        source = hit.get("_source", {})
        for parameter in source.get("parameters", []):
            code = str(parameter.get("code", "")).strip()
            if code:
                parameter_codes.add(code)

    code_to_column = get_parameter_mappings_for_table(DB_LOAD_PROFILE_TABLE, sorted(parameter_codes))
    parameter_headers = sorted({column for column in code_to_column.values()})
    rows = []

    for hit in hits:
        source = hit.get("_source", {})
        row = {
            "meter_no": source.get("meter_no", ""),
            "date": source.get("date", ""),
            "interval": source.get("interval", ""),
            "timestamp": source.get("timestamp", ""),
        }
        row.update({csv_column: "" for csv_column in parameter_headers})

        for parameter in source.get("parameters", []):
            code = parameter.get("code")
            column_name = code_to_column.get(code)
            if column_name:
                row[column_name] = parameter.get("value", "")

        rows.append(row)

    sorted_rows = sorted(
        rows,
        key=lambda item: (item["date"], int(item["interval"]) if str(item["interval"]).isdigit() else item["interval"]),
    )
    csv_headers = ["meter_no", "date", "interval", "timestamp"] + parameter_headers
    return sorted_rows, csv_headers


def build_load_profile_es_documents(flat_data: list):
    transformed_data = []
    base_keys = set(LOAD_PROFILE_CORE_HEADERS)

    for row in flat_data:
        meter_no = row.get("meter_no", "")
        date_val = row.get("date", "")
        interval_val = row.get("interval", "")
        timestamp_val = row.get("timestamp", "")
        custom_id = f"{meter_no}_{date_val}_{interval_val}"

        new_doc = {
            "_id": custom_id,
            "meter_no": meter_no,
            "date": date_val,
            "interval": interval_val,
            "timestamp": timestamp_val,
            "parameters": [],
        }

        for key, value in row.items():
            if key not in base_keys:
                new_doc["parameters"].append({"code": key, "value": value, "unit": ""})

        transformed_data.append(new_doc)

    return transformed_data


def build_billing_es_documents(flat_data: list):
    grouped_docs = {}
    base_keys = {
        "meter_no",
        "section",
        "date_time",
        "timestamp",
        "reset_method",
        "power_on_duration",
        "power_off_duration",
        "cumulative_tamper_count",
    }
    parameter_excluded_keys = {key.lower() for key in base_keys}
    parameter_excluded_keys.update({"b11", "b12", "b13", "b11_name", "b12_name", "b13_name"})

    for row in flat_data:
        meter_no = row.get("meter_no", "")
        section = row.get("section", "")
        date_time = row.get("date_time", "")
        timestamp = row.get("timestamp", "")
        reset_method = row.get("reset_method", "")
        custom_id = f"{meter_no}_{date_time}"
        doc_key = (custom_id, section)

        if doc_key not in grouped_docs:
            grouped_docs[doc_key] = {
                "_id": custom_id,
                "meter_no": meter_no,
                "section": section,
                "date_time": date_time,
                "timestamp": timestamp,
                "reset_method": reset_method,
                "power_on_duration": row.get("power_on_duration", ""),
                "power_off_duration": row.get("power_off_duration", ""),
                "cumulative_tamper_count": row.get("cumulative_tamper_count", ""),
                "parameters": [],
            }

        parameter = {
            key: value
            for key, value in row.items()
            if key.lower() not in parameter_excluded_keys and value not in (None, "")
        }
        grouped_docs[doc_key]["parameters"].append(parameter)

    return list(grouped_docs.values())


def build_event_es_documents(event_data: list):
    transformed_data = []
    for row in event_data:
        meter_no = row.get("meter_no", "")
        event_index = row.get("event_index") or 0
        code = row.get("code", "")
        status = row.get("status", "")
        logid = row.get("logid", "")
        event_time = row.get("time", "")
        transformed_data.append(
            {
                "_id": f"{meter_no}_{event_time}_{code}_{logid}_{event_index}",
                "meter_no": meter_no,
                "event_index": event_index,
                "code": code,
                "status": status,
                "logid": logid,
                "time": event_time,
                "timestamp": row.get("timestamp", ""),
                "parameters": row.get("parameters", []),
            }
        )
    return transformed_data


def build_day_profile_es_documents(day_profile_data: list):
    transformed_data = []
    for row in day_profile_data:
        meter_no = row.get("meter_no", "")
        snapshot_datetime = row.get("datetime", "")
        transformed_data.append(
            {
                "_id": f"{meter_no}_{snapshot_datetime}",
                "meter_no": meter_no,
                "datetime": snapshot_datetime,
                "timestamp": row.get("timestamp", ""),
                "parameters": row.get("parameters", []),
            }
        )
    return transformed_data


ALL_DATASET_PIPELINES = (
    {
        "name": "instantaneous",
        "extractor": parser.extract_instantaneous,
        "index_name": INSTANTANEOUS_INDEX,
        "transformer": None,
    },
    {
        "name": "load_profile",
        "extractor": parser.extract_load_profile,
        "index_name": LOAD_PROFILE_INDEX,
        "transformer": build_load_profile_es_documents,
    },
    {
        "name": "billing",
        "extractor": parser.extract_billing,
        "index_name": BILLING_INDEX,
        "transformer": build_billing_es_documents,
    },
    {
        "name": "event",
        "extractor": parser.extract_events,
        "index_name": EVENT_INDEX,
        "transformer": build_event_es_documents,
    },
    {
        "name": "day_profile",
        "extractor": parser.extract_day_profile,
        "index_name": DAY_PROFILE_INDEX,
        "transformer": build_day_profile_es_documents,
    },
)


def _make_dataset_stats():
    return {
        pipeline["name"]: {
            "published_records": 0,
            "successful_files": 0,
            "empty_files": 0,
            "failed_files": 0,
        }
        for pipeline in ALL_DATASET_PIPELINES
    }


def _process_all_datasets_for_root(root, file_path: str, meter_no: str, dataset_stats: dict):
    file_results = {}

    for pipeline in ALL_DATASET_PIPELINES:
        dataset_name = pipeline["name"]
        try:
            raw_rows = pipeline["extractor"](root, meter_no, file_path=file_path)
            if not raw_rows:
                dataset_stats[dataset_name]["empty_files"] += 1
                file_results[dataset_name] = {
                    "status": "empty",
                    "published_records": 0,
                }
                continue

            transformed_rows = (
                pipeline["transformer"](raw_rows) if pipeline["transformer"] else raw_rows
            )
            publish_result = publish_to_es_helper(transformed_rows, index_name=pipeline["index_name"])
            published_records = len(transformed_rows)
            dataset_stats[dataset_name]["published_records"] += published_records
            dataset_stats[dataset_name]["successful_files"] += 1
            file_results[dataset_name] = {
                "status": "success",
                "published_records": published_records,
                "elasticsearch": publish_result,
            }
        except HTTPException as exc:
            detail = str(getattr(exc, "detail", exc))
            if "Missing required XML section" in detail or ("No " in detail and "records found" in detail):
                dataset_stats[dataset_name]["empty_files"] += 1
                file_results[dataset_name] = {
                    "status": "skipped",
                    "published_records": 0,
                    "reason": detail,
                }
                continue

            dataset_stats[dataset_name]["failed_files"] += 1
            failure = log_processing_failure(file_path, f"publish_all_data_to_es:{dataset_name}", exc)
            file_results[dataset_name] = {
                "status": "failed",
                "published_records": 0,
                "error": failure,
            }
        except Exception as exc:
            dataset_stats[dataset_name]["failed_files"] += 1
            failure = log_processing_failure(file_path, f"publish_all_data_to_es:{dataset_name}", exc)
            file_results[dataset_name] = {
                "status": "failed",
                "published_records": 0,
                "error": failure,
            }

    return file_results


def publish_directory_data_to_es(directory_path: str, extractor, index_name: str, transformer=None):
    all_data = []
    processed_files = 0
    skipped_files = []
    failed_files = []
    files = parser.get_cdf_files(directory_path)

    for file in files:
        file_path = os.path.join(directory_path, file)
        try:
            tree = parse_cdf_xml(file_path)
            root = tree.getroot()
            meter_no = require_meter_no(root, file_path=file_path, operation="publish_directory_data_to_es")
            data = extractor(root, meter_no, file_path=file_path)
            if transformer:
                data = transformer(data)
            all_data.extend(data)
            processed_files += 1
        except Exception as exc:
            if is_expected_file_processing_issue(exc):
                skipped_files.append(_build_skipped_file_result(file_path, "publish_directory_data_to_es", exc))
                continue

            failed_files.append(log_processing_failure(file_path, "publish_directory_data_to_es", exc))
            continue

    if all_data:
        es_result = publish_to_es_helper(all_data, index_name=index_name)
    else:
        es_result = {"message": f"No records found to insert into {index_name}"}

    return {
        "status": "success",
        "directory_path": directory_path,
        "processed_files": processed_files,
        "skipped_files": len(skipped_files),
        "skipped_file_details": skipped_files,
        "failed_files_count": len(failed_files),
        "failed_files": failed_files,
        "total_records": len(all_data),
        "elasticsearch": es_result,
    }


def publish_all_data_to_es(file_path: str = None, directory_path: str = None):
    if not file_path and not directory_path:
        raise HTTPException(status_code=400, detail="Provide either file_path or directory_path.")

    source_files = [file_path] if file_path else [os.path.join(directory_path, file_name) for file_name in parser.get_cdf_files(directory_path)]
    dataset_stats = _make_dataset_stats()
    file_results = []
    processed_files = 0
    skipped_files = []
    failed_files = []

    for source_file in source_files:
        try:
            tree = parse_cdf_xml(source_file)
            root = tree.getroot()
            meter_no = require_meter_no(root, file_path=source_file, operation="publish_all_data_to_es")
            results = _process_all_datasets_for_root(root, source_file, meter_no, dataset_stats)
            processed_files += 1
            file_results.append(
                {
                    "file_path": source_file,
                    "meter_no": meter_no,
                    "datasets": results,
                }
            )
        except Exception as exc:
            if is_expected_file_processing_issue(exc):
                skipped_files.append(_build_skipped_file_result(source_file, "publish_all_data_to_es", exc))
                continue

            failed_files.append(log_processing_failure(source_file, "publish_all_data_to_es", exc))

    return {
        "status": "success",
        "source_type": "file" if file_path else "directory",
        "file_path": file_path,
        "directory_path": directory_path,
        "processed_files": processed_files,
        "skipped_files": len(skipped_files),
        "skipped_file_details": skipped_files,
        "failed_files_count": len(failed_files),
        "failed_files": failed_files,
        "datasets": dataset_stats,
        "file_results": file_results[:100],
    }
