import os
import xml.etree.ElementTree as ET
from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from fastapi import HTTPException

from .. import parser
from ..config import (
    DB_LOAD_PROFILE_TABLE,
    ES_API_KEY,
    ES_ENDPOINT,
    DAY_PROFILE_INDEX,
    EVENT_INDEX,
    LOAD_PROFILE_CORE_HEADERS,
    LOAD_PROFILE_INDEX,
)
from .sql_service import get_parameter_mappings_for_table

es_client = Elasticsearch(
    ES_ENDPOINT,
    api_key=ES_API_KEY,
)


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
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


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
    base_keys = {"meter_no", "section", "date_time", "timestamp", "reset_method"}

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
                "parameters": [],
            }

        parameter = {key: value for key, value in row.items() if key not in base_keys and value not in (None, "")}
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


def publish_directory_data_to_es(directory_path: str, extractor, index_name: str, transformer=None):
    all_data = []
    processed_files = 0
    skipped_files = 0
    files = parser.get_cdf_files(directory_path)

    for file in files:
        file_path = os.path.join(directory_path, file)
        try:
            tree = ET.parse(file_path)
            meter_no = parser.get_meter_no(tree.getroot())
            data = extractor(tree.getroot(), meter_no)
            if transformer:
                data = transformer(data)
            all_data.extend(data)
            processed_files += 1
        except ET.ParseError:
            skipped_files += 1
            continue

    if all_data:
        es_result = publish_to_es_helper(all_data, index_name=index_name)
    else:
        es_result = {"message": f"No records found to insert into {index_name}"}

    return {
        "status": "success",
        "directory_path": directory_path,
        "processed_files": processed_files,
        "skipped_files": skipped_files,
        "total_records": len(all_data),
        "elasticsearch": es_result,
    }
