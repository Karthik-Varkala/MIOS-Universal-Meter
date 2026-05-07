import csv
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO
from pathlib import Path

import boto3
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from . import parser
from .models import DirectoryRequest, FileRequest, LoadProfileExportRequest, MeterDateRequest, S3Request


load_dotenv()


app = FastAPI(
    title="CDF Data API",
    description="Extracts meter data, processes single files, handles S3 downloads, and publishes to Elasticsearch.",
)

# --- Elasticsearch Configuration ---
ES_ENDPOINT = os.getenv("ES_ENDPOINT")
ES_API_KEY = os.getenv("ES_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "MDMS")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_LOAD_PROFILE_TABLE = os.getenv("DB_LOAD_PROFILE_TABLE", "LOAD_PROFILE")
DB_DAY_PROFILE_TABLE = os.getenv("DB_DAY_PROFILE_TABLE", "DAY_PROFILE")
DB_EVENT_TABLE = os.getenv("DB_EVENT_TABLE", "EVENT_DATA")
DB_EVENT_PARAMETER_TABLE = os.getenv("DB_EVENT_PARAMETER_TABLE", "EVENT_PARAMETER_DATA")

es_client = Elasticsearch(
    ES_ENDPOINT,
    api_key=ES_API_KEY,
)

LOAD_PROFILE_INDEX = "meter-load-profile-data"
EVENT_INDEX = "meter-event-data"
DAY_PROFILE_INDEX = "meter-day-profile-data"
LOAD_PROFILE_PARAMETER_MAPPINGS = [
    ("Current R", "CURRENT_R", "P2-1-1-4-0"),
    ("Current Y", "CURRENT_Y", "P2-1-2-4-0"),
    ("Current B", "CURRENT_B", "P2-1-3-4-0"),
    ("Voltage R", "VOLTAGE_R", "P1-2-1-4-0"),
    ("Voltage Y", "VOLTAGE_Y", "P1-2-2-4-0"),
    ("Voltage B", "VOLTAGE_B", "P1-2-3-4-0"),
    ("kwh", "KWH", "P7-1-18-1-0"),
    ("kvah", "KVAH", "P7-1-18-2-0"),
    ("power", "POWER", "P7-3-18-2-0"),
    ("kvarh-lag", "KVARH_LAG", "P7-2-1-0-0"),
    ("kvarh-lead", "KVARH_LEAD", "P7-2-2-0-0"),
]
DAY_PROFILE_PARAMETER_MAPPINGS = [
    ("kwh_forwarded_total", "KWH_FORWARDED_TOTAL", "P7-1-18-2-0"),
    ("kwh_forwarded_fund", "KWH_FORWARDED_FUND", "P7-1-18-1-0"),
    ("kwh_import_fund", "KWH_IMPORT_FUND", "P7-1-5-1-0"),
    ("kwh_export_total", "KWH_EXPORT_TOTAL", "P7-1-6-2-0"),
    ("kwh_export_fund", "KWH_EXPORT_FUND", "P7-1-6-1-0"),
    ("kvah_forwarded_total", "KVAH_FORWARDED_TOTAL", "P7-3-18-2-0"),
    ("kvah_import_total", "KVAH_IMPORT_TOTAL", "P7-3-5-2-0"),
    ("kvah_export_total", "KVAH_EXPORT_TOTAL", "P7-3-6-0-0"),
    ("kvarh_q1_lag", "KVARH_Q1_LAG", "P7-2-1-0-0"),
    ("kvarh_q2_lead", "KVARH_Q2_LEAD", "P7-2-2-0-0"),
    ("kvarh_q3", "KVARH_Q3", "P7-2-3-0-0"),
    ("kvarh_q4", "KVARH_Q4", "P7-2-4-0-0"),
    ("kwh_import_total_snap", "KWH_IMPORT_TOTAL_SNAPSHOT", "P1202-1-5-2-0"),
    ("kwh_forwarded_fund_snap", "KWH_FORWARDED_FUND_SNAPSHOT", "P1203-1-14-1-0"),
    ("kvarh_q1_snap", "KVARH_Q1_SNAPSHOT", "P1202-2-19-0-0"),
    ("kvarh_q4_snap", "KVARH_Q4_SNAPSHOT", "P1202-2-20-0-0"),
]
EVENT_SQL_UNIQUE_KEY_COLUMNS = (
    "METER_NO",
    "EVENT_DATE_TIME",
    "SOURCE_EVENT_CODE",
    "LOGID",
    "SOURCE_EVENT_INDEX",
)

INSTANTANEOUS_HEADERS = ["meter_no", "code", "value", "unit"]
LOAD_PROFILE_CORE_HEADERS = ["meter_no", "date", "interval", "timestamp"]
BILLING_HEADERS = [
    "meter_no",
    "section",
    "date_time",
    "timestamp",
    "reset_method",
    "tag",
    "code",
    "value",
    "unit",
    "tod",
    "occdate",
    "mechanism_code",
]


# ==========================================
# DIRECTORY PROCESSING ENDPOINTS
# ==========================================

@app.post("/api/dir/instantaneous")
def get_dir_instantaneous(req: DirectoryRequest):
    all_data = []
    files = parser.get_cdf_files(req.directory_path)
    for file in files:
        file_path = os.path.join(req.directory_path, file)
        try:
            tree = ET.parse(file_path)
            meter_no = parser.get_meter_no(tree.getroot())
            data = parser.extract_instantaneous(tree.getroot(), meter_no)
            all_data.extend(data)

            if data:
                csv_path = os.path.join(req.directory_path, f"{Path(file).stem}_Instantaneous.csv")
                parser.save_csv(data, csv_path, INSTANTANEOUS_HEADERS)
        except ET.ParseError:
            continue
    return {"status": "success", "total_records": len(all_data), "preview": all_data[:100]}


@app.post("/api/dir/load-profile")
def get_dir_load_profile(req: DirectoryRequest):
    all_data = []
    files = parser.get_cdf_files(req.directory_path)
    for file in files:
        file_path = os.path.join(req.directory_path, file)
        try:
            tree = ET.parse(file_path)
            meter_no = parser.get_meter_no(tree.getroot())
            data = parser.extract_load_profile(tree.getroot(), meter_no)
            all_data.extend(data)

            if data:
                csv_path = os.path.join(req.directory_path, f"{Path(file).stem}_LoadProfile.csv")
                other_headers = sorted(list(set(k for row in data for k in row.keys() if k not in LOAD_PROFILE_CORE_HEADERS)))
                parser.save_csv(data, csv_path, LOAD_PROFILE_CORE_HEADERS + other_headers, is_dict_writer=True)
        except ET.ParseError:
            continue
    return {"status": "success", "total_records": len(all_data), "preview": all_data[:100]}


@app.post("/api/dir/billing")
def get_dir_billing(req: DirectoryRequest):
    all_data = []
    files = parser.get_cdf_files(req.directory_path)
    for file in files:
        file_path = os.path.join(req.directory_path, file)
        try:
            tree = ET.parse(file_path)
            meter_no = parser.get_meter_no(tree.getroot())
            data = parser.extract_billing(tree.getroot(), meter_no)
            all_data.extend(data)

            if data:
                csv_path = os.path.join(req.directory_path, f"{Path(file).stem}_Billing.csv")
                parser.save_csv(data, csv_path, BILLING_HEADERS)
        except ET.ParseError:
            continue
    return {"status": "success", "total_records": len(all_data), "preview": all_data[:100]}


# ==========================================
# SINGLE FILE PROCESSING ENDPOINTS
# ==========================================

@app.post("/api/file/instantaneous")
def get_single_file_instantaneous(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_instantaneous(tree.getroot(), meter_no)

        if data:
            directory = os.path.dirname(req.file_path)
            csv_path = os.path.join(directory, f"{Path(req.file_path).stem}_Instantaneous.csv")
            parser.save_csv(data, csv_path, INSTANTANEOUS_HEADERS)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/file/load-profile")
def get_single_file_load_profile(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_load_profile(tree.getroot(), meter_no)

        if data:
            directory = os.path.dirname(req.file_path)
            csv_path = os.path.join(directory, f"{Path(req.file_path).stem}_LoadProfile.csv")
            other_headers = sorted(list(set(k for row in data for k in row.keys() if k not in LOAD_PROFILE_CORE_HEADERS)))
            parser.save_csv(data, csv_path, LOAD_PROFILE_CORE_HEADERS + other_headers, is_dict_writer=True)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/file/billing")
def get_single_file_billing(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_billing(tree.getroot(), meter_no)

        if data:
            directory = os.path.dirname(req.file_path)
            csv_path = os.path.join(directory, f"{Path(req.file_path).stem}_Billing.csv")
            parser.save_csv(data, csv_path, BILLING_HEADERS)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


# ==========================================
# S3 INTEGRATION ENDPOINTS
# ==========================================

def download_s3_helper(req: S3Request) -> str:
    """Helper function to download the file from S3 and return the local path."""
    os.makedirs(req.download_dir, exist_ok=True)
    local_file_path = os.path.join(req.download_dir, os.path.basename(req.object_key))

    s3 = boto3.client("s3")
    try:
        s3.download_file(req.bucket_name, req.object_key, local_file_path)
        return local_file_path
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download from S3: {str(e)}")


@app.post("/api/s3/instantaneous")
def process_s3_instantaneous(req: S3Request):
    local_file_path = download_s3_helper(req)

    try:
        tree = ET.parse(local_file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_instantaneous(tree.getroot(), meter_no)

        if data:
            directory = os.path.dirname(local_file_path)
            csv_path = os.path.join(directory, f"{Path(local_file_path).stem}_Instantaneous.csv")
            parser.save_csv(data, csv_path, INSTANTANEOUS_HEADERS)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError:
        raise HTTPException(status_code=400, detail="Invalid XML format in downloaded file.")


@app.post("/api/s3/load-profile")
def process_s3_load_profile(req: S3Request):
    local_file_path = download_s3_helper(req)

    try:
        tree = ET.parse(local_file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_load_profile(tree.getroot(), meter_no)

        if data:
            directory = os.path.dirname(local_file_path)
            csv_path = os.path.join(directory, f"{Path(local_file_path).stem}_LoadProfile.csv")
            other_headers = sorted(list(set(k for row in data for k in row.keys() if k not in LOAD_PROFILE_CORE_HEADERS)))
            parser.save_csv(data, csv_path, LOAD_PROFILE_CORE_HEADERS + other_headers, is_dict_writer=True)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError:
        raise HTTPException(status_code=400, detail="Invalid XML format in downloaded file.")


@app.post("/api/s3/billing")
def process_s3_billing(req: S3Request):
    local_file_path = download_s3_helper(req)

    try:
        tree = ET.parse(local_file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_billing(tree.getroot(), meter_no)

        if data:
            directory = os.path.dirname(local_file_path)
            csv_path = os.path.join(directory, f"{Path(local_file_path).stem}_Billing.csv")
            parser.save_csv(data, csv_path, BILLING_HEADERS)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError:
        raise HTTPException(status_code=400, detail="Invalid XML format in downloaded file.")


# ==========================================
# ELASTICSEARCH INTEGRATION ENDPOINTS
# ==========================================

def publish_to_es_helper(data: list, index_name: str):
    """Helper function to push data to Elasticsearch."""
    try:
        actions = []
        for record in data:
            action = {
                "_index": index_name,
                "_source": record
            }
            
            # If we attached a custom _id to the record, extract it for Elasticsearch
            if "_id" in record:
                action["_id"] = record.pop("_id") 
                
            actions.append(action)

        success, _ = helpers.bulk(es_client, actions)
        return {"message": f"Successfully inserted {success} records into {index_name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch Bulk Insert Error: {str(e)}")


def quote_mysql_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def get_mysql_connection():
    """Create a MySQL connection for SQL export operations."""
    missing_env = [
        env_name
        for env_name, env_value in {
            "DB_HOST": DB_HOST,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
        }.items()
        if not env_value
    ]
    if missing_env:
        raise HTTPException(
            status_code=500,
            detail=f"Missing database configuration: {', '.join(missing_env)}",
        )

    try:
        import mysql.connector
    except ModuleNotFoundError:
        raise HTTPException(
            status_code=500,
            detail="mysql-connector-python is not installed. Install it in the virtual environment to use SQL export.",
        )

    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {quote_mysql_identifier(DB_NAME)}")
        cursor.execute(f"USE {quote_mysql_identifier(DB_NAME)}")
        cursor.close()
        return connection
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MySQL Connection Error: {str(exc)}")


def ensure_load_profile_sql_table(connection):
    """Create the load profile table if it does not already exist."""
    fixed_columns = [
        ("METER_NO", "VARCHAR(64) NOT NULL"),
        ("DATETIME_TIMESTAMP", "DATETIME NOT NULL"),
    ]
    parameter_columns = [
        (sql_column, "DECIMAL(18,6) NULL")
        for _, sql_column, _ in LOAD_PROFILE_PARAMETER_MAPPINGS
    ]
    column_defs = [
        f"{quote_mysql_identifier(column_name)} {column_type}"
        for column_name, column_type in fixed_columns + parameter_columns
    ]
    primary_key = ", ".join(
        quote_mysql_identifier(column_name)
        for column_name in ("METER_NO", "DATETIME_TIMESTAMP")
    )
    create_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {quote_mysql_identifier(DB_LOAD_PROFILE_TABLE)} ("
        f"{', '.join(column_defs)}, PRIMARY KEY ({primary_key}))"
    )

    cursor = connection.cursor()
    try:
        cursor.execute(create_table_sql)
        connection.commit()
    finally:
        cursor.close()


def get_load_profile_sql_headers():
    return ["METER_NO", "DATETIME_TIMESTAMP"] + [
        sql_column for _, sql_column, _ in LOAD_PROFILE_PARAMETER_MAPPINGS
    ]


def prepare_load_profile_sql_value(column_name: str, value):
    if value in ("", None):
        return None
    if column_name == "DATETIME_TIMESTAMP":
        for timestamp_format in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, timestamp_format)
            except ValueError:
                continue
    return value


def build_load_profile_sql_rows(hits: list):
    """Flatten selected parameter codes into SQL-table-shaped rows."""
    code_to_sql_column = {
        code: sql_column
        for _, sql_column, code in LOAD_PROFILE_PARAMETER_MAPPINGS
    }
    rows = []

    for hit in hits:
        source = hit.get("_source", {})
        row = {
            "METER_NO": source.get("meter_no", ""),
            "DATETIME_TIMESTAMP": source.get("timestamp", ""),
        }
        row.update(
            {
                sql_column: None
                for _, sql_column, _ in LOAD_PROFILE_PARAMETER_MAPPINGS
            }
        )

        for parameter in source.get("parameters", []):
            code = parameter.get("code")
            sql_column = code_to_sql_column.get(code)
            if sql_column:
                row[sql_column] = parameter.get("value", "")

        rows.append(row)

    return sorted(rows, key=lambda item: item["DATETIME_TIMESTAMP"])


def save_load_profile_rows_to_sql(rows: list):
    """Persist load profile rows to the configured MySQL table."""
    connection = get_mysql_connection()
    headers = get_load_profile_sql_headers()
    quoted_columns = ", ".join(quote_mysql_identifier(header) for header in headers)
    placeholders = ", ".join(["%s"] * len(headers))
    update_clause = ", ".join(
        f"{quote_mysql_identifier(header)} = VALUES({quote_mysql_identifier(header)})"
        for header in headers
        if header not in {"METER_NO", "DATETIME_TIMESTAMP"}
    )
    insert_sql = (
        f"INSERT INTO {quote_mysql_identifier(DB_LOAD_PROFILE_TABLE)} ({quoted_columns}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )
    values = [
        tuple(prepare_load_profile_sql_value(header, row.get(header)) for header in headers)
        for row in rows
    ]

    try:
        ensure_load_profile_sql_table(connection)
        cursor = connection.cursor()
        try:
            cursor.executemany(insert_sql, values)
            connection.commit()
            affected_rows = cursor.rowcount
            return affected_rows
        finally:
            cursor.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MySQL Insert Error: {str(exc)}")
    finally:
        connection.close()


def ensure_day_profile_sql_table(connection):
    """Create the day profile table if it does not already exist."""
    fixed_columns = [
        ("METER_NO", "VARCHAR(64) NOT NULL"),
        ("DATETIME_TIMESTAMP", "DATETIME NOT NULL"),
    ]
    parameter_columns = [
        (sql_column, "DECIMAL(18,6) NULL")
        for _, sql_column, _ in DAY_PROFILE_PARAMETER_MAPPINGS
    ]
    column_defs = [
        f"{quote_mysql_identifier(column_name)} {column_type}"
        for column_name, column_type in fixed_columns + parameter_columns
    ]
    primary_key = ", ".join(
        quote_mysql_identifier(column_name)
        for column_name in ("METER_NO", "DATETIME_TIMESTAMP")
    )
    create_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {quote_mysql_identifier(DB_DAY_PROFILE_TABLE)} ("
        f"{', '.join(column_defs)}, PRIMARY KEY ({primary_key}))"
    )

    cursor = connection.cursor()
    try:
        cursor.execute(create_table_sql)
        connection.commit()
    finally:
        cursor.close()


def get_day_profile_sql_headers():
    return ["METER_NO", "DATETIME_TIMESTAMP"] + [
        sql_column for _, sql_column, _ in DAY_PROFILE_PARAMETER_MAPPINGS
    ]


def prepare_day_profile_sql_value(column_name: str, value):
    return prepare_load_profile_sql_value(column_name, value)


def build_day_profile_sql_rows(hits: list):
    """Flatten selected D6 register codes into SQL-table-shaped rows."""
    code_to_sql_column = {
        code: sql_column
        for _, sql_column, code in DAY_PROFILE_PARAMETER_MAPPINGS
    }
    rows = []

    for hit in hits:
        source = hit.get("_source", {})
        row = {
            "METER_NO": source.get("meter_no", ""),
            "DATETIME_TIMESTAMP": source.get("timestamp", ""),
        }
        row.update(
            {
                sql_column: None
                for _, sql_column, _ in DAY_PROFILE_PARAMETER_MAPPINGS
            }
        )

        for parameter in source.get("parameters", []):
            code = parameter.get("code")
            sql_column = code_to_sql_column.get(code)
            if sql_column:
                row[sql_column] = parameter.get("value", "")

        rows.append(row)

    return sorted(rows, key=lambda item: item["DATETIME_TIMESTAMP"])


def save_day_profile_rows_to_sql(rows: list):
    """Persist day profile rows to the configured MySQL table."""
    connection = get_mysql_connection()
    headers = get_day_profile_sql_headers()
    quoted_columns = ", ".join(quote_mysql_identifier(header) for header in headers)
    placeholders = ", ".join(["%s"] * len(headers))
    update_clause = ", ".join(
        f"{quote_mysql_identifier(header)} = VALUES({quote_mysql_identifier(header)})"
        for header in headers
        if header not in {"METER_NO", "DATETIME_TIMESTAMP"}
    )
    insert_sql = (
        f"INSERT INTO {quote_mysql_identifier(DB_DAY_PROFILE_TABLE)} ({quoted_columns}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )
    values = [
        tuple(prepare_day_profile_sql_value(header, row.get(header)) for header in headers)
        for row in rows
    ]

    try:
        ensure_day_profile_sql_table(connection)
        cursor = connection.cursor()
        try:
            cursor.executemany(insert_sql, values)
            connection.commit()
            affected_rows = cursor.rowcount
            return affected_rows
        finally:
            cursor.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MySQL Insert Error: {str(exc)}")
    finally:
        connection.close()


def ensure_event_sql_tables(connection):
    """Create the event parent and parameter child tables if they do not exist."""
    event_table = quote_mysql_identifier(DB_EVENT_TABLE)
    parameter_table = quote_mysql_identifier(DB_EVENT_PARAMETER_TABLE)
    unique_key = ", ".join(
        quote_mysql_identifier(column_name)
        for column_name in EVENT_SQL_UNIQUE_KEY_COLUMNS
    )
    create_event_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {event_table} ("
        f"{quote_mysql_identifier('ID')} BIGINT NOT NULL AUTO_INCREMENT, "
        f"{quote_mysql_identifier('METER_NO')} VARCHAR(64) NOT NULL, "
        f"{quote_mysql_identifier('SOURCE_EVENT_CODE')} VARCHAR(128) NOT NULL, "
        f"{quote_mysql_identifier('EVENT_STATUS')} VARCHAR(128) NULL, "
        f"{quote_mysql_identifier('LOGID')} VARCHAR(128) NOT NULL, "
        f"{quote_mysql_identifier('EVENT_DATE_TIME')} DATETIME NOT NULL, "
        f"{quote_mysql_identifier('SOURCE_EVENT_INDEX')} INT NOT NULL, "
        f"PRIMARY KEY ({quote_mysql_identifier('ID')}), "
        f"UNIQUE KEY {quote_mysql_identifier('UQ_EVENT')} ({unique_key})"
        f")"
    )
    create_parameter_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {parameter_table} ("
        f"{quote_mysql_identifier('ID')} BIGINT NOT NULL AUTO_INCREMENT, "
        f"{quote_mysql_identifier('EVENT_ID')} BIGINT NOT NULL, "
        f"{quote_mysql_identifier('PARAMETER_INDEX')} INT NOT NULL, "
        f"{quote_mysql_identifier('PARAMETER_CODE')} VARCHAR(128) NOT NULL, "
        f"{quote_mysql_identifier('VALUE')} VARCHAR(255) NULL, "
        f"{quote_mysql_identifier('UNIT')} VARCHAR(64) NULL, "
        f"PRIMARY KEY ({quote_mysql_identifier('ID')}), "
        f"UNIQUE KEY {quote_mysql_identifier('UQ_EVENT_PARAMETER')} "
        f"({quote_mysql_identifier('EVENT_ID')}, {quote_mysql_identifier('PARAMETER_INDEX')}), "
        f"CONSTRAINT {quote_mysql_identifier('FK_EVENT_PARAMETER_EVENT')} "
        f"FOREIGN KEY ({quote_mysql_identifier('EVENT_ID')}) "
        f"REFERENCES {event_table} ({quote_mysql_identifier('ID')}) "
        f"ON DELETE CASCADE"
        f")"
    )

    cursor = connection.cursor()
    try:
        cursor.execute(create_event_table_sql)
        cursor.execute(create_parameter_table_sql)
        cursor.execute(f"SHOW COLUMNS FROM {event_table}")
        event_columns = {column[0] for column in cursor.fetchall()}
        required_event_columns = {
            "ID",
            "METER_NO",
            "SOURCE_EVENT_CODE",
            "EVENT_STATUS",
            "LOGID",
            "EVENT_DATE_TIME",
            "SOURCE_EVENT_INDEX",
        }
        if not required_event_columns.issubset(event_columns):
            missing_columns = sorted(required_event_columns - event_columns)
            raise RuntimeError(
                f"{DB_EVENT_TABLE} already exists with the old event schema. "
                f"Missing columns: {', '.join(missing_columns)}. "
                f"Create a migrated parent event table or rename DB_EVENT_TABLE."
            )

        cursor.execute(f"SHOW COLUMNS FROM {parameter_table}")
        parameter_columns = {column[0] for column in cursor.fetchall()}
        required_parameter_columns = {
            "ID",
            "EVENT_ID",
            "PARAMETER_INDEX",
            "PARAMETER_CODE",
            "VALUE",
            "UNIT",
        }
        if not required_parameter_columns.issubset(parameter_columns):
            missing_columns = sorted(required_parameter_columns - parameter_columns)
            raise RuntimeError(
                f"{DB_EVENT_PARAMETER_TABLE} already exists with an incompatible schema. "
                f"Missing columns: {', '.join(missing_columns)}."
            )

        connection.commit()
    finally:
        cursor.close()


def prepare_event_sql_datetime(value):
    return prepare_load_profile_sql_value("DATETIME_TIMESTAMP", value)


def build_event_sql_rows(hits: list):
    """Build parent event rows with child parameter rows for normalized SQL storage."""
    grouped_rows = {}

    for hit in hits:
        source = hit.get("_source", {})
        meter_no = source.get("meter_no", "")
        event_index = source.get("event_index") or 0
        timestamp = source.get("timestamp", "") or parser.format_datetime_to_iso(source.get("time", ""))
        source_event_code = source.get("code", "")
        logid = source.get("logid", "")
        row_key = (meter_no, timestamp, source_event_code, logid, event_index)

        if row_key not in grouped_rows:
            grouped_rows[row_key] = {
                "METER_NO": meter_no,
                "EVENT_DATE_TIME": timestamp,
                "SOURCE_EVENT_CODE": source_event_code,
                "EVENT_STATUS": source.get("status", ""),
                "LOGID": logid,
                "SOURCE_EVENT_INDEX": event_index,
                "PARAMETERS": [],
            }

        row = grouped_rows[row_key]
        if source.get("status") not in ("", None):
            row["EVENT_STATUS"] = source.get("status", "")

        for fallback_index, parameter in enumerate(source.get("parameters", []), start=1):
            parameter_code = parameter.get("code")
            if not parameter_code:
                continue

            row["PARAMETERS"].append(
                {
                    "PARAMETER_INDEX": parameter.get("parameter_index") or fallback_index,
                    "PARAMETER_CODE": parameter_code,
                    "VALUE": parameter.get("value"),
                    "UNIT": parameter.get("unit"),
                }
            )

    rows = []
    for row in grouped_rows.values():
        row["PARAMETERS"] = sorted(
            row["PARAMETERS"],
            key=lambda parameter: parameter["PARAMETER_INDEX"],
        )
        rows.append(row)

    return sorted(
        rows,
        key=lambda item: (
            item["EVENT_DATE_TIME"],
            item["SOURCE_EVENT_CODE"],
            item["LOGID"],
            item["SOURCE_EVENT_INDEX"],
        ),
    )


def save_event_rows_to_sql(rows: list):
    """Persist event parent rows and their parameter child rows to MySQL."""
    connection = get_mysql_connection()
    event_headers = [
        "METER_NO",
        "SOURCE_EVENT_CODE",
        "EVENT_STATUS",
        "LOGID",
        "EVENT_DATE_TIME",
        "SOURCE_EVENT_INDEX",
    ]
    event_columns = ", ".join(quote_mysql_identifier(header) for header in event_headers)
    event_placeholders = ", ".join(["%s"] * len(event_headers))
    event_update_clause = (
        f"{quote_mysql_identifier('EVENT_STATUS')} = VALUES({quote_mysql_identifier('EVENT_STATUS')}), "
        f"{quote_mysql_identifier('ID')} = LAST_INSERT_ID({quote_mysql_identifier('ID')})"
    )
    event_insert_sql = (
        f"INSERT INTO {quote_mysql_identifier(DB_EVENT_TABLE)} ({event_columns}) "
        f"VALUES ({event_placeholders}) "
        f"ON DUPLICATE KEY UPDATE {event_update_clause}"
    )
    parameter_insert_sql = (
        f"INSERT INTO {quote_mysql_identifier(DB_EVENT_PARAMETER_TABLE)} "
        f"({quote_mysql_identifier('EVENT_ID')}, {quote_mysql_identifier('PARAMETER_INDEX')}, "
        f"{quote_mysql_identifier('PARAMETER_CODE')}, {quote_mysql_identifier('VALUE')}, "
        f"{quote_mysql_identifier('UNIT')}) "
        f"VALUES (%s, %s, %s, %s, %s) "
        f"ON DUPLICATE KEY UPDATE "
        f"{quote_mysql_identifier('PARAMETER_CODE')} = VALUES({quote_mysql_identifier('PARAMETER_CODE')}), "
        f"{quote_mysql_identifier('VALUE')} = VALUES({quote_mysql_identifier('VALUE')}), "
        f"{quote_mysql_identifier('UNIT')} = VALUES({quote_mysql_identifier('UNIT')})"
    )

    try:
        ensure_event_sql_tables(connection)
        cursor = connection.cursor()
        try:
            affected_rows = 0
            parameter_rows = 0
            for row in rows:
                event_values = (
                    "" if row.get("METER_NO") is None else str(row.get("METER_NO")),
                    "" if row.get("SOURCE_EVENT_CODE") is None else str(row.get("SOURCE_EVENT_CODE")),
                    None if row.get("EVENT_STATUS") in ("", None) else str(row.get("EVENT_STATUS")),
                    "" if row.get("LOGID") is None else str(row.get("LOGID")),
                    prepare_event_sql_datetime(row.get("EVENT_DATE_TIME")),
                    row.get("SOURCE_EVENT_INDEX") or 0,
                )
                cursor.execute(event_insert_sql, event_values)
                affected_rows += cursor.rowcount
                event_id = cursor.lastrowid

                parameters = [
                    (
                        event_id,
                        parameter.get("PARAMETER_INDEX"),
                        parameter.get("PARAMETER_CODE"),
                        None if parameter.get("VALUE") in ("", None) else str(parameter.get("VALUE")),
                        None if parameter.get("UNIT") in ("", None) else str(parameter.get("UNIT")),
                    )
                    for parameter in row.get("PARAMETERS", [])
                ]
                if parameters:
                    cursor.executemany(parameter_insert_sql, parameters)
                    affected_rows += cursor.rowcount
                    parameter_rows += len(parameters)

            connection.commit()
            return {
                "affected_rows": affected_rows,
                "event_rows": len(rows),
                "parameter_rows": parameter_rows,
            }
        finally:
            cursor.close()
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail=f"MySQL Insert Error: {str(exc)}")
    finally:
        connection.close()


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
    """Fetch load profile documents for a specific meter and date."""
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
            body={
                "size": 5000,
                "_source": source_fields,
                "query": query,
            },
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_load_profile_month_docs_from_es(meter_no: str, date: str):
    """Fetch load profile documents for a specific meter and month."""
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
            body={
                "size": 10000,
                "_source": source_fields,
                "query": query,
            },
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_day_profile_month_docs_from_es(meter_no: str, date: str):
    """Fetch day profile documents for a specific meter and month."""
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
            body={
                "size": 10000,
                "_source": source_fields,
                "query": query,
            },
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_event_docs_from_es(meter_no: str, date: str):
    """Fetch event documents for a specific meter and date."""
    target_date = parse_request_date(date)
    hyphen_date_pattern = f"{target_date.strftime('%d-%m-%Y')}*"
    slash_date_pattern = f"{target_date.strftime('%d/%m/%Y')}*"
    source_fields = [
        "meter_no",
        "event_index",
        "code",
        "status",
        "logid",
        "time",
        "timestamp",
        "parameters",
    ]
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
            body={
                "size": 5000,
                "_source": source_fields,
                "query": query,
            },
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def fetch_event_month_docs_from_es(meter_no: str, date: str):
    """Fetch event documents for a specific meter and month."""
    target_date = parse_request_date(date)
    hyphen_month_pattern = f"*-{target_date.strftime('%m-%Y')}*"
    slash_month_pattern = f"*/{target_date.strftime('%m/%Y')}*"
    source_fields = [
        "meter_no",
        "event_index",
        "code",
        "status",
        "logid",
        "time",
        "timestamp",
        "parameters",
    ]
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
            body={
                "size": 10000,
                "_source": source_fields,
                "query": query,
            },
        )
        return response.get("hits", {}).get("hits", [])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Elasticsearch Search Error: {str(exc)}")


def build_load_profile_export_rows(hits: list):
    """Flatten selected parameter codes into export-ready rows."""
    code_to_column = {
        code: csv_column
        for csv_column, _, code in LOAD_PROFILE_PARAMETER_MAPPINGS
    }
    rows = []

    for hit in hits:
        source = hit.get("_source", {})
        row = {
            "meter_no": source.get("meter_no", ""),
            "date": source.get("date", ""),
            "interval": source.get("interval", ""),
            "timestamp": source.get("timestamp", ""),
        }
        row.update(
            {
                csv_column: ""
                for csv_column, _, _ in LOAD_PROFILE_PARAMETER_MAPPINGS
            }
        )

        for parameter in source.get("parameters", []):
            code = parameter.get("code")
            column_name = code_to_column.get(code)
            if column_name:
                row[column_name] = parameter.get("value", "")

        rows.append(row)

    return sorted(
        rows,
        key=lambda item: (
            item["date"],
            int(item["interval"]) if str(item["interval"]).isdigit() else item["interval"],
        ),
    )


def build_load_profile_es_documents(flat_data: list):
    """Transform flat load profile rows into Elasticsearch documents."""
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
                new_doc["parameters"].append(
                    {
                        "code": key,
                        "value": value,
                        "unit": "",
                    }
                )

        transformed_data.append(new_doc)

    return transformed_data


def build_billing_es_documents(flat_data: list):
    """Transform flat billing rows into section-wise Elasticsearch documents."""
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

        parameter = {
            key: value
            for key, value in row.items()
            if key not in base_keys and value not in (None, "")
        }
        grouped_docs[doc_key]["parameters"].append(parameter)

    return list(grouped_docs.values())


def build_event_es_documents(event_data: list):
    """Attach stable document ids to parsed event records."""
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
    """Attach stable document ids to parsed D6 day profile records."""
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


def publish_directory_data_to_es(req: DirectoryRequest, extractor, index_name: str, transformer=None):
    """Read all CDF files in a directory, build records, and publish them to Elasticsearch."""
    all_data = []
    processed_files = 0
    skipped_files = 0
    files = parser.get_cdf_files(req.directory_path)

    for file in files:
        file_path = os.path.join(req.directory_path, file)
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
        "directory_path": req.directory_path,
        "processed_files": processed_files,
        "skipped_files": skipped_files,
        "total_records": len(all_data),
        "elasticsearch": es_result,
    }


@app.post("/api/elasticsearch/instantaneous")
def es_push_instantaneous(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_instantaneous(tree.getroot(), meter_no)
        return publish_to_es_helper(data, index_name="meter-instantaneous-data")
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/elasticsearch/dir/instantaneous")
def es_push_dir_instantaneous(req: DirectoryRequest):
    return publish_directory_data_to_es(
        req,
        extractor=parser.extract_instantaneous,
        index_name="meter-instantaneous-data",
    )


@app.post("/api/elasticsearch/load-profile")
def es_push_load_profile(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        flat_data = parser.extract_load_profile(tree.getroot(), meter_no)
        transformed_data = build_load_profile_es_documents(flat_data)
        return publish_to_es_helper(transformed_data, index_name=LOAD_PROFILE_INDEX)
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/elasticsearch/dir/load-profile")
def es_push_dir_load_profile(req: DirectoryRequest):
    return publish_directory_data_to_es(
        req,
        extractor=parser.extract_load_profile,
        index_name=LOAD_PROFILE_INDEX,
        transformer=build_load_profile_es_documents,
    )


@app.post("/api/elasticsearch/load-profile/export")
def export_load_profile_from_es(req: LoadProfileExportRequest):
    hits = fetch_load_profile_docs_from_es(req.meter_no, req.date)
    rows = build_load_profile_export_rows(hits)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No load profile data found for the given meter no and date.",
        )

    csv_headers = ["meter_no", "date", "interval", "timestamp"] + [
        csv_column for csv_column, _, _ in LOAD_PROFILE_PARAMETER_MAPPINGS
    ]
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


@app.post("/api/elasticsearch/load-profile/save-to-sql")
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


@app.post("/api/elasticsearch/load-profile/save-month-to-sql")
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


@app.post("/api/elasticsearch/billing")
def es_push_billing(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        flat_data = parser.extract_billing(tree.getroot(), meter_no)
        transformed_data = build_billing_es_documents(flat_data)
        return publish_to_es_helper(transformed_data, index_name="meter-billing-data")
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/elasticsearch/dir/billing")
def es_push_dir_billing(req: DirectoryRequest):
    return publish_directory_data_to_es(
        req,
        extractor=parser.extract_billing,
        index_name="meter-billing-data",
        transformer=build_billing_es_documents,
    )


@app.post("/api/elasticsearch/event")
def es_push_event(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        event_data = parser.extract_events(tree.getroot(), meter_no)
        transformed_data = build_event_es_documents(event_data)
        return publish_to_es_helper(transformed_data, index_name=EVENT_INDEX)
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/elasticsearch/dir/event")
def es_push_dir_event(req: DirectoryRequest):
    return publish_directory_data_to_es(
        req,
        extractor=parser.extract_events,
        index_name=EVENT_INDEX,
        transformer=build_event_es_documents,
    )


@app.post("/api/elasticsearch/event/save-to-sql")
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


@app.post("/api/elasticsearch/event/save-month-to-sql")
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


@app.post("/api/elasticsearch/day-profile")
def es_push_day_profile(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        day_profile_data = parser.extract_day_profile(tree.getroot(), meter_no)
        transformed_data = build_day_profile_es_documents(day_profile_data)
        return publish_to_es_helper(transformed_data, index_name=DAY_PROFILE_INDEX)
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/elasticsearch/dir/day-profile")
def es_push_dir_day_profile(req: DirectoryRequest):
    return publish_directory_data_to_es(
        req, 
        extractor=parser.extract_day_profile,
        index_name=DAY_PROFILE_INDEX,
        transformer=build_day_profile_es_documents,
    )


@app.post("/api/elasticsearch/day-profile/save-month-to-sql")
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
