import re
from datetime import datetime

from fastapi import HTTPException

from .. import parser
from ..config import (
    DB_BILLING_TABLE,
    DB_DAY_PROFILE_TABLE,
    DB_EVENT_PARAMETER_TABLE,
    DB_EVENT_TABLE,
    DB_HOST,
    DB_LOAD_PROFILE_TABLE,
    DB_NAME,
    DB_PARAMETER_MAPPING_TABLE,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    EVENT_SQL_UNIQUE_KEY_COLUMNS,
)
from ..logging_utils import get_logger, log_exception


logger = get_logger(__name__)


DEFAULT_PARAMETER_MAPPINGS = (
    (DB_LOAD_PROFILE_TABLE, "P2-1-1-4-0", "CURRENT_R"),
    (DB_LOAD_PROFILE_TABLE, "P2-1-2-4-0", "CURRENT_Y"),
    (DB_LOAD_PROFILE_TABLE, "P2-1-3-4-0", "CURRENT_B"),
    (DB_LOAD_PROFILE_TABLE, "P1-2-1-4-0", "VOLTAGE_R"),
    (DB_LOAD_PROFILE_TABLE, "P1-2-2-4-0", "VOLTAGE_Y"),
    (DB_LOAD_PROFILE_TABLE, "P1-2-3-4-0", "VOLTAGE_B"),
    (DB_LOAD_PROFILE_TABLE, "P7-1-18-1-0", "KWH"),
    (DB_LOAD_PROFILE_TABLE, "P7-1-18-2-0", "KWH"),
    (DB_LOAD_PROFILE_TABLE, "P7-3-18-2-0", "KVAH_IMPORT"),
    (DB_LOAD_PROFILE_TABLE, "P7-2-1-0-0", "KVARH_LAG"),
    (DB_LOAD_PROFILE_TABLE, "P7-2-2-0-0", "KVARH_LEAD"),
    (DB_LOAD_PROFILE_TABLE, "P7-2-19-0-0", "KVARH_REACTIVE_LAG"),
    (DB_LOAD_PROFILE_TABLE, "P7-2-20-0-0", "KVARH_REACTIVE_LEAD"),
    (DB_DAY_PROFILE_TABLE, "P7-1-18-2-0", "KWH_FORWARDED_TOTAL"),
    (DB_DAY_PROFILE_TABLE, "P7-1-18-1-0", "KWH_FORWARDED_FUND"),
    (DB_DAY_PROFILE_TABLE, "P7-1-5-1-0", "KWH_IMPORT_FUND"),
    (DB_DAY_PROFILE_TABLE, "P7-1-6-2-0", "KWH_EXPORT_TOTAL"),
    (DB_DAY_PROFILE_TABLE, "P7-1-6-1-0", "KWH_EXPORT_FUND"),
    (DB_DAY_PROFILE_TABLE, "P7-3-18-2-0", "KVAH_FORWARDED_TOTAL"),
    (DB_DAY_PROFILE_TABLE, "P7-3-5-2-0", "KVAH_IMPORT_TOTAL"),
    (DB_DAY_PROFILE_TABLE, "P7-3-6-0-0", "KVAH_EXPORT_TOTAL"),
    (DB_DAY_PROFILE_TABLE, "P7-2-1-0-0", "KVARH_Q1_LAG"),
    (DB_DAY_PROFILE_TABLE, "P7-2-2-0-0", "KVARH_Q2_LEAD"),
    (DB_DAY_PROFILE_TABLE, "P7-2-3-0-0", "KVARH_Q3"),
    (DB_DAY_PROFILE_TABLE, "P7-2-4-0-0", "KVARH_Q4"),
    (DB_DAY_PROFILE_TABLE, "P1202-1-5-2-0", "KWH_IMPORT_TOTAL_SNAPSHOT"),
    (DB_DAY_PROFILE_TABLE, "P1203-1-14-1-0", "KWH_FORWARDED_FUND_SNAPSHOT"),
    (DB_DAY_PROFILE_TABLE, "P1202-2-19-0-0", "KVARH_Q1_SNAPSHOT"),
    (DB_DAY_PROFILE_TABLE, "P1202-2-20-0-0", "KVARH_Q4_SNAPSHOT"),
)


PARAMETER_COLUMN_PRIORITIES = {
    (DB_LOAD_PROFILE_TABLE, "KWH"): {
        "P7-1-18-1-0": 1,
        "P7-1-18-2-0": 2,
    },
}

PROFILE_COLUMN_ALIASES = {
    DB_LOAD_PROFILE_TABLE: {
        "KWH": ("KWH_IMPORT",),
        "KVAH_IMPORT": ("KVAH", "POWER"),
        "KVARH_REACTIVE_LAG": ("P7_2_19_0_0",),
        "KVARH_REACTIVE_LEAD": ("P7_2_20_0_0",),
    },
}


def quote_mysql_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def get_mysql_connection():
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
        log_exception(logger, "MySQL connection failed", exc, database=DB_NAME, host=DB_HOST, port=DB_PORT)
        raise HTTPException(status_code=500, detail=f"MySQL Connection Error: {str(exc)}")


def ensure_parameter_mapping_table(connection):
    create_mapping_sql = (
        f"CREATE TABLE IF NOT EXISTS {quote_mysql_identifier(DB_PARAMETER_MAPPING_TABLE)} ("
        f"{quote_mysql_identifier('TABLE_NAME')} VARCHAR(128) NOT NULL, "
        f"{quote_mysql_identifier('PARAMETER_CODE')} VARCHAR(128) NOT NULL, "
        f"{quote_mysql_identifier('COLUMN_NAME')} VARCHAR(128) NOT NULL, "
        f"PRIMARY KEY ({quote_mysql_identifier('TABLE_NAME')}, {quote_mysql_identifier('PARAMETER_CODE')})"
        f")"
    )
    cursor = connection.cursor()
    try:
        cursor.execute(create_mapping_sql)
        connection.commit()
    finally:
        cursor.close()


def seed_default_parameter_mappings(connection):
    ensure_parameter_mapping_table(connection)
    cursor = connection.cursor()
    try:
        cursor.executemany(
            f"INSERT INTO {quote_mysql_identifier(DB_PARAMETER_MAPPING_TABLE)} "
            f"({quote_mysql_identifier('TABLE_NAME')}, {quote_mysql_identifier('PARAMETER_CODE')}, {quote_mysql_identifier('COLUMN_NAME')}) "
            f"VALUES (%s, %s, %s) "
            f"ON DUPLICATE KEY UPDATE {quote_mysql_identifier('COLUMN_NAME')} = VALUES({quote_mysql_identifier('COLUMN_NAME')})",
            DEFAULT_PARAMETER_MAPPINGS,
        )
        connection.commit()
    finally:
        cursor.close()


def normalize_parameter_code_to_column_name(parameter_code: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", str(parameter_code or "").strip())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = "PARAMETER"
    if not (normalized[0].isalpha() or normalized[0] == "_"):
        normalized = f"P_{normalized}"
    return normalized.upper()


def _collect_parameter_codes_from_hits(hits: list) -> list:
    parameter_codes = set()
    for hit in hits:
        source = hit.get("_source", {})
        for parameter in source.get("parameters", []):
            code = str(parameter.get("code", "")).strip()
            if code:
                parameter_codes.add(code)
    return sorted(parameter_codes)


def ensure_parameter_mappings_for_table(connection, table_name: str, parameter_codes: list):
    seed_default_parameter_mappings(connection)
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"SELECT {quote_mysql_identifier('PARAMETER_CODE')}, {quote_mysql_identifier('COLUMN_NAME')} "
            f"FROM {quote_mysql_identifier(DB_PARAMETER_MAPPING_TABLE)} "
            f"WHERE {quote_mysql_identifier('TABLE_NAME')} = %s",
            (table_name,),
        )
        rows = cursor.fetchall()
        code_to_column = {}
        for parameter_code, column_name in rows:
            code = str(parameter_code or "").strip()
            column = str(column_name or "").strip()
            if code and column:
                code_to_column[code] = column.upper()

        used_column_names = {column.upper() for column in code_to_column.values()}
        missing_codes = [code for code in parameter_codes if code not in code_to_column]
        new_rows = []
        for code in missing_codes:
            base_column_name = normalize_parameter_code_to_column_name(code)
            candidate = base_column_name
            suffix = 2
            while candidate.upper() in used_column_names:
                candidate = f"{base_column_name}_{suffix}"
                suffix += 1
            code_to_column[code] = candidate
            used_column_names.add(candidate.upper())
            new_rows.append((table_name, code, candidate))

        if new_rows:
            cursor.executemany(
                f"INSERT INTO {quote_mysql_identifier(DB_PARAMETER_MAPPING_TABLE)} "
                f"({quote_mysql_identifier('TABLE_NAME')}, {quote_mysql_identifier('PARAMETER_CODE')}, {quote_mysql_identifier('COLUMN_NAME')}) "
                f"VALUES (%s, %s, %s)",
                new_rows,
            )
            connection.commit()

        return code_to_column
    finally:
        cursor.close()


def get_parameter_mappings_for_table(table_name: str, parameter_codes: list = None):
    connection = get_mysql_connection()
    try:
        return ensure_parameter_mappings_for_table(connection, table_name, parameter_codes or [])
    finally:
        connection.close()


def get_parameter_column_priority(table_name: str, column_name: str, parameter_code: str) -> int:
    column_priorities = PARAMETER_COLUMN_PRIORITIES.get((table_name, str(column_name or "").upper()), {})
    return column_priorities.get(parameter_code, 100)


def merge_profile_column_aliases(connection, target_table: str, existing_columns: set[str]):
    alias_map = PROFILE_COLUMN_ALIASES.get(target_table, {})
    if not alias_map:
        return

    cursor = connection.cursor()
    try:
        for target_column, alias_columns in alias_map.items():
            if target_column.upper() not in existing_columns:
                continue
            for alias_column in alias_columns:
                if alias_column.upper() not in existing_columns:
                    continue
                cursor.execute(
                    f"UPDATE {quote_mysql_identifier(target_table)} "
                    f"SET {quote_mysql_identifier(target_column)} = {quote_mysql_identifier(alias_column)} "
                    f"WHERE {quote_mysql_identifier(target_column)} IS NULL "
                    f"AND {quote_mysql_identifier(alias_column)} IS NOT NULL"
                )
        connection.commit()
    finally:
        cursor.close()


def ensure_profile_sql_table_with_columns(connection, target_table: str, parameter_columns: list):
    fixed_columns = [("METER_NO", "VARCHAR(64) NOT NULL"), ("DATETIME_TIMESTAMP", "DATETIME NOT NULL")]
    fixed_column_defs = [
        f"{quote_mysql_identifier(column_name)} {column_type}"
        for column_name, column_type in fixed_columns
    ]
    primary_key = ", ".join(quote_mysql_identifier(column_name) for column_name in ("METER_NO", "DATETIME_TIMESTAMP"))
    create_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {quote_mysql_identifier(target_table)} "
        f"({', '.join(fixed_column_defs)}, PRIMARY KEY ({primary_key}))"
    )

    cursor = connection.cursor()
    try:
        cursor.execute(create_table_sql)
        cursor.execute(f"SHOW COLUMNS FROM {quote_mysql_identifier(target_table)}")
        existing_columns = {str(column[0]).upper() for column in cursor.fetchall()}
        for column_name in parameter_columns:
            if column_name.upper() not in existing_columns:
                cursor.execute(
                    f"ALTER TABLE {quote_mysql_identifier(target_table)} "
                    f"ADD COLUMN {quote_mysql_identifier(column_name)} DECIMAL(18,6) NULL"
                )
                existing_columns.add(column_name.upper())
        connection.commit()
    finally:
        cursor.close()

    merge_profile_column_aliases(connection, target_table, existing_columns)


def build_profile_sql_rows(hits: list, target_table: str):
    parameter_codes = _collect_parameter_codes_from_hits(hits)
    code_to_sql_column = get_parameter_mappings_for_table(target_table, parameter_codes)
    parameter_columns = sorted({column_name for column_name in code_to_sql_column.values()})
    rows = []

    for hit in hits:
        source = hit.get("_source", {})
        row = {
            "METER_NO": source.get("meter_no", ""),
            "DATETIME_TIMESTAMP": source.get("timestamp", ""),
        }
        row.update({sql_column: None for sql_column in parameter_columns})

        for parameter in source.get("parameters", []):
            code = parameter.get("code")
            sql_column = code_to_sql_column.get(code)
            if sql_column:
                selected_priorities = row.setdefault("_PARAMETER_PRIORITIES", {})
                new_priority = get_parameter_column_priority(target_table, sql_column, code)
                current_priority = selected_priorities.get(sql_column, 100)
                if row.get(sql_column) not in ("", None) and new_priority >= current_priority:
                    continue
                row[sql_column] = parameter.get("value", "")
                selected_priorities[sql_column] = new_priority

        row.pop("_PARAMETER_PRIORITIES", None)

        rows.append(row)

    return sorted(rows, key=lambda item: item["DATETIME_TIMESTAMP"]), parameter_columns


def build_load_profile_sql_rows(hits: list):
    rows, _ = build_profile_sql_rows(hits, DB_LOAD_PROFILE_TABLE)
    return rows


def get_load_profile_sql_headers(rows: list):
    dynamic_headers = sorted({key for row in rows for key in row.keys() if key not in {"METER_NO", "DATETIME_TIMESTAMP"}})
    return ["METER_NO", "DATETIME_TIMESTAMP"] + dynamic_headers


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


def save_load_profile_rows_to_sql(rows: list):
    connection = get_mysql_connection()
    headers = get_load_profile_sql_headers(rows)
    quoted_columns = ", ".join(quote_mysql_identifier(header) for header in headers)
    placeholders = ", ".join(["%s"] * len(headers))
    update_clause = ", ".join(
        f"{quote_mysql_identifier(header)} = VALUES({quote_mysql_identifier(header)})"
        for header in headers
        if header not in {"METER_NO", "DATETIME_TIMESTAMP"}
    )
    if not update_clause:
        update_clause = f"{quote_mysql_identifier('METER_NO')} = VALUES({quote_mysql_identifier('METER_NO')})"
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
        ensure_profile_sql_table_with_columns(
            connection,
            DB_LOAD_PROFILE_TABLE,
            [header for header in headers if header not in {"METER_NO", "DATETIME_TIMESTAMP"}],
        )
        cursor = connection.cursor()
        try:
            cursor.executemany(insert_sql, values)
            connection.commit()
            return cursor.rowcount
        finally:
            cursor.close()
    except Exception as exc:
        log_exception(logger, "MySQL load profile insert failed", exc, table=DB_LOAD_PROFILE_TABLE, row_count=len(rows))
        raise HTTPException(status_code=500, detail=f"MySQL Insert Error: {str(exc)}")
    finally:
        connection.close()


def get_day_profile_sql_headers(rows: list):
    dynamic_headers = sorted({key for row in rows for key in row.keys() if key not in {"METER_NO", "DATETIME_TIMESTAMP"}})
    return ["METER_NO", "DATETIME_TIMESTAMP"] + dynamic_headers


def prepare_day_profile_sql_value(column_name: str, value):
    return prepare_load_profile_sql_value(column_name, value)


def build_day_profile_sql_rows(hits: list):
    rows, _ = build_profile_sql_rows(hits, DB_DAY_PROFILE_TABLE)
    return rows


def save_day_profile_rows_to_sql(rows: list):
    connection = get_mysql_connection()
    headers = get_day_profile_sql_headers(rows)
    quoted_columns = ", ".join(quote_mysql_identifier(header) for header in headers)
    placeholders = ", ".join(["%s"] * len(headers))
    update_clause = ", ".join(
        f"{quote_mysql_identifier(header)} = VALUES({quote_mysql_identifier(header)})"
        for header in headers
        if header not in {"METER_NO", "DATETIME_TIMESTAMP"}
    )
    if not update_clause:
        update_clause = f"{quote_mysql_identifier('METER_NO')} = VALUES({quote_mysql_identifier('METER_NO')})"
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
        ensure_profile_sql_table_with_columns(
            connection,
            DB_DAY_PROFILE_TABLE,
            [header for header in headers if header not in {"METER_NO", "DATETIME_TIMESTAMP"}],
        )
        cursor = connection.cursor()
        try:
            cursor.executemany(insert_sql, values)
            connection.commit()
            return cursor.rowcount
        finally:
            cursor.close()
    except Exception as exc:
        log_exception(logger, "MySQL day profile insert failed", exc, table=DB_DAY_PROFILE_TABLE, row_count=len(rows))
        raise HTTPException(status_code=500, detail=f"MySQL Insert Error: {str(exc)}")
    finally:
        connection.close()


def _collect_billing_parameter_codes_from_hits(hits: list) -> list:
    parameter_codes = set()
    for hit in hits:
        source = hit.get("_source", {})
        for parameter in source.get("parameters", []):
            code = str(parameter.get("code", "")).strip()
            if code:
                parameter_codes.add(code)
    return sorted(parameter_codes)


def build_billing_sql_rows(hits: list):
    parameter_codes = _collect_billing_parameter_codes_from_hits(hits)
    code_to_sql_column = get_parameter_mappings_for_table(DB_BILLING_TABLE, parameter_codes)
    dynamic_columns = sorted({column_name for column_name in code_to_sql_column.values()})
    rows = []

    for hit in hits:
        source = hit.get("_source", {})
        fallback_from_parameters = {}
        for parameter in source.get("parameters", []):
            if not isinstance(parameter, dict):
                continue
            for key in (
                "b11",
                "b12",
                "b13",
                "power_on_duration",
                "power_off_duration",
                "cumulative_tamper_count",
            ):
                if key not in fallback_from_parameters and parameter.get(key) not in ("", None):
                    fallback_from_parameters[key] = parameter.get(key)

        row = {
            "METER_NO": source.get("meter_no", ""),
            "SECTION": source.get("section", ""),
            "BILLING_DATE_TIME": source.get("timestamp", "") or parser.format_datetime_to_iso(source.get("date_time", "")),
            "RESET_METHOD": source.get("reset_method", ""),
            "POWER_ON_DURATION": (
                source.get("power_on_duration", "")
                or source.get("b11", "")
                or fallback_from_parameters.get("power_on_duration", "")
                or fallback_from_parameters.get("b11", "")
            ),
            "POWER_OFF_DURATION": (
                source.get("power_off_duration", "")
                or source.get("b12", "")
                or fallback_from_parameters.get("power_off_duration", "")
                or fallback_from_parameters.get("b12", "")
            ),
            "CUMULATIVE_TAMPER_COUNT": (
                source.get("cumulative_tamper_count", "")
                or source.get("b13", "")
                or fallback_from_parameters.get("cumulative_tamper_count", "")
                or fallback_from_parameters.get("b13", "")
            ),
        }
        row.update({column_name: None for column_name in dynamic_columns})

        for parameter in source.get("parameters", []):
            code = parameter.get("code")
            sql_column = code_to_sql_column.get(code)
            if sql_column:
                row[sql_column] = parameter.get("value", "")

        rows.append(row)

    return sorted(rows, key=lambda item: (item["BILLING_DATE_TIME"], item["SECTION"]))


def get_billing_sql_headers(rows: list):
    fixed_headers = ["METER_NO", "SECTION", "BILLING_DATE_TIME", "RESET_METHOD"]
    dynamic_headers = sorted({key for row in rows for key in row.keys() if key not in fixed_headers})
    return fixed_headers + dynamic_headers


def prepare_billing_sql_value(column_name: str, value):
    if column_name == "BILLING_DATE_TIME":
        return prepare_load_profile_sql_value("DATETIME_TIMESTAMP", value)
    if value in ("", None):
        return None
    return value


def ensure_billing_sql_table_with_columns(connection, dynamic_columns: list):
    fixed_columns = [
        ("METER_NO", "VARCHAR(64) NOT NULL"),
        ("SECTION", "VARCHAR(64) NOT NULL"),
        ("BILLING_DATE_TIME", "DATETIME NOT NULL"),
        ("RESET_METHOD", "VARCHAR(128) NULL"),
    ]
    fixed_column_defs = [
        f"{quote_mysql_identifier(column_name)} {column_type}"
        for column_name, column_type in fixed_columns
    ]
    primary_key = ", ".join(
        quote_mysql_identifier(column_name) for column_name in ("METER_NO", "SECTION", "BILLING_DATE_TIME")
    )
    create_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {quote_mysql_identifier(DB_BILLING_TABLE)} "
        f"({', '.join(fixed_column_defs)}, PRIMARY KEY ({primary_key}))"
    )

    cursor = connection.cursor()
    try:
        cursor.execute(create_table_sql)
        cursor.execute(f"SHOW COLUMNS FROM {quote_mysql_identifier(DB_BILLING_TABLE)}")
        existing_columns = {str(column[0]).upper() for column in cursor.fetchall()}
        for column_name in dynamic_columns:
            if column_name.upper() not in existing_columns:
                is_text_column = column_name.upper().endswith("_NAME")
                column_type = "VARCHAR(255) NULL" if is_text_column else "DECIMAL(18,6) NULL"
                cursor.execute(
                    f"ALTER TABLE {quote_mysql_identifier(DB_BILLING_TABLE)} "
                    f"ADD COLUMN {quote_mysql_identifier(column_name)} {column_type}"
                )
        connection.commit()
    finally:
        cursor.close()


def save_billing_rows_to_sql(rows: list):
    connection = get_mysql_connection()
    headers = get_billing_sql_headers(rows)
    quoted_columns = ", ".join(quote_mysql_identifier(header) for header in headers)
    placeholders = ", ".join(["%s"] * len(headers))
    update_clause = ", ".join(
        f"{quote_mysql_identifier(header)} = VALUES({quote_mysql_identifier(header)})"
        for header in headers
        if header not in {"METER_NO", "SECTION", "BILLING_DATE_TIME"}
    )
    if not update_clause:
        update_clause = f"{quote_mysql_identifier('METER_NO')} = VALUES({quote_mysql_identifier('METER_NO')})"
    insert_sql = (
        f"INSERT INTO {quote_mysql_identifier(DB_BILLING_TABLE)} ({quoted_columns}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )
    values = [
        tuple(prepare_billing_sql_value(header, row.get(header)) for header in headers)
        for row in rows
    ]

    try:
        ensure_billing_sql_table_with_columns(
            connection,
            [header for header in headers if header not in {"METER_NO", "SECTION", "BILLING_DATE_TIME", "RESET_METHOD"}],
        )
        cursor = connection.cursor()
        try:
            cursor.executemany(insert_sql, values)
            connection.commit()
            return cursor.rowcount
        finally:
            cursor.close()
    except Exception as exc:
        log_exception(logger, "MySQL billing insert failed", exc, table=DB_BILLING_TABLE, row_count=len(rows))
        raise HTTPException(status_code=500, detail=f"MySQL Insert Error: {str(exc)}")
    finally:
        connection.close()


def ensure_event_sql_tables(connection):
    event_table = quote_mysql_identifier(DB_EVENT_TABLE)
    parameter_table = quote_mysql_identifier(DB_EVENT_PARAMETER_TABLE)
    unique_key = ", ".join(quote_mysql_identifier(column_name) for column_name in EVENT_SQL_UNIQUE_KEY_COLUMNS)
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
        row["PARAMETERS"] = sorted(row["PARAMETERS"], key=lambda parameter: parameter["PARAMETER_INDEX"])
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
        log_exception(
            logger,
            "MySQL event insert failed",
            exc,
            event_table=DB_EVENT_TABLE,
            event_parameter_table=DB_EVENT_PARAMETER_TABLE,
            row_count=len(rows),
        )
        raise HTTPException(status_code=500, detail=f"MySQL Insert Error: {str(exc)}")
    finally:
        connection.close()
