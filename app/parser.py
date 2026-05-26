import csv
import os
from datetime import datetime, timedelta

from fastapi import HTTPException

from .logging_utils import append_invalid_record
from .validation import ensure_records_present, require_xml_attribute, require_xml_section


def get_cdf_files(directory_path: str):
    if not os.path.isdir(directory_path):
        raise HTTPException(status_code=400, detail=f"Directory not found: {directory_path}")
    files = [f for f in os.listdir(directory_path) if f.lower().endswith(".cdf")]
    if not files:
        raise HTTPException(status_code=404, detail="No .cdf files found.")
    return files


def get_meter_no(root):
    d1 = root.find(".//D1")
    if d1 is not None:
        g1 = d1.find("G1")
        if g1 is not None and g1.text:
            return str(g1.text).strip()
    return "Unknown"


def _raise_data_validation_error(message: str, operation: str, file_path: str = "", **context):
    append_invalid_record(message, operation=operation, file_path=file_path, **context)
    raise HTTPException(status_code=400, detail=message)


def extract_instantaneous(root, meter_no, file_path: str = ""):
    data = []
    d2 = require_xml_section(root, "D2", operation="extract_instantaneous", file_path=file_path)
    for index, param in enumerate(d2.findall("INSTPARAM"), start=1):
        code = require_xml_attribute(
            param,
            "CODE",
            operation="extract_instantaneous",
            file_path=file_path,
            element_name=f"INSTPARAM[{index}]",
        )
        data.append(
            {
                "meter_no": meter_no,
                "code": code,
                "value": param.get("VALUE", ""),
                "unit": param.get("UNIT", ""),
            }
        )
    ensure_records_present(data, operation="extract_instantaneous", record_type="instantaneous", file_path=file_path)
    return data


def get_load_profile_interval_period(root):
    d4 = root.find(".//D4")
    if d4 is None:
        return None

    interval_period = d4.get("INTERVALPERIOD")
    if not interval_period:
        return None

    try:
        return int(interval_period)
    except (TypeError, ValueError):
        return None


def get_timestamp(date_str, interval_minutes, slot_id):
    if not date_str or interval_minutes in (None, "") or slot_id in (None, ""):
        return ""

    start_of_day = None
    for date_format in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            start_of_day = datetime.strptime(date_str, date_format)
            break
        except ValueError:
            continue

    if start_of_day is None:
        return ""

    try:
        target_time = start_of_day + timedelta(minutes=int(slot_id) * int(interval_minutes))
    except (TypeError, ValueError):
        return ""

    return target_time.strftime("%Y-%m-%dT%H:%M:%S")


def format_datetime_to_iso(datetime_str):
    if not datetime_str:
        return ""

    for datetime_format in (
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S:%f",
        "%d/%m/%Y %H:%M:%S:%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            parsed_datetime = datetime.strptime(datetime_str, datetime_format)
            return parsed_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue

    return ""


def normalize_billing_tag_data(b_tag):
    normalized_data = {
        "tag": b_tag.tag.lower(),
        "code": "",
        "value": "",
        "unit": "",
        "tod": "",
        "occdate": "",
        "mechanism_code": "",
    }

    attribute_map = {
        "PARAMCODE": "code",
        "CODE": "code",
        "VALUE": "value",
        "UNIT": "unit",
        "TOD": "tod",
        "OCCDATE": "occdate",
        "MECHANISMCODE": "mechanism_code",
    }

    for attribute_name, attribute_value in b_tag.attrib.items():
        normalized_name = attribute_map.get(attribute_name, attribute_name.lower())
        normalized_data[normalized_name] = attribute_value

    return normalized_data


def extract_load_profile(root, meter_no, file_path: str = ""):
    data = []
    d4 = require_xml_section(root, "D4", operation="extract_load_profile", file_path=file_path)
    interval_period = get_load_profile_interval_period(root)
    if interval_period is None:
        _raise_data_validation_error(
            "Missing or invalid D4.INTERVALPERIOD value.",
            operation="extract_load_profile",
            file_path=file_path,
            section="D4",
        )

    for day_index, day_profile in enumerate(d4.findall("DAYPROFILE"), start=1):
        date = require_xml_attribute(
            day_profile,
            "DATE",
            operation="extract_load_profile",
            file_path=file_path,
            element_name=f"DAYPROFILE[{day_index}]",
        )
        for interval_index, ip in enumerate(day_profile.findall("IP"), start=1):
            interval = require_xml_attribute(
                ip,
                "INTERVAL",
                operation="extract_load_profile",
                file_path=file_path,
                element_name=f"IP[{interval_index}]",
            )
            timestamp = get_timestamp(date, interval_period, interval)
            if not timestamp:
                _raise_data_validation_error(
                    "Unable to build timestamp from DATE, INTERVALPERIOD, and INTERVAL.",
                    operation="extract_load_profile",
                    file_path=file_path,
                    date=date,
                    interval=interval,
                    interval_period=interval_period,
                )

            row = {
                "meter_no": meter_no,
                "date": date,
                "interval": interval,
                "timestamp": timestamp,
            }
            for parameter_index, param in enumerate(ip.findall("PARAMETER"), start=1):
                parameter_code = require_xml_attribute(
                    param,
                    "PARAMCODE",
                    operation="extract_load_profile",
                    file_path=file_path,
                    element_name=f"PARAMETER[{parameter_index}]",
                )
                row[parameter_code] = param.get("VALUE", "")
            data.append(row)
    ensure_records_present(data, operation="extract_load_profile", record_type="load profile", file_path=file_path)
    return data


def extract_billing(root, meter_no, file_path: str = ""):
    data = []
    d3 = require_xml_section(root, "D3", operation="extract_billing", file_path=file_path)
    for section_index, sub in enumerate(d3, start=1):
        dt = require_xml_attribute(
            sub,
            "DATETIME",
            operation="extract_billing",
            file_path=file_path,
            element_name=f"{sub.tag}[{section_index}]",
        )
        timestamp = format_datetime_to_iso(dt)
        if not timestamp:
            _raise_data_validation_error(
                "Invalid billing DATETIME format.",
                operation="extract_billing",
                file_path=file_path,
                billing_datetime=dt,
            )

        reset_method = sub.get("MECHANISM", "")
        billing_lookup = {}

        if not reset_method:
            b2 = sub.find("B2")
            if b2 is not None:
                reset_method = b2.get("MECHANISM", "")

        for child in sub:
            child_tag = child.tag.upper()
            billing_lookup[child_tag] = child.get("VALUE", "")

        top_level_fields = {
            "power_on_duration": billing_lookup.get("B11", ""),
            "power_off_duration": billing_lookup.get("B12", ""),
            "cumulative_tamper_count": billing_lookup.get("B13", ""),
        }

        for billing_index, b_tag in enumerate(sub, start=1):
            if b_tag.tag not in ["B2", "B5"]:
                continue

            if not (b_tag.get("PARAMCODE") or b_tag.get("CODE")):
                _raise_data_validation_error(
                    "Missing billing parameter code in B2/B5 tag.",
                    operation="extract_billing",
                    file_path=file_path,
                    section=sub.tag,
                    element=f"{b_tag.tag}[{billing_index}]",
                )

            row = {
                "meter_no": meter_no,
                "section": sub.tag,
                "date_time": dt,
                "timestamp": timestamp,
                "reset_method": reset_method,
            }
            row.update(top_level_fields)
            row.update(normalize_billing_tag_data(b_tag))
            data.append(row)

    ensure_records_present(data, operation="extract_billing", record_type="billing", file_path=file_path)
    return data


def extract_events(root, meter_no, file_path: str = ""):
    data = []
    d5 = require_xml_section(root, "D5", operation="extract_events", file_path=file_path)

    for event_index, event in enumerate(d5.findall("EVENT"), start=1):
        event_time = require_xml_attribute(
            event,
            "TIME",
            operation="extract_events",
            file_path=file_path,
            element_name=f"EVENT[{event_index}]",
        )
        timestamp = format_datetime_to_iso(event_time)
        if not timestamp:
            _raise_data_validation_error(
                "Invalid event TIME format.",
                operation="extract_events",
                file_path=file_path,
                event_time=event_time,
                event_index=event_index,
            )
        row = {
            "meter_no": meter_no,
            "event_index": event_index,
            "code": require_xml_attribute(
                event,
                "CODE",
                operation="extract_events",
                file_path=file_path,
                element_name=f"EVENT[{event_index}]",
            ),
            "status": event.get("STATUS", ""),
            "logid": require_xml_attribute(
                event,
                "LOGID",
                operation="extract_events",
                file_path=file_path,
                element_name=f"EVENT[{event_index}]",
            ),
            "time": event_time,
            "timestamp": timestamp,
            "parameters": [],
        }

        for parameter_index, snapshot in enumerate(event.findall("SNAPSHOT"), start=1):
            row["parameters"].append(
                {
                    "parameter_index": parameter_index,
                    "code": require_xml_attribute(
                        snapshot,
                        "PARAMCODE",
                        operation="extract_events",
                        file_path=file_path,
                        element_name=f"SNAPSHOT[{parameter_index}]",
                    ),
                    "value": snapshot.get("VALUE", ""),
                    "unit": snapshot.get("UNIT", ""),
                }
            )

        data.append(row)

    ensure_records_present(data, operation="extract_events", record_type="event", file_path=file_path)
    return data


def extract_day_profile(root, meter_no, file_path: str = ""):
    data = []
    d6 = require_xml_section(root, "D6", operation="extract_day_profile", file_path=file_path)

    for snapshot_index, snapshot in enumerate(d6.findall("SNAPSHOT"), start=1):
        snapshot_datetime = require_xml_attribute(
            snapshot,
            "DATETIME",
            operation="extract_day_profile",
            file_path=file_path,
            element_name=f"SNAPSHOT[{snapshot_index}]",
        )
        timestamp = format_datetime_to_iso(snapshot_datetime)
        if not timestamp:
            _raise_data_validation_error(
                "Invalid day profile DATETIME format.",
                operation="extract_day_profile",
                file_path=file_path,
                snapshot_datetime=snapshot_datetime,
                snapshot_index=snapshot_index,
            )
        row = {
            "meter_no": meter_no,
            "datetime": snapshot_datetime,
            "timestamp": timestamp,
            "parameters": [],
        }

        for register_index, register in enumerate(snapshot.findall("REGISTER"), start=1):
            row["parameters"].append(
                {
                    "code": require_xml_attribute(
                        register,
                        "PARAMCODE",
                        operation="extract_day_profile",
                        file_path=file_path,
                        element_name=f"REGISTER[{register_index}]",
                    ),
                    "value": register.get("VALUE", ""),
                    "unit": register.get("UNIT", ""),
                }
            )

        data.append(row)

    ensure_records_present(data, operation="extract_day_profile", record_type="day profile", file_path=file_path)
    return data


def save_csv(data, file_path, headers, is_dict_writer=False):
    if not data:
        return

    with open(file_path, "w", newline="") as f:
        if is_dict_writer:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        else:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in data:
                writer.writerow([row[h] for h in headers])
