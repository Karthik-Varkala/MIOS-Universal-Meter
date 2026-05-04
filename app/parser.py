import csv
import os
from datetime import datetime, timedelta

from fastapi import HTTPException


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
            return g1.text
    return "Unknown"


def extract_instantaneous(root, meter_no):
    data = []
    d2 = root.find(".//D2")
    if d2 is not None:
        for param in d2.findall("INSTPARAM"):
            data.append(
                {
                    "meter_no": meter_no,
                    "code": param.get("CODE"),
                    "value": param.get("VALUE"),
                    "unit": param.get("UNIT"),
                }
            )
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

    for datetime_format in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M"):
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


def extract_load_profile(root, meter_no):
    data = []
    d4 = root.find(".//D4")
    interval_period = get_load_profile_interval_period(root)
    if d4 is not None:
        for day_profile in d4.findall("DAYPROFILE"):
            date = day_profile.get("DATE")
            for ip in day_profile.findall("IP"):
                interval = ip.get("INTERVAL")
                row = {
                    "meter_no": meter_no,
                    "date": date,
                    "interval": interval,
                    "timestamp": get_timestamp(date, interval_period, interval),
                }
                for param in ip.findall("PARAMETER"):
                    row[param.get("PARAMCODE")] = param.get("VALUE")
                data.append(row)
    return data


def extract_billing(root, meter_no):
    data = []
    d3 = root.find(".//D3")
    if d3 is not None:
        for sub in d3:
            dt = sub.get("DATETIME")
            reset_method = sub.get("MECHANISM", "")
            if not reset_method:
                b2 = sub.find("B2")
                if b2 is not None:
                    reset_method = b2.get("MECHANISM", "")

            for b_tag in sub:
                if b_tag.tag == "B2":
                    continue

                row = {
                    "meter_no": meter_no,
                    "section": sub.tag,
                    "date_time": dt,
                    "timestamp": format_datetime_to_iso(dt),
                    "reset_method": reset_method,
                }
                row.update(normalize_billing_tag_data(b_tag))
                data.append(row)
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
