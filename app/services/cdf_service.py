import os
import xml.etree.ElementTree as ET
from pathlib import Path

from fastapi import HTTPException

from .. import parser
from ..config import BILLING_HEADERS, INSTANTANEOUS_HEADERS, LOAD_PROFILE_CORE_HEADERS


def _build_load_profile_headers(data: list):
    other_headers = sorted(
        list(set(key for row in data for key in row.keys() if key not in LOAD_PROFILE_CORE_HEADERS))
    )
    return LOAD_PROFILE_CORE_HEADERS + other_headers


def _process_directory(directory_path: str, extractor, csv_suffix: str, headers=None, is_dict_writer=False):
    all_data = []
    files = parser.get_cdf_files(directory_path)
    for file in files:
        file_path = os.path.join(directory_path, file)
        try:
            tree = ET.parse(file_path)
            meter_no = parser.get_meter_no(tree.getroot())
            data = extractor(tree.getroot(), meter_no)
            all_data.extend(data)

            if data:
                csv_path = os.path.join(directory_path, f"{Path(file).stem}_{csv_suffix}.csv")
                if headers is None:
                    resolved_headers = _build_load_profile_headers(data)
                    parser.save_csv(data, csv_path, resolved_headers, is_dict_writer=True)
                else:
                    parser.save_csv(data, csv_path, headers, is_dict_writer=is_dict_writer)
        except ET.ParseError:
            continue

    return {"status": "success", "total_records": len(all_data), "preview": all_data[:100]}


def _process_file(file_path: str, extractor, csv_suffix: str, headers=None, is_dict_writer=False):
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = extractor(tree.getroot(), meter_no)

        if data:
            directory = os.path.dirname(file_path)
            csv_path = os.path.join(directory, f"{Path(file_path).stem}_{csv_suffix}.csv")
            if headers is None:
                resolved_headers = _build_load_profile_headers(data)
                parser.save_csv(data, csv_path, resolved_headers, is_dict_writer=True)
            else:
                parser.save_csv(data, csv_path, headers, is_dict_writer=is_dict_writer)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError as exc:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(exc)}")


def process_directory_instantaneous(directory_path: str):
    return _process_directory(
        directory_path=directory_path,
        extractor=parser.extract_instantaneous,
        csv_suffix="Instantaneous",
        headers=INSTANTANEOUS_HEADERS,
    )


def process_directory_load_profile(directory_path: str):
    return _process_directory(
        directory_path=directory_path,
        extractor=parser.extract_load_profile,
        csv_suffix="LoadProfile",
    )


def process_directory_billing(directory_path: str):
    return _process_directory(
        directory_path=directory_path,
        extractor=parser.extract_billing,
        csv_suffix="Billing",
        headers=BILLING_HEADERS,
    )


def process_file_instantaneous(file_path: str):
    return _process_file(
        file_path=file_path,
        extractor=parser.extract_instantaneous,
        csv_suffix="Instantaneous",
        headers=INSTANTANEOUS_HEADERS,
    )


def process_file_load_profile(file_path: str):
    return _process_file(
        file_path=file_path,
        extractor=parser.extract_load_profile,
        csv_suffix="LoadProfile",
    )


def process_file_billing(file_path: str):
    return _process_file(
        file_path=file_path,
        extractor=parser.extract_billing,
        csv_suffix="Billing",
        headers=BILLING_HEADERS,
    )
