import os
from pathlib import Path

from fastapi import HTTPException

from .. import parser
from ..config import BILLING_HEADERS, INSTANTANEOUS_HEADERS, LOAD_PROFILE_CORE_HEADERS
from ..validation import log_processing_failure, parse_cdf_xml, require_meter_no


def _build_load_profile_headers(data: list):
    other_headers = sorted(
        list(set(key for row in data for key in row.keys() if key not in LOAD_PROFILE_CORE_HEADERS))
    )
    return LOAD_PROFILE_CORE_HEADERS + other_headers


def _process_directory(
    directory_path: str,
    extractor,
    csv_suffix: str,
    operation_name: str,
    headers=None,
    is_dict_writer=False,
):
    all_data = []
    failed_files = []
    files = parser.get_cdf_files(directory_path)
    processed_files = 0
    for file in files:
        file_path = os.path.join(directory_path, file)
        try:
            tree = parse_cdf_xml(file_path)
            root = tree.getroot()
            meter_no = require_meter_no(root, file_path=file_path, operation=operation_name)
            data = extractor(root, meter_no, file_path=file_path)
            all_data.extend(data)

            if data:
                csv_path = os.path.join(directory_path, f"{Path(file).stem}_{csv_suffix}.csv")
                if headers is None:
                    resolved_headers = _build_load_profile_headers(data)
                    parser.save_csv(data, csv_path, resolved_headers, is_dict_writer=True)
                else:
                    parser.save_csv(data, csv_path, headers, is_dict_writer=is_dict_writer)
            processed_files += 1
        except Exception as exc:
            failed_files.append(log_processing_failure(file_path, operation_name, exc))
            continue

    return {
        "status": "success",
        "processed_files": processed_files,
        "skipped_files": len(failed_files),
        "failed_files": failed_files,
        "total_records": len(all_data),
        "preview": all_data[:100],
    }


def _process_file(file_path: str, extractor, csv_suffix: str, operation_name: str, headers=None, is_dict_writer=False):
    try:
        tree = parse_cdf_xml(file_path)
        root = tree.getroot()
        meter_no = require_meter_no(root, file_path=file_path, operation=operation_name)
        data = extractor(root, meter_no, file_path=file_path)

        if data:
            directory = os.path.dirname(file_path)
            csv_path = os.path.join(directory, f"{Path(file_path).stem}_{csv_suffix}.csv")
            if headers is None:
                resolved_headers = _build_load_profile_headers(data)
                parser.save_csv(data, csv_path, resolved_headers, is_dict_writer=True)
            else:
                parser.save_csv(data, csv_path, headers, is_dict_writer=is_dict_writer)

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except HTTPException as exc:
        log_processing_failure(file_path, operation_name, exc)
        raise
    except Exception as exc:
        log_processing_failure(file_path, operation_name, exc)
        raise


def process_directory_instantaneous(directory_path: str):
    return _process_directory(
        directory_path=directory_path,
        extractor=parser.extract_instantaneous,
        csv_suffix="Instantaneous",
        operation_name="process_directory_instantaneous",
        headers=INSTANTANEOUS_HEADERS,
    )


def process_directory_load_profile(directory_path: str):
    return _process_directory(
        directory_path=directory_path,
        extractor=parser.extract_load_profile,
        csv_suffix="LoadProfile",
        operation_name="process_directory_load_profile",
    )


def process_directory_billing(directory_path: str):
    return _process_directory(
        directory_path=directory_path,
        extractor=parser.extract_billing,
        csv_suffix="Billing",
        operation_name="process_directory_billing",
        headers=BILLING_HEADERS,
    )


def process_file_instantaneous(file_path: str):
    return _process_file(
        file_path=file_path,
        extractor=parser.extract_instantaneous,
        csv_suffix="Instantaneous",
        operation_name="process_file_instantaneous",
        headers=INSTANTANEOUS_HEADERS,
    )


def process_file_load_profile(file_path: str):
    return _process_file(
        file_path=file_path,
        extractor=parser.extract_load_profile,
        csv_suffix="LoadProfile",
        operation_name="process_file_load_profile",
    )


def process_file_billing(file_path: str):
    return _process_file(
        file_path=file_path,
        extractor=parser.extract_billing,
        csv_suffix="Billing",
        operation_name="process_file_billing",
        headers=BILLING_HEADERS,
    )
