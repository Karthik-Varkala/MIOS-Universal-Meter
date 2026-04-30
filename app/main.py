import os
import xml.etree.ElementTree as ET
from pathlib import Path

import boto3
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from fastapi import FastAPI, HTTPException

from . import parser
from .models import DirectoryRequest, FileRequest, S3Request


load_dotenv()


app = FastAPI(
    title="CDF Data API",
    description="Extracts meter data, processes single files, handles S3 downloads, and publishes to Elasticsearch.",
)

# --- Elasticsearch Configuration ---
ES_ENDPOINT = os.getenv("ES_ENDPOINT")
ES_API_KEY = os.getenv("ES_API_KEY")

es_client = Elasticsearch(
    ES_ENDPOINT,
    api_key=ES_API_KEY,
)


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
                parser.save_csv(data, csv_path, ["MeterNo", "Code", "Value", "Unit"])
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
                core_headers = ["MeterNo", "Date", "Interval"]
                other_headers = sorted(list(set(k for row in data for k in row.keys() if k not in core_headers)))
                parser.save_csv(data, csv_path, core_headers + other_headers, is_dict_writer=True)
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
                parser.save_csv(data, csv_path, ["MeterNo", "Section", "DateTime", "Code", "Value", "Unit"])
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
            parser.save_csv(data, csv_path, ["MeterNo", "Code", "Value", "Unit"])

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
            core_headers = ["MeterNo", "Date", "Interval"]
            other_headers = sorted(list(set(k for row in data for k in row.keys() if k not in core_headers)))
            parser.save_csv(data, csv_path, core_headers + other_headers, is_dict_writer=True)

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
            parser.save_csv(data, csv_path, ["MeterNo", "Section", "DateTime", "Code", "Value", "Unit"])

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
            parser.save_csv(data, csv_path, ["MeterNo", "Code", "Value", "Unit"])

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
            core_headers = ["MeterNo", "Date", "Interval"]
            other_headers = sorted(list(set(k for row in data for k in row.keys() if k not in core_headers)))
            parser.save_csv(data, csv_path, core_headers + other_headers, is_dict_writer=True)

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
            parser.save_csv(data, csv_path, ["MeterNo", "Section", "DateTime", "Code", "Value", "Unit"])

        return {"status": "success", "meter_no": meter_no, "total_records": len(data), "data": data[:100]}
    except ET.ParseError:
        raise HTTPException(status_code=400, detail="Invalid XML format in downloaded file.")


# ==========================================
# ELASTICSEARCH INTEGRATION ENDPOINTS
# ==========================================

def publish_to_es_helper(data: list, index_name: str):
    """Helper function to push a list of dictionaries to a specific ES index."""
    if not data:
        return {"status": "skipped", "message": f"No data found to publish to {index_name}."}

    actions = [{"_index": index_name, "_source": record} for record in data]

    try:
        success, failed = helpers.bulk(es_client, actions, stats_only=True)
        return {
            "status": "success",
            "index": index_name,
            "documents_published": success,
            "failed_publish": failed,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch Bulk Insert Error: {str(e)}")


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


@app.post("/api/elasticsearch/load-profile")
def es_push_load_profile(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_load_profile(tree.getroot(), meter_no)
        return publish_to_es_helper(data, index_name="meter-load-profile-data")
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")


@app.post("/api/elasticsearch/billing")
def es_push_billing(req: FileRequest):
    if not os.path.isfile(req.file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        tree = ET.parse(req.file_path)
        meter_no = parser.get_meter_no(tree.getroot())
        data = parser.extract_billing(tree.getroot(), meter_no)
        return publish_to_es_helper(data, index_name="meter-billing-data")
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"XML Parse Error: {str(e)}")
