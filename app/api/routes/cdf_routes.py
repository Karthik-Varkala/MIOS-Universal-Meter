from fastapi import APIRouter, HTTPException, Request

from ...services.cdf_service import (
    process_directory_billing,
    process_directory_instantaneous,
    process_directory_load_profile,
    process_file_billing,
    process_file_instantaneous,
    process_file_load_profile,
)
from ...services.s3_service import download_s3_file
from ..request_parsing import extract_single_path, extract_s3_request


router = APIRouter()


@router.post("/api/dir/instantaneous")
async def get_dir_instantaneous(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return process_directory_instantaneous(directory_path)


@router.post("/api/dir/load-profile")
async def get_dir_load_profile(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return process_directory_load_profile(directory_path)


@router.post("/api/dir/billing")
async def get_dir_billing(request: Request):
    directory_path = await extract_single_path(request, "directory_path")
    return process_directory_billing(directory_path)


@router.post("/api/file/instantaneous")
async def get_single_file_instantaneous(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return process_file_instantaneous(file_path)


@router.post("/api/file/load-profile")
async def get_single_file_load_profile(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return process_file_load_profile(file_path)


@router.post("/api/file/billing")
async def get_single_file_billing(request: Request):
    file_path = await extract_single_path(request, "file_path")
    return process_file_billing(file_path)


@router.post("/api/s3/instantaneous")
async def process_s3_instantaneous(request: Request):
    req_data = await extract_s3_request(request)
    local_file_path = download_s3_file(req_data["bucket_name"], req_data["object_key"], req_data["download_dir"])
    try:
        return process_file_instantaneous(local_file_path)
    except HTTPException as exc:
        if exc.status_code == 400 and str(exc.detail).startswith("Unable to read file:"):
            raise HTTPException(status_code=400, detail="Unable to read the downloaded file.")
        raise


@router.post("/api/s3/load-profile")
async def process_s3_load_profile(request: Request):
    req_data = await extract_s3_request(request)
    local_file_path = download_s3_file(req_data["bucket_name"], req_data["object_key"], req_data["download_dir"])
    try:
        return process_file_load_profile(local_file_path)
    except HTTPException as exc:
        if exc.status_code == 400 and str(exc.detail).startswith("Unable to read file:"):
            raise HTTPException(status_code=400, detail="Unable to read the downloaded file.")
        raise


@router.post("/api/s3/billing")
async def process_s3_billing(request: Request):
    req_data = await extract_s3_request(request)
    local_file_path = download_s3_file(req_data["bucket_name"], req_data["object_key"], req_data["download_dir"])
    try:
        return process_file_billing(local_file_path)
    except HTTPException as exc:
        if exc.status_code == 400 and str(exc.detail).startswith("Unable to read file:"):
            raise HTTPException(status_code=400, detail="Unable to read the downloaded file.")
        raise
