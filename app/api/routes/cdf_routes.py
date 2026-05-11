from fastapi import APIRouter, HTTPException

from ...models import DirectoryRequest, FileRequest, S3Request
from ...services.cdf_service import (
    process_directory_billing,
    process_directory_instantaneous,
    process_directory_load_profile,
    process_file_billing,
    process_file_instantaneous,
    process_file_load_profile,
)
from ...services.s3_service import download_s3_file


router = APIRouter()


@router.post("/api/dir/instantaneous")
def get_dir_instantaneous(req: DirectoryRequest):
    return process_directory_instantaneous(req.directory_path)


@router.post("/api/dir/load-profile")
def get_dir_load_profile(req: DirectoryRequest):
    return process_directory_load_profile(req.directory_path)


@router.post("/api/dir/billing")
def get_dir_billing(req: DirectoryRequest):
    return process_directory_billing(req.directory_path)


@router.post("/api/file/instantaneous")
def get_single_file_instantaneous(req: FileRequest):
    return process_file_instantaneous(req.file_path)


@router.post("/api/file/load-profile")
def get_single_file_load_profile(req: FileRequest):
    return process_file_load_profile(req.file_path)


@router.post("/api/file/billing")
def get_single_file_billing(req: FileRequest):
    return process_file_billing(req.file_path)


@router.post("/api/s3/instantaneous")
def process_s3_instantaneous(req: S3Request):
    local_file_path = download_s3_file(req.bucket_name, req.object_key, req.download_dir)
    try:
        return process_file_instantaneous(local_file_path)
    except HTTPException as exc:
        if exc.status_code == 400 and str(exc.detail).startswith("XML Parse Error:"):
            raise HTTPException(status_code=400, detail="Invalid XML format in downloaded file.")
        raise


@router.post("/api/s3/load-profile")
def process_s3_load_profile(req: S3Request):
    local_file_path = download_s3_file(req.bucket_name, req.object_key, req.download_dir)
    try:
        return process_file_load_profile(local_file_path)
    except HTTPException as exc:
        if exc.status_code == 400 and str(exc.detail).startswith("XML Parse Error:"):
            raise HTTPException(status_code=400, detail="Invalid XML format in downloaded file.")
        raise


@router.post("/api/s3/billing")
def process_s3_billing(req: S3Request):
    local_file_path = download_s3_file(req.bucket_name, req.object_key, req.download_dir)
    try:
        return process_file_billing(local_file_path)
    except HTTPException as exc:
        if exc.status_code == 400 and str(exc.detail).startswith("XML Parse Error:"):
            raise HTTPException(status_code=400, detail="Invalid XML format in downloaded file.")
        raise
