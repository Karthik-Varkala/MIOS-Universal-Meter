import os

import boto3
from fastapi import HTTPException

from ..logging_utils import get_logger, log_exception


logger = get_logger(__name__)


def download_s3_file(bucket_name: str, object_key: str, download_dir: str) -> str:
    os.makedirs(download_dir, exist_ok=True)
    local_file_path = os.path.join(download_dir, os.path.basename(object_key))

    s3 = boto3.client("s3")
    try:
        s3.download_file(bucket_name, object_key, local_file_path)
        return local_file_path
    except Exception as exc:
        log_exception(
            logger,
            "S3 download failed",
            exc,
            bucket_name=bucket_name,
            object_key=object_key,
            download_dir=download_dir,
            local_file_path=local_file_path,
        )
        raise HTTPException(status_code=500, detail=f"Failed to download from S3: {str(exc)}")
