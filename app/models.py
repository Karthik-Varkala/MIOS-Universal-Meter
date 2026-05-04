from typing import Optional

from pydantic import BaseModel


class DirectoryRequest(BaseModel):
    directory_path: str


class FileRequest(BaseModel):
    file_path: str


class S3Request(BaseModel):
    bucket_name: str
    object_key: str
    download_dir: Optional[str] = "S3files"


class LoadProfileExportRequest(BaseModel):
    meter_no: str
    date: str
