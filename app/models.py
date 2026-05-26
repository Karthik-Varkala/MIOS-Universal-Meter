from typing import Optional

from pydantic import BaseModel, Field, field_validator

from .validation import (
    validate_date_text,
    validate_directory_path_value,
    validate_download_dir_value,
    validate_file_path_value,
    validate_non_empty_text,
)


class DirectoryRequest(BaseModel):
    directory_path: str

    @field_validator("directory_path")
    @classmethod
    def validate_directory_path(cls, value: str) -> str:
        return validate_directory_path_value(value)


class FileRequest(BaseModel):
    file_path: str

    @field_validator("file_path")
    @classmethod
    def validate_file_path(cls, value: str) -> str:
        return validate_file_path_value(value)


class S3Request(BaseModel):
    bucket_name: str
    object_key: str
    download_dir: Optional[str] = Field(default="S3files", validate_default=True)

    @field_validator("bucket_name")
    @classmethod
    def validate_bucket_name(cls, value: str) -> str:
        return validate_non_empty_text(value, "bucket_name")

    @field_validator("object_key")
    @classmethod
    def validate_object_key(cls, value: str) -> str:
        return validate_non_empty_text(value, "object_key")

    @field_validator("download_dir")
    @classmethod
    def validate_download_dir(cls, value: Optional[str]) -> str:
        return validate_download_dir_value(value or "S3files")


class LoadProfileExportRequest(BaseModel):
    meter_no: str
    date: str

    @field_validator("meter_no")
    @classmethod
    def validate_meter_no(cls, value: str) -> str:
        return validate_non_empty_text(value, "meter_no")

    @field_validator("date")
    @classmethod
    def validate_date(cls, value: str) -> str:
        return validate_date_text(value)


class MeterRequest(BaseModel):
    meter_no: str

    @field_validator("meter_no")
    @classmethod
    def validate_meter_no(cls, value: str) -> str:
        return validate_non_empty_text(value, "meter_no")


class MeterDateRequest(BaseModel):
    meter_no: str
    date: str

    @field_validator("meter_no")
    @classmethod
    def validate_meter_no(cls, value: str) -> str:
        return validate_non_empty_text(value, "meter_no")

    @field_validator("date")
    @classmethod
    def validate_date(cls, value: str) -> str:
        return validate_date_text(value)
