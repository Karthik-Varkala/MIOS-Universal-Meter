from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

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


class AllDataRequest(BaseModel):
    file_path: Optional[str] = Field(default=None)
    directory_path: Optional[str] = Field(default=None)

    @field_validator("file_path")
    @classmethod
    def validate_file_path(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        return validate_file_path_value(value)

    @field_validator("directory_path")
    @classmethod
    def validate_directory_path(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        return validate_directory_path_value(value)

    @model_validator(mode="after")
    def validate_source(self):
        has_file = bool(self.file_path)
        has_directory = bool(self.directory_path)
        if has_file == has_directory:
            raise ValueError("Provide exactly one of file_path or directory_path.")
        return self


class EsToSqlAllDataRequest(BaseModel):
    meter_no: Optional[str] = None
    meter_nos: Optional[list[str]] = None
    all_meters: bool = False
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @field_validator("meter_no")
    @classmethod
    def validate_meter_no(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        return validate_non_empty_text(value, "meter_no")

    @field_validator("meter_nos")
    @classmethod
    def validate_meter_nos(cls, value: Optional[list[str]]) -> Optional[list[str]]:
        if value is None:
            return None
        normalized_values = [validate_non_empty_text(meter_no, "meter_nos") for meter_no in value]
        unique_values = list(dict.fromkeys(normalized_values))
        if not unique_values:
            raise ValueError("meter_nos cannot be empty.")
        return unique_values

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_optional_date(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        return validate_date_text(value)

    @model_validator(mode="after")
    def validate_scope_and_dates(self):
        meter_scope_count = sum(
            [
                bool(self.meter_no),
                bool(self.meter_nos),
                bool(self.all_meters),
            ]
        )
        if meter_scope_count != 1:
            raise ValueError("Provide exactly one of meter_no, meter_nos, or all_meters=true.")

        has_start_date = bool(self.start_date)
        has_end_date = bool(self.end_date)
        if has_start_date != has_end_date:
            raise ValueError("Provide both start_date and end_date, or omit both for total data.")

        if has_start_date and has_end_date:
            start = self._parse_date(self.start_date)
            end = self._parse_date(self.end_date)
            if start > end:
                raise ValueError("start_date cannot be after end_date.")

        return self

    @staticmethod
    def _parse_date(value: str) -> datetime:
        for date_format in ("%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                continue
        raise ValueError("Invalid date format. Use either dd-mm-yyyy or yyyy-mm-dd.")
