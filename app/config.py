import os

from dotenv import load_dotenv


load_dotenv()

ES_ENDPOINT = os.getenv("ES_ENDPOINT")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_CLOUD_ID = os.getenv("ES_CLOUD_ID")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "MDMS")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_LOAD_PROFILE_TABLE = os.getenv("DB_LOAD_PROFILE_TABLE", "LOAD_PROFILE")
DB_DAY_PROFILE_TABLE = os.getenv("DB_DAY_PROFILE_TABLE", "DAY_PROFILE")
DB_BILLING_TABLE = os.getenv("DB_BILLING_TABLE", "MONTHLY_BILLING")
DB_EVENT_TABLE = os.getenv("DB_EVENT_TABLE", "EVENT_DATA")
DB_EVENT_PARAMETER_TABLE = os.getenv("DB_EVENT_PARAMETER_TABLE", "EVENT_PARAMETER_DATA")
DB_PARAMETER_MAPPING_TABLE = os.getenv("DB_PARAMETER_MAPPING_TABLE", "PARAMETER_MAPPING")
DB_UPLOAD_HISTORY_TABLE = os.getenv("DB_UPLOAD_HISTORY_TABLE", "UPLOAD_FILE_HISTORY")

LOAD_PROFILE_INDEX = "meter-load-profile-data"
INSTANTANEOUS_INDEX = "meter-instantaneous-data"
EVENT_INDEX = "meter-event-data"
DAY_PROFILE_INDEX = "meter-day-profile-data"
BILLING_INDEX = "meter-billing-data"

EVENT_SQL_UNIQUE_KEY_COLUMNS = (
    "METER_NO",
    "EVENT_DATE_TIME",
    "SOURCE_EVENT_CODE",
    "LOGID",
    "SOURCE_EVENT_INDEX",
)

INSTANTANEOUS_HEADERS = ["meter_no", "code", "value", "unit"]
LOAD_PROFILE_CORE_HEADERS = ["meter_no", "date", "interval", "timestamp"]
BILLING_HEADERS = [
    "meter_no",
    "section",
    "date_time",
    "timestamp",
    "reset_method",
    "tag",
    "code",
    "value",
    "unit",
    "tod",
    "occdate",
    "mechanism_code",
]
