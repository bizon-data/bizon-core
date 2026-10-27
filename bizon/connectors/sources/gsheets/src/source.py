import json
import re
import socket
import ssl
from collections import Counter
from typing import List, Optional, Tuple
from uuid import uuid4

import backoff
import google.auth
import gspread
import gspread.utils
import requests
from google.auth.exceptions import TransportError
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

# gspread turns a 403 into a bare PermissionError: a stable verdict that must fail fast, so it is not listed.
RETRYABLE_EXCEPTION_TYPES = (
    APIError,
    TransportError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    socket.timeout,
    ssl.SSLError,
    ConnectionError,
    TimeoutError,
)


def is_retryable(exc: Exception) -> bool:
    """Only 408/429/5xx and transport-level errors are transient; a 403/404 never fixes itself."""
    if isinstance(exc, APIError):
        status = exc.response.status_code
        return status == 408 or status == 429 or 500 <= status < 600
    if isinstance(exc, PermissionError):
        return False
    return isinstance(exc, RETRYABLE_EXCEPTION_TYPES)


class GsheetsSourceConfig(SourceConfig):
    worksheet_name: str = Field(description="Name of the worksheet to fetch data from", default=...)
    spreadsheet_url: str = Field(description="URL of the spreadsheet", default=...)
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )
    column_names: list[str] = Field(
        description="Column names to fetch from the worksheet, if empty all columns will be fetched",
        default=[],
    )
    convert_column_names_to_sql_format: bool = Field(
        description="Convert column names to SQL format (lowercase, no spaces, etc)",
        default=True,
    )
    request_max_time_seconds: int = Field(
        description="Total time budget in seconds for retrying each Google Sheets call",
        default=300,
    )


class GsheetsSource(AbstractSource):
    def __init__(self, config: GsheetsSourceConfig):
        super().__init__(config)
        self.config: GsheetsSourceConfig = config
        self.normalization_pattern = re.compile(r"(?<!^)(?=[A-Z])")

        # gspread uses its own session, so bizon's HTTP retry policy never sees these calls.
        # gspread's BackOffHTTPClient is not used because it also retries every 403.
        self._retry = backoff.on_exception(
            backoff.expo,
            RETRYABLE_EXCEPTION_TYPES,
            max_tries=8,
            max_time=self.config.request_max_time_seconds,
            jitter=backoff.full_jitter,
            giveup=lambda e: not is_retryable(e),
        )

    @staticmethod
    def streams() -> List[str]:
        return ["worksheet"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return GsheetsSourceConfig

    def get_gspread_client(self) -> gspread.client.Client:
        if self.config.service_account_key:
            # use creds to create a client to interact with the Google Drive API
            credentials_dict = json.loads(self.config.service_account_key)
            credentials = Credentials.from_service_account_info(credentials_dict)
            credentials = credentials.with_scopes(gspread.auth.READONLY_SCOPES)
            gc = gspread.authorize(credentials)
        else:
            # use default credentials
            credentials, project_id = google.auth.default(scopes=gspread.auth.READONLY_SCOPES)
            gc = gspread.authorize(credentials)
        return gc

    def _identity_email(self) -> Optional[str]:
        """Best-effort email of the identity reading the sheet, for the 403 hint. Never raises."""
        if self.config.service_account_key:
            try:
                return json.loads(self.config.service_account_key).get("client_email")
            except Exception:
                return None
        try:
            credentials, _ = google.auth.default(scopes=gspread.auth.READONLY_SCOPES)
            return getattr(credentials, "service_account_email", None)
        except Exception:
            return None

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            gc = self.get_gspread_client()
            sh = self._retry(gc.open_by_url)(self.config.spreadsheet_url)
        except PermissionError as e:
            # str(e) is empty here, so name the type and the identity the sheet must be shared with
            identity = self._identity_email()
            account = repr(identity) if identity else "the pipeline's service account"
            return False, (
                f"Failed to open spreadsheet {self.config.spreadsheet_url!r}: {type(e).__name__}: {e} "
                f"-- the sheet answered HTTP 403. Share the spreadsheet with {account} "
                "(Viewer access is enough) and re-run."
            )
        except Exception as e:
            return False, f"Failed to open spreadsheet {self.config.spreadsheet_url!r}: {type(e).__name__}: {e}"

        try:
            self._retry(sh.worksheet)(self.config.worksheet_name)
        except gspread.WorksheetNotFound:
            return False, f"Worksheet not found, available worksheets: {sh.worksheets()}"
        except Exception as e:
            return False, (
                f"Failed to open worksheet {self.config.worksheet_name!r} of spreadsheet "
                f"{self.config.spreadsheet_url!r}: {type(e).__name__}: {e}"
            )
        return True, None

    def get_authenticator(self) -> AuthBase | None:
        return None

    def get_total_records_count(self) -> int | None:
        return None

    def check_column_names_are_unique(self, column_names: List[str]) -> bool:
        """Check if all column names are unique, otherwise raise an error listing duplicates"""
        if len(column_names) != len(set(column_names)):
            duplicates = [item for item, count in Counter(column_names).items() if count > 1]
            logger.error(
                f"Column names are not unique: {duplicates}, found following columns: {column_names}."
                f"Please provide unique column names in the config using `column_names` param or remove duplicated column in sheets."
            )
            raise ValueError(f"Column names are not unique: {duplicates}, found following columns: {column_names}.")
        return True

    def normalize_record_to_sql_format_inplace(self, records: List[dict]) -> dict:
        """Normalize record to SQL format inplace"""
        for i, record in enumerate(records):
            records[i] = {
                self.normalization_pattern.sub("_", k).lower().replace(" ", "_"): v for k, v in record.items()
            }

    def keep_only_selected_columns(self, records: List[dict], column_names: List[str]) -> List[dict]:
        """Keep only selected columns in records"""
        return [{k: v for k, v in record.items() if k in column_names} for record in records]

    def get(self, pagination: dict = None) -> SourceIteration:
        gc = self.get_gspread_client()
        sh = self._retry(gc.open_by_url)(self.config.spreadsheet_url)
        worksheet = self._retry(sh.worksheet)(self.config.worksheet_name)

        worksheet_column_names = self._retry(worksheet.row_values)(1)

        if self.config.column_names:
            logger.info(f"Using provided column names: {','.join(self.config.column_names)}")

            self.check_column_names_are_unique(self.config.column_names)

            # Ensure column names are all present in worksheet_column_names
            for column_name in self.config.column_names:
                if column_name not in worksheet_column_names:
                    raise ValueError(f"Column name {column_name} not found in worksheet")

            # Get all records
            all_records = self._retry(worksheet.get_all_records)(expected_headers=self.config.column_names)
            all_records = self.keep_only_selected_columns(all_records, self.config.column_names)

        else:
            # Ensure column names are unique
            self.check_column_names_are_unique(worksheet_column_names)

            # Get all records
            all_records = self._retry(worksheet.get_all_records)()

        if self.config.convert_column_names_to_sql_format:
            self.normalize_record_to_sql_format_inplace(all_records)

        return SourceIteration(
            records=[SourceRecord(id=uuid4().hex, data=record) for record in all_records],
            next_pagination={},
        )
