import json
import re
from typing import Any, Counter, List, Tuple, Type
from uuid import uuid4

import google.auth
import gspread
import gspread.utils
from google.oauth2.service_account import Credentials
from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


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


class GsheetsSource(AbstractSource):

    def __init__(self, config: GsheetsSourceConfig):
        super().__init__(config)
        self.config: GsheetsSourceConfig = config
        self.normalization_pattern = re.compile(r"(?<!^)(?=[A-Z])")

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

    def check_connection(self) -> Tuple[bool | Any | None]:
        gc = self.get_gspread_client()

        # Open a sheet from a spreadsheet in one go
        sh = gc.open_by_url(self.config.spreadsheet_url)
        try:
            _ = sh.worksheet(self.config.worksheet_name)
        except gspread.WorksheetNotFound:
            return False, f"Worksheet not found, available worksheets: {sh.worksheets()}"
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

    def get(self, pagination: dict = None) -> SourceIteration:
        gc = self.get_gspread_client()
        worksheet = gc.open_by_url(self.config.spreadsheet_url).worksheet(self.config.worksheet_name)

        worksheet_column_names = worksheet.row_values(1)

        if self.config.column_names:
            logger.info(f"Using provided column names: {','.join(self.config.column_names)}")

            self.check_column_names_are_unique(self.config.column_names)

            # Ensure column names are all present in worksheet_column_names
            for column_name in self.config.column_names:
                if column_name not in worksheet_column_names:
                    raise ValueError(f"Column name {column_name} not found in worksheet")

            # Get all records
            all_records = worksheet.get_all_records(expected_headers=self.config.column_names)
            all_records = gspread.utils.to_records(headers=self.config.column_names, values=all_records)

        else:
            # Ensure column names are unique
            self.check_column_names_are_unique(worksheet_column_names)

            # Get all records
            all_records = worksheet.get_all_records()

        if self.config.convert_column_names_to_sql_format:
            self.normalize_record_to_sql_format_inplace(all_records)

        return SourceIteration(
            records=[SourceRecord(id=uuid4().hex, data=record) for record in all_records],
            next_pagination={},
        )
