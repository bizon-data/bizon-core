import json
import re
from typing import Any, Counter, List, Tuple
from uuid import uuid4

import google.auth
import gspread
from google.oauth2.service_account import Credentials
from loguru import logger
from requests.auth import AuthBase

from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import Source

from .config import GsheetsSourceConfig


class GsheetsSource(Source):

    def __init__(self, config: GsheetsSourceConfig):
        super().__init__(config)
        self.config: GsheetsSourceConfig = config
        self.normalization_pattern = re.compile(r"(?<!^)(?=[A-Z])")

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
            logger.error(f"Column names are not unique: {duplicates}.")
            raise ValueError(f"Column names are not unique: {duplicates}.")
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

        # Ensure column names are unique
        column_names = worksheet.row_values(1)
        self.check_column_names_are_unique(column_names)

        # Get all records
        all_records = worksheet.get_all_records()

        if self.config.convert_column_names_to_sql_format:
            self.normalize_record_to_sql_format_inplace(all_records)

        return SourceIteration(
            records=[SourceRecord(id=uuid4().hex, data=record) for record in all_records],
            next_pagination={},
        )
