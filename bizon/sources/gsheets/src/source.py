import json
from typing import Any, Tuple
from uuid import uuid4

import google.auth
import gspread
from google.oauth2.service_account import Credentials
from requests.auth import AuthBase

from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import Source

from .config import GsheetsSourceConfig


class GsheetsSource(Source):

    def __init__(self, config: GsheetsSourceConfig):
        super().__init__(config)
        self.config: GsheetsSourceConfig = config

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

    def get(self, pagination: dict = None) -> SourceIteration:
        gc = self.get_gspread_client()
        worksheet = gc.open_by_url(self.config.spreadsheet_url).worksheet(self.config.worksheet_name)
        all_records = worksheet.get_all_records()
        return SourceIteration(
            records=[SourceRecord(id=uuid4().hex, data=record) for record in all_records],
            next_pagination={},
        )
