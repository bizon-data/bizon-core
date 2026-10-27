import json
from unittest.mock import MagicMock

import pytest
import requests

gspread = pytest.importorskip("gspread")

from gspread.exceptions import APIError  # noqa: E402

from bizon.connectors.sources.gsheets.src.source import (  # noqa: E402
    GsheetsSource,
    GsheetsSourceConfig,
    is_retryable,
)

SHEET_URL = "https://docs.google.com/spreadsheets/d/abc/edit"


def api_error(status: int) -> APIError:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps({"error": {"code": status, "message": "boom", "status": "X"}}).encode()
    return APIError(response)


def call(name: str, *side_effect) -> MagicMock:
    # backoff logs the wrapped callable's __name__, which a bare MagicMock lacks
    mock = MagicMock(side_effect=list(side_effect))
    mock.__name__ = name
    return mock


def make_source(**overrides) -> GsheetsSource:
    config = GsheetsSourceConfig.model_validate(
        {
            "name": "gsheets",
            "stream": "worksheet",
            "spreadsheet_url": SHEET_URL,
            "worksheet_name": "Sheet1",
            "init_pipeline": False,
            **overrides,
        }
    )
    return GsheetsSource(config)


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_transient_api_errors_are_retryable(status):
    assert is_retryable(api_error(status))


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_client_api_errors_are_not_retryable(status):
    assert not is_retryable(api_error(status))


@pytest.mark.parametrize(
    "exc",
    [requests.exceptions.ConnectionError(), requests.exceptions.ReadTimeout(), TimeoutError(), ConnectionResetError()],
)
def test_network_errors_are_retryable(exc):
    assert is_retryable(exc)


@pytest.mark.parametrize("exc", [PermissionError(), ValueError("bad"), KeyError("x")])
def test_other_errors_are_not_retryable(exc):
    assert not is_retryable(exc)


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda seconds: None)


def test_transient_error_is_retried_then_succeeds(monkeypatch, no_sleep):
    sheet = MagicMock()
    client = MagicMock()
    client.open_by_url = call("open_by_url", api_error(503), api_error(503), sheet)
    monkeypatch.setattr(GsheetsSource, "get_gspread_client", lambda self: client)

    assert make_source().check_connection() == (True, None)
    assert client.open_by_url.call_count == 3


def test_permission_error_fails_fast_and_names_the_account(monkeypatch, no_sleep):
    client = MagicMock()
    client.open_by_url.side_effect = PermissionError()
    monkeypatch.setattr(GsheetsSource, "get_gspread_client", lambda self: client)
    key = json.dumps({"client_email": "reader@example.iam.gserviceaccount.com"})

    ok, message = make_source(service_account_key=key).check_connection()

    assert ok is False
    assert client.open_by_url.call_count == 1
    assert "PermissionError" in message
    assert "Share the spreadsheet with 'reader@example.iam.gserviceaccount.com'" in message


def test_permission_error_without_known_identity_still_explains(monkeypatch, no_sleep):
    client = MagicMock()
    client.open_by_url.side_effect = PermissionError()
    monkeypatch.setattr(GsheetsSource, "get_gspread_client", lambda self: client)
    monkeypatch.setattr(GsheetsSource, "_identity_email", lambda self: None)

    ok, message = make_source().check_connection()

    assert ok is False
    assert "Share the spreadsheet with the pipeline's service account" in message


def test_get_retries_every_gspread_call(monkeypatch, no_sleep):
    worksheet = MagicMock()
    worksheet.row_values = call("row_values", api_error(500), ["Name", "camelCase"])
    worksheet.get_all_records = call("get_all_records", api_error(429), [{"Name": "a", "camelCase": 1}])
    sheet = MagicMock()
    sheet.worksheet = call("worksheet", api_error(502), worksheet)
    client = MagicMock()
    client.open_by_url = call("open_by_url", api_error(503), sheet)
    monkeypatch.setattr(GsheetsSource, "get_gspread_client", lambda self: client)

    iteration = make_source().get()

    assert [r.data for r in iteration.records] == [{"name": "a", "camel_case": 1}]
    assert iteration.next_pagination == {}
