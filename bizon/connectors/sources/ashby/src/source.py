"""Ashby recruiting API source: jobs and job postings (with their descriptions)."""

from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase, HTTPBasicAuth

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.ashbyhq.com"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)


class AshbyStreams(str, Enum):
    JOB_POSTINGS = "job_postings"
    JOBS = "jobs"


_ENDPOINTS = {
    AshbyStreams.JOB_POSTINGS: "/jobPosting.list",
    AshbyStreams.JOBS: "/job.list",
}
_JOB_POSTING_INFO_ENDPOINT = "/jobPosting.info"


class AshbySourceConfig(SourceConfig):
    stream: AshbyStreams
    api_key: str
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class AshbyAuth(AuthBase):
    """Ashby API key authentication: key as Basic username, empty password."""

    def __init__(self, api_key: str):
        self._basic = HTTPBasicAuth(api_key, "")

    def __call__(self, request):
        return self._basic(request)


class AshbyApiError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class AshbySource(AbstractSource):
    def __init__(self, config: AshbySourceConfig):
        super().__init__(config)
        self.config: AshbySourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return AshbyAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in AshbyStreams]

    @staticmethod
    def get_config_class() -> type:
        return AshbySourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self._request(_ENDPOINTS[AshbyStreams.JOBS], {"limit": 1})
            return True, None
        except Exception as exc:
            return False, str(exc)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _request(self, path: str, payload: dict) -> dict:
        response = self.session.post(
            f"{BASE_URL}{path}",
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Ashby returned non-JSON response from {path}: {response.text[:500]}") from exc

        if not isinstance(data, dict):
            raise RuntimeError(f"Ashby returned an unexpected response from {path}")

        if data.get("success", True) is False:
            error_info = data.get("errorInfo") or {}
            errors = data.get("errors") or []
            first_error = errors[0] if errors and isinstance(errors[0], dict) else {}
            code = str(error_info.get("code") or first_error.get("code") or "unknown")
            message = error_info.get("message") or first_error.get("message") or f"Ashby request failed for {path}"
            raise AshbyApiError(code, str(message))

        return data

    @staticmethod
    def _next_cursor(data: dict, current_cursor: Optional[str] = None) -> Optional[str]:
        if not data.get("moreDataAvailable"):
            return None
        cursor = data.get("nextCursor")
        if not cursor:
            raise RuntimeError("Ashby reported moreDataAvailable without nextCursor")
        if cursor == current_cursor:
            raise RuntimeError("Ashby returned a non-advancing pagination cursor")
        return str(cursor)

    def _fetch_page(
        self,
        path: str,
        body: dict,
        pagination: Optional[dict] = None,
    ) -> Tuple[List[dict], dict]:
        payload = dict(body)
        payload["limit"] = PAGE_SIZE
        cursor = pagination.get("cursor") if pagination else None
        if cursor:
            payload["cursor"] = cursor

        data = self._request(path, payload)
        records = data.get("results") or []
        if not isinstance(records, list):
            raise RuntimeError(f"Ashby returned a non-list results field from {path}")

        next_cursor = self._next_cursor(data, cursor)
        next_pagination = {"cursor": next_cursor} if next_cursor else {}
        return records, next_pagination

    @staticmethod
    def _record_id(record: dict, stream: AshbyStreams) -> str:
        record_id = record.get("id")
        if record_id is None:
            raise RuntimeError(f"Ashby {stream.value} record has no id")
        return str(record_id)

    def _fetch_job_posting_description(self, posting_id: str) -> dict:
        data = self._request(
            _JOB_POSTING_INFO_ENDPOINT,
            {"jobPostingId": posting_id},
        )
        details = data.get("results")
        if not isinstance(details, dict):
            raise RuntimeError(f"Ashby returned no job posting details for {posting_id}")

        required_fields = (
            "descriptionHtml",
            "descriptionPlain",
            "descriptionParts",
        )
        missing_fields = [field for field in required_fields if field not in details]
        if missing_fields:
            raise RuntimeError(f"Ashby job posting {posting_id} details are missing {', '.join(missing_fields)}")

        description_parts = details["descriptionParts"]
        if not isinstance(description_parts, dict):
            raise RuntimeError(f"Ashby job posting {posting_id} has invalid descriptionParts")

        description_body = description_parts.get("descriptionBody")
        if not isinstance(description_body, dict) or not all(field in description_body for field in ("html", "plain")):
            raise RuntimeError(f"Ashby job posting {posting_id} has invalid descriptionBody")

        return {
            "descriptionHtml": details["descriptionHtml"],
            "descriptionPlain": details["descriptionPlain"],
            "descriptionParts": description_parts,
        }

    def _get_top_level(self, stream: AshbyStreams, pagination: Optional[dict]) -> SourceIteration:
        raw_records, next_pagination = self._fetch_page(_ENDPOINTS[stream], {}, pagination)
        records = []
        for raw_record in raw_records:
            record_id = self._record_id(raw_record, stream)
            record = dict(raw_record)
            if stream == AshbyStreams.JOB_POSTINGS:
                record.update(self._fetch_job_posting_description(record_id))
            records.append(SourceRecord(id=record_id, data=record))
        logger.info(f"Fetched {len(records)} Ashby {stream.value} records (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        return self._get_top_level(self.config.stream, pagination)
