"""Chargebee source: incremental subscription, comment and event streams over the v2 list endpoints.

`full_scan` ignores the cursor and re-fetches every record, for accumulating-snapshot tables: Chargebee does not
bump `updated_at` when a subscription's metered usage changes.
"""

from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase, HTTPBasicAuth

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

DEFAULT_PAGE_SIZE = 100  # Chargebee's documented max for list endpoints

# `after` is exclusive; back off so a record exactly on the boundary isn't dropped. Duplicates resolve downstream.
CURSOR_OVERLAP_SECONDS = 60


class ChargebeeStreams(str, Enum):
    SUBSCRIPTION = "subscription"
    COMMENT = "comment"
    EVENT = "event"


# Each list entry wraps its resource under the stream name: {"list": [{"subscription": {...}, ...}], ...}.
_ENDPOINTS = {
    ChargebeeStreams.SUBSCRIPTION: "subscriptions",
    ChargebeeStreams.COMMENT: "comments",
    ChargebeeStreams.EVENT: "events",
}

_CURSOR_FIELD = {
    ChargebeeStreams.SUBSCRIPTION: "updated_at",
    ChargebeeStreams.COMMENT: "created_at",
    ChargebeeStreams.EVENT: "occurred_at",
}


class ChargebeeAuth(AuthBase):
    """Site API key as Basic username, empty password."""

    def __init__(self, api_key: str):
        self._basic = HTTPBasicAuth(api_key, "")

    def __call__(self, request):
        return self._basic(request)


class ChargebeeSourceConfig(SourceConfig):
    stream: ChargebeeStreams
    site: str = Field(..., description="Chargebee site name, the <site> in <site>.chargebee.com")
    api_key: str = Field(..., description="Chargebee site API key")
    start_date: str = Field(..., description="ISO-8601 lower bound for the first sync, e.g. 2025-01-01T00:00:00Z")
    page_size: int = DEFAULT_PAGE_SIZE
    full_scan: bool = Field(
        False, description="Ignore the cursor and re-fetch every record on every run, for accumulating snapshots"
    )
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class ChargebeeSource(AbstractSource):
    def __init__(self, config: ChargebeeSourceConfig):
        super().__init__(config)
        self.config: ChargebeeSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return ChargebeeAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in ChargebeeStreams]

    @staticmethod
    def get_config_class() -> type:
        return ChargebeeSourceConfig

    def _base_url(self) -> str:
        return f"https://{self.config.site}.chargebee.com/api/v2"

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(
                f"{self._base_url()}/subscriptions",
                params={"limit": 1},
                timeout=(10, 60),
            )
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    @staticmethod
    def _to_epoch(iso_date: str) -> int:
        return int(datetime.fromisoformat(iso_date.replace("Z", "+00:00")).timestamp())

    def _fetch_page(self, endpoint: str, params: dict) -> Tuple[list, Optional[str]]:
        response = self.session.get(
            f"{self._base_url()}/{endpoint}",
            params=params,
            timeout=(10, 60),
        )
        response.raise_for_status()
        data = response.json()
        return data.get("list", []), data.get("next_offset")

    def _get_filtered_stream(
        self, stream: ChargebeeStreams, after_epoch: Optional[int], pagination: dict = None
    ) -> SourceIteration:
        """after_epoch is None for a full_scan, which omits the TimestampFilter."""
        cursor_field = _CURSOR_FIELD[stream]
        envelope_key = stream.value

        # Offset pagination over a mutating sort key would miss or repeat records updated mid-scan, so a full scan
        # sorts on the immutable created_at. The incremental path's cursor field pairs with <cursor>[after].
        sort_field = cursor_field if after_epoch is not None else "created_at"

        params = {
            "limit": self.config.page_size,
            "sort_by[asc]": sort_field,
        }
        if after_epoch is not None:
            params[f"{cursor_field}[after]"] = after_epoch
        if pagination and pagination.get("offset"):
            params["offset"] = pagination["offset"]

        items, next_offset = self._fetch_page(_ENDPOINTS[stream], params)

        records = [SourceRecord(id=str(entry[envelope_key]["id"]), data=entry[envelope_key]) for entry in items]
        next_pagination = {"offset": next_offset} if next_offset else {}

        logger.info(f"Fetched {len(records)} {stream.value} records (more={bool(next_offset)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _dispatch(self, after_epoch: int, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream in _CURSOR_FIELD:
            return self._get_filtered_stream(stream, None if self.config.full_scan else after_epoch, pagination)
        raise NotImplementedError(f"Stream '{stream}' is not implemented for Chargebee source")

    def get(self, pagination: dict = None) -> SourceIteration:
        return self._dispatch(self._to_epoch(self.config.start_date), pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        after_epoch = int(source_state.last_run.timestamp()) - CURSOR_OVERLAP_SECONDS
        return self._dispatch(after_epoch, pagination)
