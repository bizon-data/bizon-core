"""Contrast.io webinar platform source: events, registrations, live/replay views and poll votes.

`page` is capped at 100 (422 past it), so one date window tops out at ~10k rows; the date-windowed streams are
split into quarterly windows. Records carry their own videoId/eventStartsAt/eventEndsAt for joining.
"""

from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://connect.getcontrast.io/v1"

# Full-history floor for the date-windowed streams, which return nothing without a window.
FULL_HISTORY_START = date(2000, 1, 1)


class ContrastIoStreams(str, Enum):
    EVENTS = "events"
    LIVE_VIEWS = "live_views"
    POLL_VOTES = "poll_votes"
    REGISTRATIONS = "registrations"
    REPLAY_VIEWS = "replay_views"


_DATE_WINDOWED_STREAMS = {
    ContrastIoStreams.LIVE_VIEWS,
    ContrastIoStreams.POLL_VOTES,
    ContrastIoStreams.REGISTRATIONS,
    ContrastIoStreams.REPLAY_VIEWS,
}


def _quarter_windows(end_bound: date) -> List[Tuple[str, str]]:
    """Quarterly [start, end) windows from FULL_HISTORY_START through end_bound."""
    windows = []
    year, q = FULL_HISTORY_START.year, 0
    while True:
        start = date(year, q * 3 + 1, 1)
        if start > end_bound:
            break
        ny, nq = (year + 1, 0) if q == 3 else (year, q + 1)
        end = min(date(ny, nq * 3 + 1, 1), end_bound)
        windows.append((start.isoformat(), end.isoformat()))
        if end >= end_bound:
            break
        year, q = ny, nq
    return windows


class ContrastIoSourceConfig(SourceConfig):
    stream: ContrastIoStreams
    api_key: str = Field(..., description="Contrast.io API key")
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class ContrastIoAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class ContrastIoSource(AbstractSource):
    def __init__(self, config: ContrastIoSourceConfig):
        super().__init__(config)
        self.config: ContrastIoSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in ContrastIoStreams]

    @staticmethod
    def get_config_class() -> type:
        return ContrastIoSourceConfig

    def get_authenticator(self) -> AuthBase:
        return ContrastIoAuth(self.config.api_key)

    def _fetch(self, params: dict) -> dict:
        response = self.session.get(
            f"{BASE_URL}/{self.config.stream.value}",
            params=params,
            timeout=(10, 60),
        )
        response.raise_for_status()
        return response.json()

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            params: dict[str, Any] = {"page": 1}
            if ContrastIoStreams(self.config.stream) in _DATE_WINDOWED_STREAMS:
                today = datetime.now(timezone.utc).date()
                params["startDate"] = (today - timedelta(days=1)).isoformat()
                params["endDate"] = (today + timedelta(days=1)).isoformat()
            self._fetch(params)
            return True, None
        except Exception as e:  # noqa: BLE001 - surfaced to caller
            return False, repr(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = ContrastIoStreams(self.config.stream)
        if stream in _DATE_WINDOWED_STREAMS:
            return self._get_windowed(pagination)
        return self._get_flat(pagination)

    def _get_flat(self, pagination: dict = None) -> SourceIteration:
        pagination = pagination or {}
        page = pagination.get("page", 1)

        payload = self._fetch({"page": page})
        items = payload.get("data") or []
        meta = payload.get("meta") or {}
        next_page = meta.get("nextPage")

        records = [SourceRecord(id=str(item["id"]), data=item) for item in items]
        next_pagination = {"page": next_page} if next_page else {}

        logger.info(
            f"Contrast.io {self.config.stream.value}: fetched {len(records)} records "
            f"(page={page}, total={meta.get('total')}, has_more={bool(next_pagination)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_windowed(self, pagination: dict = None) -> SourceIteration:
        pagination = pagination or {}
        end_bound = datetime.now(timezone.utc).date() + timedelta(days=1)
        windows = _quarter_windows(end_bound)

        window_idx = pagination.get("window_idx", 0)
        page = pagination.get("page", 1)

        if window_idx >= len(windows):
            return SourceIteration(records=[], next_pagination={})

        start, end = windows[window_idx]
        payload = self._fetch({"page": page, "startDate": start, "endDate": end})
        items = payload.get("data") or []
        meta = payload.get("meta") or {}
        next_page = meta.get("nextPage")

        records = [SourceRecord(id=str(item["id"]), data=item) for item in items]

        if next_page:
            next_pagination = {"window_idx": window_idx, "page": next_page}
        elif window_idx + 1 < len(windows):
            next_pagination = {"window_idx": window_idx + 1, "page": 1}
        else:
            next_pagination = {}

        logger.info(
            f"Contrast.io {self.config.stream.value}: fetched {len(records)} records "
            f"(window={start}..{end} [{window_idx + 1}/{len(windows)}], page={page}, "
            f"total={meta.get('total')}, has_more={bool(next_pagination)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)
