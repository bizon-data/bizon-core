"""Deel HR platform source (REST v2): time off, people and managers."""

from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.letsdeel.com/rest/v2"

# Without a read timeout a stalled socket never becomes retryable and hangs to the pod deadline.
REQUEST_TIMEOUT = (10, 60)

# Only filenames of uploaded documents (e.g. sick notes): sensitive and of no analytical use.
DROPPED_FIELDS = ("attachments",)


PAGE_SIZE = 100


class DeelStreams(str, Enum):
    TIME_OFFS = "time_offs"
    PEOPLE = "people"
    MANAGERS = "managers"


STREAM_PATHS = {
    DeelStreams.TIME_OFFS: "/time_offs",
    DeelStreams.PEOPLE: "/people",
    DeelStreams.MANAGERS: "/managers",
}

OFFSET_STREAMS = {DeelStreams.PEOPLE, DeelStreams.MANAGERS}


class DeelSourceConfig(SourceConfig):
    stream: DeelStreams
    api_key: str
    # Non-2xx bodies are reported before raising.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class DeelAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class DeelSource(AbstractSource):
    def __init__(self, config: DeelSourceConfig):
        super().__init__(config)
        self.config: DeelSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in DeelStreams]

    @staticmethod
    def get_config_class() -> type:
        return DeelSourceConfig

    def get_authenticator(self) -> AuthBase:
        return DeelAuth(self.config.api_key)

    def _url(self) -> str:
        return f"{BASE_URL}{STREAM_PATHS[self.config.stream]}"

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(self._url(), timeout=REQUEST_TIMEOUT)
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _to_records(self, items: List[dict]) -> List[SourceRecord]:
        return [
            SourceRecord(id=str(item["id"]), data={k: v for k, v in item.items() if k not in DROPPED_FIELDS})
            for item in items
        ]

    def get(self, pagination: Optional[dict] = None) -> SourceIteration:
        if self.config.stream in OFFSET_STREAMS:
            return self._get_offset(pagination)
        return self._get_cursor(pagination)

    def _get_offset(self, pagination: Optional[dict]) -> SourceIteration:
        offset = (pagination or {}).get("offset", 0)
        response = self.session.get(
            self._url(),
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=REQUEST_TIMEOUT,
        )
        if not response.ok:
            logger.error(f"Deel API error {response.status_code}: {response.text}")
            response.raise_for_status()

        body = response.json()
        items = body.get("data", [])
        # `total_rows` counts what remains from `offset`, not the grand total; a short page doesn't mean the end.
        remaining = (body.get("page") or {}).get("total_rows")
        if len(items) < PAGE_SIZE and isinstance(remaining, int) and remaining > len(items):
            raise ValueError(f"Deel returned {len(items)} rows at offset {offset} but reports {remaining} remaining")

        records = self._to_records(items)
        has_more = len(items) >= PAGE_SIZE
        logger.info(f"Fetched {len(records)} {self.config.stream.value} at offset {offset} (has_more={has_more})")
        return SourceIteration(
            records=records,
            next_pagination={"offset": offset + len(items)} if has_more else {},
        )

    def _get_cursor(self, pagination: Optional[dict]) -> SourceIteration:
        cursor = (pagination or {}).get("next")
        response = self.session.get(
            self._url(),
            params={"next": cursor} if cursor else None,
            timeout=REQUEST_TIMEOUT,
        )
        if not response.ok:
            logger.error(f"Deel API error {response.status_code}: {response.text}")
            response.raise_for_status()

        body = response.json()
        next_cursor = body.get("next")
        # A missing cursor while more pages exist would otherwise end the run with a truncated table.
        if body.get("has_next_page") and not next_cursor:
            raise ValueError("Deel reported has_next_page but returned no `next` cursor")
        if next_cursor and next_cursor == cursor:
            raise ValueError("Deel returned the same `next` cursor twice")

        records = self._to_records(body.get("data", []))
        logger.info(f"Fetched {len(records)} {self.config.stream.value} (has_more={bool(next_cursor)})")
        return SourceIteration(
            records=records,
            next_pagination={"next": next_cursor} if next_cursor else {},
        )
