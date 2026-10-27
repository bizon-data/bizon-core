import json
import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field, model_validator
from requests import Response
from requests.auth import AuthBase
from requests.exceptions import HTTPError, RetryError, Timeout

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

UTC = timezone.utc
_FRACTION = re.compile(r"\.(\d+)")

DEFAULT_BASE_URL = "https://us.posthog.com"
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_METADATA_PAGE_SIZE = 100
DEFAULT_EVENTS_WINDOW_MINUTES = 60
DEFAULT_REQUEST_TIMEOUT_SECONDS = 120
MIN_UUID = "00000000-0000-0000-0000-000000000000"
RETRYABLE_POSTHOG_STATUS_CODES = {408, 429, 500, 502, 503, 504}
UNICODE_REPLACEMENT_CHARACTER = "\ufffd"


class PostHogStreams(str, Enum):
    EVENTS = "events"
    PERSONS = "persons"
    EVENT_DEFINITIONS = "event_definitions"
    PROPERTY_DEFINITIONS = "property_definitions"
    COHORTS = "cohorts"


class PostHogSourceConfig(SourceConfig):
    stream: PostHogStreams
    api_key: str
    project_id: int
    base_url: str = DEFAULT_BASE_URL
    start_date: Optional[str] = Field(
        default=None, description="ISO-8601 start of the events backfill. Required for the events stream."
    )
    batch_size: int = Field(default=DEFAULT_BATCH_SIZE, ge=1, le=50_000)
    metadata_page_size: int = Field(default=DEFAULT_METADATA_PAGE_SIZE, ge=1, le=500)
    events_window_minutes: int = Field(default=DEFAULT_EVENTS_WINDOW_MINUTES, ge=1, le=1_440)
    request_timeout_seconds: int = Field(default=DEFAULT_REQUEST_TIMEOUT_SECONDS, ge=10, le=3_600)
    # The source logs the error body itself before raising, so the session must not raise first.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )

    @model_validator(mode="after")
    def _events_need_a_start_date(self):
        if self.stream == PostHogStreams.EVENTS and not self.start_date:
            raise ValueError("start_date is required for the events stream")
        return self


class PostHogAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        request.headers["Accept"] = "application/json"
        return request


class PostHogSource(AbstractSource):
    def __init__(self, config: PostHogSourceConfig):
        super().__init__(config)
        self.config: PostHogSourceConfig = config
        self.base_url = self.config.base_url.rstrip("/")

    def get_authenticator(self) -> AuthBase:
        return PostHogAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in PostHogStreams]

    @staticmethod
    def get_config_class() -> type:
        return PostHogSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response_json = self._request_json(
                "GET",
                f"/api/projects/{self.config.project_id}/",
            )
            if int(response_json["id"]) == self.config.project_id:
                return True, None
            return False, f"Project {self.config.project_id} is not visible to the provided API key"
        except Exception as exc:
            return False, str(exc)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _request_json(
        self,
        method: str,
        path_or_url: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> dict:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            timeout=(30, self.config.request_timeout_seconds),
        )
        return self._decode_json_response(response)

    @staticmethod
    def _sanitize_string(value: str) -> str:
        if not any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            return value

        return "".join(UNICODE_REPLACEMENT_CHARACTER if 0xD800 <= ord(char) <= 0xDFFF else char for char in value)

    @classmethod
    def _sanitize_json_value(cls, value: Any) -> Any:
        if isinstance(value, str):
            return cls._sanitize_string(value)

        if isinstance(value, list):
            return [cls._sanitize_json_value(item) for item in value]

        if isinstance(value, dict):
            sanitized_dict = {}
            for key, item in value.items():
                sanitized_key = cls._sanitize_string(key) if isinstance(key, str) else key
                sanitized_dict[sanitized_key] = cls._sanitize_json_value(item)
            return sanitized_dict

        return value

    @classmethod
    def _decode_json_response(cls, response: Response) -> dict:
        try:
            payload = response.json()
        except Exception:
            payload = None

        if payload is not None:
            payload = cls._sanitize_json_value(payload)

        if not response.ok:
            body_preview = response.text[:1_000]
            logger.error(f"PostHog API error {response.status_code}: {body_preview}")
            response.raise_for_status()

        if isinstance(payload, dict) and payload.get("error"):
            raise RuntimeError(f"PostHog query error: {payload['error']}")

        if isinstance(payload, dict) and payload.get("type") in {"validation_error", "authentication_error"}:
            raise RuntimeError(
                f"PostHog API error [{payload.get('code', 'unknown')}]: {payload.get('detail', response.text[:500])}"
            )

        if payload is None:
            raise RuntimeError("PostHog response was not valid JSON")

        return payload

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        # Before Python 3.11, fromisoformat rejects a trailing Z and fractions that are not 3 or 6 digits.
        normalized = _FRACTION.sub(lambda m: "." + m.group(1)[:6].ljust(6, "0"), value.replace("Z", "+00:00"), count=1)
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @classmethod
    def _to_epoch_ms(cls, value: str | datetime) -> int:
        if isinstance(value, str):
            dt = cls._parse_datetime(value)
        else:
            dt = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _json_load_if_needed(value: Any) -> Any:
        if not isinstance(value, str):
            return value

        stripped = value.strip()
        if not stripped or stripped[0] not in "[{":
            return value

        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return value

    @classmethod
    def _query_rows_to_records(
        cls,
        response_json: dict,
        *,
        id_field: str,
        timestamp_field: str | None = None,
    ) -> list[SourceRecord]:
        columns = response_json.get("columns") or []
        rows = response_json.get("results") or []
        records: list[SourceRecord] = []

        for row in rows:
            data = dict(zip(columns, row, strict=False))
            for key, value in list(data.items()):
                data[key] = cls._sanitize_json_value(cls._json_load_if_needed(value))

            record_kwargs: dict[str, Any] = {
                "id": str(data[id_field]),
                "data": data,
            }
            if timestamp_field and data.get(timestamp_field):
                record_kwargs["timestamp"] = cls._parse_datetime(str(data[timestamp_field]))

            records.append(SourceRecord(**record_kwargs))

        return records

    def _run_hogql(self, sql: str, *, name: str) -> dict:
        return self._request_json(
            "POST",
            f"/api/projects/{self.config.project_id}/query/",
            json_body={
                "query": {
                    "kind": "HogQLQuery",
                    "query": sql,
                },
                "name": name,
            },
        )

    def _window_size_ms(self) -> int:
        return self.config.events_window_minutes * 60 * 1000

    @staticmethod
    def _is_async_query_response(response_json: dict) -> bool:
        return bool(response_json.get("query_status")) and response_json.get("results") is None

    def _initial_events_pagination(self, start_ms: int, stop_ms: int) -> dict:
        window_end_ms = min(start_ms + self._window_size_ms(), stop_ms)
        return {
            "stop_ms": stop_ms,
            "window_start_ms": start_ms,
            "window_end_ms": window_end_ms,
            "cursor_ts_ms": start_ms - 1,
            "cursor_uuid": MIN_UUID,
        }

    def _advance_events_window(self, pagination: dict) -> dict:
        next_window_start_ms = pagination["window_end_ms"]
        stop_ms = pagination["stop_ms"]
        if next_window_start_ms >= stop_ms:
            return {}

        next_window_end_ms = min(next_window_start_ms + self._window_size_ms(), stop_ms)
        return {
            **pagination,
            "window_start_ms": next_window_start_ms,
            "window_end_ms": next_window_end_ms,
            "cursor_ts_ms": next_window_start_ms - 1,
            "cursor_uuid": MIN_UUID,
        }

    def _shrink_events_window(self, pagination: dict, *, reason: str) -> dict:
        effective_start_ms = max(pagination["window_start_ms"], pagination["cursor_ts_ms"])
        remaining_ms = pagination["window_end_ms"] - effective_start_ms
        if remaining_ms <= 1:
            raise RuntimeError(
                "PostHog event query remained too expensive even at the minimum window size "
                f"after {reason}. Reduce batch_size or start with a narrower events_window_minutes."
            )

        new_window_end_ms = effective_start_ms + max(1, remaining_ms // 2)
        if new_window_end_ms >= pagination["window_end_ms"]:
            raise RuntimeError("Failed to shrink PostHog event window after an asynchronous query response.")

        logger.warning(
            "PostHog event query hit {} for window={}..{}; retrying with smaller window ending at {}",
            reason,
            pagination["window_start_ms"],
            pagination["window_end_ms"],
            new_window_end_ms,
        )
        return {
            **pagination,
            "window_end_ms": new_window_end_ms,
        }

    @staticmethod
    def _is_retryable_events_query_exception(exc: Exception) -> bool:
        if isinstance(exc, (RetryError, Timeout)):
            return True

        if isinstance(exc, HTTPError) and exc.response is not None:
            return exc.response.status_code in RETRYABLE_POSTHOG_STATUS_CODES

        return False

    @staticmethod
    def _describe_events_query_exception(exc: Exception) -> str:
        if isinstance(exc, HTTPError) and exc.response is not None:
            return f"HTTP {exc.response.status_code}"

        return exc.__class__.__name__

    def _events_sql(self, pagination: dict) -> str:
        return f"""
SELECT
    uuid,
    toUnixTimestamp64Milli(timestamp) AS ts_ms,
    timestamp,
    event,
    distinct_id,
    person_id,
    properties
FROM events
WHERE
    toUnixTimestamp64Milli(timestamp) >= {pagination["window_start_ms"]}
    AND toUnixTimestamp64Milli(timestamp) < {pagination["window_end_ms"]}
    AND (
        toUnixTimestamp64Milli(timestamp) > {pagination["cursor_ts_ms"]}
        OR (
            toUnixTimestamp64Milli(timestamp) = {pagination["cursor_ts_ms"]}
            AND uuid > '{pagination["cursor_uuid"]}'
        )
    )
ORDER BY ts_ms ASC, uuid ASC
LIMIT {self.config.batch_size}
""".strip()

    def _get_events_with_pagination(self, pagination: dict) -> SourceIteration:
        current_pagination = pagination

        while current_pagination:
            try:
                response_json = self._run_hogql(
                    self._events_sql(current_pagination),
                    name=f"posthog_{self.config.stream.value}_page",
                )
            except Exception as exc:
                if self._is_retryable_events_query_exception(exc):
                    current_pagination = self._shrink_events_window(
                        current_pagination,
                        reason=self._describe_events_query_exception(exc),
                    )
                    continue
                raise

            if self._is_async_query_response(response_json):
                current_pagination = self._shrink_events_window(
                    current_pagination,
                    reason="an asynchronous query response",
                )
                continue

            records = self._query_rows_to_records(
                response_json,
                id_field="uuid",
                timestamp_field="timestamp",
            )

            if records:
                last_row = records[-1].data
                if len(records) >= self.config.batch_size:
                    next_pagination = {
                        **current_pagination,
                        "cursor_ts_ms": last_row["ts_ms"],
                        "cursor_uuid": last_row["uuid"],
                    }
                else:
                    next_pagination = self._advance_events_window(current_pagination)

                logger.info(
                    "Fetched {} PostHog events (window={}..{}, has_more={})",
                    len(records),
                    current_pagination["window_start_ms"],
                    current_pagination["window_end_ms"],
                    bool(next_pagination),
                )
                return SourceIteration(records=records, next_pagination=next_pagination)

            current_pagination = self._advance_events_window(current_pagination)

        return SourceIteration(records=[], next_pagination={})

    def get_events(self, pagination: dict = None) -> SourceIteration:
        if pagination:
            current_pagination = pagination
        else:
            start_ms = self._to_epoch_ms(self.config.start_date)
            stop_ms = self._to_epoch_ms(datetime.now(tz=UTC))
            current_pagination = self._initial_events_pagination(start_ms=start_ms, stop_ms=stop_ms)

        return self._get_events_with_pagination(current_pagination)

    def _get_events_incremental(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        if pagination:
            current_pagination = pagination
        else:
            start_dt = source_state.last_run
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=UTC)
            else:
                start_dt = start_dt.astimezone(UTC)

            start_ms = self._to_epoch_ms(start_dt)
            # A fixed upper bound for the whole run keeps successive jobs on disjoint [start, stop) windows.
            stop_ms = self._to_epoch_ms(datetime.now(tz=UTC))
            current_pagination = self._initial_events_pagination(start_ms=start_ms, stop_ms=stop_ms)

        return self._get_events_with_pagination(current_pagination)

    def get_persons(self, pagination: dict = None) -> SourceIteration:
        iteration = self._rest_paginated_iteration(
            f"/api/projects/{self.config.project_id}/persons/",
            pagination,
            params={"limit": self.config.batch_size},
        )
        records: list[SourceRecord] = []
        for record in iteration.records:
            timestamp = None
            if record.data.get("created_at"):
                timestamp = self._parse_datetime(str(record.data["created_at"]))
            records.append(
                SourceRecord(
                    id=record.id,
                    data=record.data,
                    timestamp=timestamp or datetime.now(tz=UTC),
                )
            )

        logger.info(f"Fetched {len(records)} PostHog persons (has_more={bool(iteration.next_pagination)})")
        return SourceIteration(records=records, next_pagination=iteration.next_pagination)

    def _rest_paginated_iteration(
        self, path: str, pagination: dict = None, *, params: dict | None = None
    ) -> SourceIteration:
        if pagination and pagination.get("next_url"):
            response_json = self._request_json("GET", pagination["next_url"])
        else:
            response_json = self._request_json("GET", path, params=params)

        results = response_json.get("results", [])
        records = [SourceRecord(id=str(item["id"]), data=item) for item in results]
        next_url = response_json.get("next")
        next_pagination = {"next_url": next_url} if next_url else {}
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_event_definitions(self, pagination: dict = None) -> SourceIteration:
        iteration = self._rest_paginated_iteration(
            f"/api/projects/{self.config.project_id}/event_definitions/",
            pagination,
            params={"limit": self.config.metadata_page_size},
        )
        logger.info(f"Fetched {len(iteration.records)} PostHog event definitions")
        return iteration

    def get_property_definitions(self, pagination: dict = None) -> SourceIteration:
        iteration = self._rest_paginated_iteration(
            f"/api/projects/{self.config.project_id}/property_definitions/",
            pagination,
            params={"limit": self.config.metadata_page_size},
        )
        logger.info(f"Fetched {len(iteration.records)} PostHog property definitions")
        return iteration

    def get_cohorts(self, pagination: dict = None) -> SourceIteration:
        iteration = self._rest_paginated_iteration(
            f"/api/projects/{self.config.project_id}/cohorts/",
            pagination,
            params={"limit": self.config.metadata_page_size},
        )
        logger.info(f"Fetched {len(iteration.records)} PostHog cohorts")
        return iteration

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == PostHogStreams.EVENTS:
            return self._get_events_incremental(source_state, pagination)

        raise NotImplementedError(f"Incremental sync is not implemented for PostHog stream '{stream.value}'")

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if stream == PostHogStreams.EVENTS:
            return self.get_events(pagination)
        if stream == PostHogStreams.PERSONS:
            return self.get_persons(pagination)
        if stream == PostHogStreams.EVENT_DEFINITIONS:
            return self.get_event_definitions(pagination)
        if stream == PostHogStreams.PROPERTY_DEFINITIONS:
            return self.get_property_definitions(pagination)
        if stream == PostHogStreams.COHORTS:
            return self.get_cohorts(pagination)

        raise NotImplementedError(f"Stream '{stream.value}' is not implemented for PostHog source")
