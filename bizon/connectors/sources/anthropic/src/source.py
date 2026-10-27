"""Anthropic Analytics source: Claude engagement analytics and cost/usage reports from the Admin Analytics API."""

from datetime import date, timedelta
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.anthropic.com/v1/organizations/analytics"

# The report endpoints are effectively real-time, so they only need to stay on complete UTC days: a partial day
# captured mid-flight is never re-requested once the cursor moves past it.
REPORT_DATA_LAG_DAYS = 1

# The engagement endpoints lag ~2 days and answer a too-recent date with a 400 instead of an empty result, so they
# keep one spare day above the observed lag.
ENGAGEMENT_DATA_LAG_DAYS = 3

REPORT_STREAMS = frozenset({"user_cost_report", "user_usage_report", "cost_report", "usage_report"})

# Re-requested on every incremental run, independently of the per-stream lag: Anthropic restates cost/usage for up
# to 30 days, and a cursor derived from the lag would skip days whenever a lag changes. Dedup downstream on the
# business key ordered by _bizon_loaded_at.
INCREMENTAL_OVERLAP_DAYS = 3

TIME_REPORT_BUCKET_WIDTH = "1d"

# Sent as group_by[] to the report endpoints; record ids are built from these keys (see _group_suffix).
REPORT_GROUP_BY = ["model", "product"]


class AnthropicStreams(str, Enum):
    USERS = "users"
    SUMMARIES = "summaries"
    CHAT_PROJECTS = "chat_projects"
    SKILLS = "skills"
    CONNECTORS = "connectors"
    USER_COST_REPORT = "user_cost_report"
    USER_USAGE_REPORT = "user_usage_report"
    COST_REPORT = "cost_report"
    USAGE_REPORT = "usage_report"


class AnthropicApiKeyAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["x-api-key"] = self.api_key
        request.headers["anthropic-version"] = "2023-06-01"
        return request


class AnthropicSourceConfig(SourceConfig):
    stream: AnthropicStreams
    api_key: str
    page_size: int = 100
    start_date: str = Field(
        ...,
        description="Earliest day (YYYY-MM-DD) fetched by a first or full sync when starting_date is unset",
    )
    starting_date: Optional[str] = Field(
        None,
        description="Explicit start day (YYYY-MM-DD). When set, incremental runs ignore the cursor and re-fetch from it",
    )
    ending_date: Optional[str] = Field(
        None, description="Last day (YYYY-MM-DD) to fetch. Defaults to today minus the stream's data lag"
    )
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class AnthropicSource(AbstractSource):
    def __init__(self, config: AnthropicSourceConfig):
        super().__init__(config)
        self.config: AnthropicSourceConfig = config
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in AnthropicStreams]

    @staticmethod
    def get_config_class() -> type:
        return AnthropicSourceConfig

    def get_authenticator(self) -> AuthBase:
        return AnthropicApiKeyAuth(api_key=self.config.api_key)

    def _lag_days(self) -> int:
        if self.config.stream in REPORT_STREAMS:
            return REPORT_DATA_LAG_DAYS
        return ENGAGEMENT_DATA_LAG_DAYS

    def _end_date(self) -> date:
        if self.config.ending_date:
            return date.fromisoformat(self.config.ending_date)
        return date.today() - timedelta(days=self._lag_days())

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            # Probes /users, an engagement endpoint, so it needs the engagement lag whatever the stream: a report
            # stream's shorter lag would 400 here.
            check_date = (date.today() - timedelta(days=ENGAGEMENT_DATA_LAG_DAYS)).isoformat()
            response = self.session.get(
                f"{BASE_URL}/users",
                params={"limit": 1, "date": check_date},
            )
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _start_date(self) -> date:
        if self.config.starting_date:
            return date.fromisoformat(self.config.starting_date)
        return date.fromisoformat(self.config.start_date)

    def _get_summaries(self, pagination: dict = None) -> SourceIteration:
        """Summaries returns one entry per day for the whole range in one response; the endpoint rejects limit/page.

        ending_date is exclusive in the API, hence the extra day.
        """
        params = {
            "starting_date": self._start_date().isoformat(),
            "ending_date": (self._end_date() + timedelta(days=1)).isoformat(),
        }

        response = self.session.get(f"{BASE_URL}/summaries", params=params)
        response.raise_for_status()
        data = response.json()

        items = data.get("summaries", [])
        records = [SourceRecord(id=str(item.get("starting_at", i)), data=item) for i, item in enumerate(items)]

        logger.info(f"Fetched {len(records)} summary records")
        return SourceIteration(records=records, next_pagination={})

    def _get_by_date(self, endpoint: str, pagination: dict = None) -> SourceIteration:
        """Iterate day by day over a single-date endpoint; `page` walks multi-page results within a day."""
        if pagination and pagination.get("current_date"):
            current_date = date.fromisoformat(pagination["current_date"])
        else:
            current_date = self._start_date()

        end_date = self._end_date()

        params = {
            "limit": self.config.page_size,
            "date": current_date.isoformat(),
        }
        if pagination and pagination.get("page"):
            params["page"] = pagination["page"]

        response = self.session.get(f"{BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        data = response.json()

        items = data.get("data", data.get("results", []))
        records = [
            SourceRecord(
                id=f"{current_date.isoformat()}_{item.get('id', i)}",
                data={"date": current_date.isoformat(), **item},
            )
            for i, item in enumerate(items)
        ]

        next_cursor = data.get("next_page")
        if next_cursor:
            next_pagination = {"current_date": current_date.isoformat(), "page": next_cursor}
        elif current_date < end_date:
            next_pagination = {"current_date": (current_date + timedelta(days=1)).isoformat()}
        else:
            next_pagination = {}

        logger.info(f"Fetched {len(records)} records from {endpoint} for date={current_date}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _group_suffix(row: dict) -> str:
        """Id fragment from the grouped dimensions; group_by returns one row per unique combination of them."""
        return "_".join(str(row.get(key)) for key in REPORT_GROUP_BY)

    def _get_user_report(self, endpoint: str, pagination: dict = None) -> SourceIteration:
        """Daily loop for user_cost_report / user_usage_report, one single-day range per call."""
        if pagination and pagination.get("current_date"):
            current_date = date.fromisoformat(pagination["current_date"])
        else:
            current_date = self._start_date()

        end_date = self._end_date()
        next_date = current_date + timedelta(days=1)

        params = {
            "limit": self.config.page_size,
            "starting_at": f"{current_date.isoformat()}T00:00:00Z",
            "ending_at": f"{next_date.isoformat()}T00:00:00Z",
            "group_by[]": REPORT_GROUP_BY,
        }
        if pagination and pagination.get("page"):
            params["page"] = pagination["page"]

        response = self.session.get(f"{BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        data = response.json()

        items = data.get("data", [])
        records = [
            SourceRecord(
                # One row per (user x grouped dims) per day; user_id alone would collide across a user's models.
                id=f"{current_date.isoformat()}_{item.get('actor', {}).get('user_id', i)}_{self._group_suffix(item)}",
                data={"date": current_date.isoformat(), **item},
            )
            for i, item in enumerate(items)
        ]

        next_cursor = data.get("next_page")
        if next_cursor:
            next_pagination = {"current_date": current_date.isoformat(), "page": next_cursor}
        elif current_date < end_date:
            next_pagination = {"current_date": next_date.isoformat()}
        else:
            next_pagination = {}

        logger.info(f"Fetched {len(records)} records from {endpoint} for date={current_date}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_time_report(self, endpoint: str, pagination: dict = None) -> SourceIteration:
        """Daily loop for cost_report / usage_report, flattening data[].results[] with the bucket bounds attached.

        `limit` caps buckets rather than rows here, and a single-day 1d range yields one bucket, so it is omitted.
        """
        if pagination and pagination.get("current_date"):
            current_date = date.fromisoformat(pagination["current_date"])
        else:
            current_date = self._start_date()

        end_date = self._end_date()
        next_date = current_date + timedelta(days=1)

        params = {
            "starting_at": f"{current_date.isoformat()}T00:00:00Z",
            "ending_at": f"{next_date.isoformat()}T00:00:00Z",
            "bucket_width": TIME_REPORT_BUCKET_WIDTH,
            "group_by[]": REPORT_GROUP_BY,
        }
        if pagination and pagination.get("page"):
            params["page"] = pagination["page"]

        response = self.session.get(f"{BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        data = response.json()

        records = []
        for bucket in data.get("data", []):
            bucket_start = bucket.get("starting_at")
            bucket_end = bucket.get("ending_at")
            for i, result in enumerate(bucket.get("results", []) or []):
                records.append(
                    SourceRecord(
                        id=f"{current_date.isoformat()}_{self._group_suffix(result)}",
                        data={
                            "date": current_date.isoformat(),
                            "bucket_starting_at": bucket_start,
                            "bucket_ending_at": bucket_end,
                            **result,
                        },
                    )
                )

        next_cursor = data.get("next_page")
        if next_cursor:
            next_pagination = {"current_date": current_date.isoformat(), "page": next_cursor}
        elif current_date < end_date:
            next_pagination = {"current_date": next_date.isoformat()}
        else:
            next_pagination = {}

        logger.info(f"Fetched {len(records)} records from {endpoint} for date={current_date}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    _USER_REPORT_ENDPOINTS = {
        AnthropicStreams.USER_COST_REPORT: "user_cost_report",
        AnthropicStreams.USER_USAGE_REPORT: "user_usage_report",
    }
    _TIME_REPORT_ENDPOINTS = {
        AnthropicStreams.COST_REPORT: "cost_report",
        AnthropicStreams.USAGE_REPORT: "usage_report",
    }

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """Resume from last_run minus INCREMENTAL_OVERLAP_DAYS up to today minus the stream's lag.

        Reaching back before the previous end date picks up restatements and self-heals if a lag is changed.
        """
        if self.config.starting_date:
            return self.get(pagination=pagination)

        incremental_start = source_state.last_run.date() - timedelta(days=INCREMENTAL_OVERLAP_DAYS)

        if incremental_start > self._end_date():
            logger.info(
                f"No new data available: incremental_start={incremental_start} > end_date={self._end_date()}. "
                "Skipping fetch."
            )
            return SourceIteration(records=[], next_pagination={})

        stream = self.config.stream

        endpoint_map = {
            AnthropicStreams.USERS: "users",
            AnthropicStreams.CHAT_PROJECTS: "apps/chat/projects",
            AnthropicStreams.SKILLS: "skills",
            AnthropicStreams.CONNECTORS: "connectors",
        }

        if stream in endpoint_map:
            if not pagination:
                pagination = {"current_date": incremental_start.isoformat()}
            return self._get_by_date(endpoint_map[stream], pagination)

        if stream in self._USER_REPORT_ENDPOINTS:
            if not pagination:
                pagination = {"current_date": incremental_start.isoformat()}
            return self._get_user_report(self._USER_REPORT_ENDPOINTS[stream], pagination)

        if stream in self._TIME_REPORT_ENDPOINTS:
            if not pagination:
                pagination = {"current_date": incremental_start.isoformat()}
            return self._get_time_report(self._TIME_REPORT_ENDPOINTS[stream], pagination)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Anthropic source")

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if stream == AnthropicStreams.SUMMARIES:
            return self._get_summaries(pagination)

        endpoint_map = {
            AnthropicStreams.USERS: "users",
            AnthropicStreams.CHAT_PROJECTS: "apps/chat/projects",
            AnthropicStreams.SKILLS: "skills",
            AnthropicStreams.CONNECTORS: "connectors",
        }

        if stream in endpoint_map:
            return self._get_by_date(endpoint_map[stream], pagination)

        if stream in self._USER_REPORT_ENDPOINTS:
            return self._get_user_report(self._USER_REPORT_ENDPOINTS[stream], pagination)

        if stream in self._TIME_REPORT_ENDPOINTS:
            return self._get_time_report(self._TIME_REPORT_ENDPOINTS[stream], pagination)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Anthropic source")
