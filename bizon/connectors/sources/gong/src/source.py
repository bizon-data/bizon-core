"""Gong source: calls, extensive calls, transcripts, users and scorecards from the v2 API."""

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase, HTTPBasicAuth

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.gong.io"


class GongStreams(str, Enum):
    CALLS = "calls"
    CALLS_EXTENSIVE = "calls_extensive"
    TRANSCRIPTS = "transcripts"
    USERS = "users"
    USERS_EXTENSIVE = "users_extensive"
    SCORECARDS = "scorecards"
    ANSWERED_SCORECARDS = "answered_scorecards"


CALLS_EXTENSIVE_CONTENT_SELECTOR = {
    "context": "Extended",
    "contextTiming": ["Now", "TimeOfCall"],
    "exposedFields": {
        "parties": True,
        "content": {
            "structure": True,
            "topics": True,
            "trackers": True,
            "trackerOccurrences": True,
            "pointsOfInterest": True,
            "brief": True,
            "outline": True,
            "highlights": True,
            "callOutcome": True,
            "keyPoints": True,
        },
        "interaction": {
            "speakers": True,
            "video": True,
            "personInteractionStats": True,
            "questions": True,
        },
        "collaboration": {"publicComments": True},
        "media": True,
    },
}


class GongSourceConfig(SourceConfig):
    stream: GongStreams
    access_key: str = Field(..., description="Gong API access key")
    access_key_secret: str = Field(..., description="Gong API access key secret")
    start_date: str = Field(..., description="ISO-8601 fromDateTime for the first sync of the call streams")
    # fromDateTime matches a call's START time but transcripts publish after it ends. Each run re-appends the
    # window (dedupe on id downstream), so size it to the longest call plus the publish delay.
    lookback_hours: int = 12
    # toDateTime for the first/full-refresh run only, for bounded historical backfills.
    end_date: Optional[str] = None
    # Gong answers 404 when a filter window matches zero records.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class GongSource(AbstractSource):
    def __init__(self, config: GongSourceConfig):
        super().__init__(config)
        self.config: GongSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return HTTPBasicAuth(self.config.access_key, self.config.access_key_secret)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in GongStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return GongSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/v2/users", params={"limit": 1}, timeout=(10, 30))
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Returns {} for a 404, Gong's "no records" signal."""
        url = f"{BASE_URL}{path}"
        response = self.session.request(method, url, timeout=(10, 300), **kwargs)
        if response.status_code == 404:
            logger.info(f"Gong returned 404 (no records) for {method} {path}")
            return {}
        if not response.ok:
            msg = f"Gong API error {method} {path}: HTTP {response.status_code} — {response.text[:500]}"
            logger.error(msg)
            raise RuntimeError(msg)
        return response.json()

    def _fetch_get(self, path: str, params: dict, pagination: Optional[dict], records_key: str) -> Tuple[list, dict]:
        if pagination and pagination.get("cursor"):
            data = self._request("GET", path, params={"cursor": pagination["cursor"]})
        else:
            data = self._request("GET", path, params=params)
        records = data.get(records_key, []) if data else []
        next_cursor = (data.get("records") or {}).get("cursor") if data else None
        next_pagination = {"cursor": next_cursor} if next_cursor else {}
        return records, next_pagination

    def _fetch_post(self, path: str, body: dict, pagination: Optional[dict], records_key: str) -> Tuple[list, dict]:
        """Gong needs the full request body on every page, with the cursor added on top; anything less 400s."""
        payload = dict(body)
        if pagination and pagination.get("cursor"):
            payload["cursor"] = pagination["cursor"]
        data = self._request("POST", path, json=payload)
        records = data.get(records_key, []) if data else []
        next_cursor = (data.get("records") or {}).get("cursor") if data else None
        next_pagination = {"cursor": next_cursor} if next_cursor else {}
        return records, next_pagination

    @staticmethod
    def _iso(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_calls(self, pagination: dict = None, from_dt: str = None, to_dt: str = None) -> SourceIteration:
        params = {}
        if from_dt:
            params["fromDateTime"] = from_dt
        if to_dt:
            params["toDateTime"] = to_dt
        records_raw, next_pagination = self._fetch_get("/v2/calls", params, pagination, records_key="calls")
        records = [SourceRecord(id=str(r["id"]), data=r) for r in records_raw]
        logger.info(f"Fetched {len(records)} calls (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_calls_extensive(self, pagination: dict = None, from_dt: str = None, to_dt: str = None) -> SourceIteration:
        body = {
            "filter": {},
            "contentSelector": CALLS_EXTENSIVE_CONTENT_SELECTOR,
        }
        if from_dt:
            body["filter"]["fromDateTime"] = from_dt
        if to_dt:
            body["filter"]["toDateTime"] = to_dt
        records_raw, next_pagination = self._fetch_post("/v2/calls/extensive", body, pagination, records_key="calls")
        records = [SourceRecord(id=str(r["metaData"]["id"]), data=r) for r in records_raw]
        logger.info(f"Fetched {len(records)} calls_extensive (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_transcripts(self, pagination: dict = None, from_dt: str = None, to_dt: str = None) -> SourceIteration:
        body = {"filter": {}}
        if from_dt:
            body["filter"]["fromDateTime"] = from_dt
        if to_dt:
            body["filter"]["toDateTime"] = to_dt
        records_raw, next_pagination = self._fetch_post(
            "/v2/calls/transcript", body, pagination, records_key="callTranscripts"
        )
        records = [SourceRecord(id=str(r["callId"]), data=r) for r in records_raw]
        logger.info(f"Fetched {len(records)} transcripts (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_users(self, pagination: dict = None) -> SourceIteration:
        records_raw, next_pagination = self._fetch_get(
            "/v2/users", params={}, pagination=pagination, records_key="users"
        )
        records = [SourceRecord(id=str(r["id"]), data=r) for r in records_raw]
        logger.info(f"Fetched {len(records)} users (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_users_extensive(self, pagination: dict = None) -> SourceIteration:
        body = {"filter": {"includeAvatars": False}}
        records_raw, next_pagination = self._fetch_post("/v2/users/extensive", body, pagination, records_key="users")
        records = [SourceRecord(id=str(r["id"]), data=r) for r in records_raw]
        logger.info(f"Fetched {len(records)} users_extensive (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_scorecards(self, pagination: dict = None) -> SourceIteration:
        records_raw, next_pagination = self._fetch_get(
            "/v2/settings/scorecards",
            params={},
            pagination=pagination,
            records_key="scorecards",
        )
        records = [SourceRecord(id=str(r["scorecardId"]), data=r) for r in records_raw]
        logger.info(f"Fetched {len(records)} scorecards (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_answered_scorecards(self, pagination: dict = None) -> SourceIteration:
        # This endpoint's filter is date-only (callFromDate/reviewFromDate...); a fromDateTime is silently ignored.
        body = {"filter": {}}
        records_raw, next_pagination = self._fetch_post(
            "/v2/stats/activity/scorecards",
            body,
            pagination,
            records_key="answeredScorecards",
        )
        records = [SourceRecord(id=str(r["answeredScorecardId"]), data=r) for r in records_raw]
        logger.info(f"Fetched {len(records)} answered_scorecards (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        from_dt = self.config.start_date
        to_dt = self.config.end_date

        if stream == GongStreams.CALLS:
            return self.get_calls(pagination, from_dt=from_dt, to_dt=to_dt)
        if stream == GongStreams.CALLS_EXTENSIVE:
            return self.get_calls_extensive(pagination, from_dt=from_dt, to_dt=to_dt)
        if stream == GongStreams.TRANSCRIPTS:
            return self.get_transcripts(pagination, from_dt=from_dt, to_dt=to_dt)
        if stream == GongStreams.USERS:
            return self.get_users(pagination)
        if stream == GongStreams.USERS_EXTENSIVE:
            return self.get_users_extensive(pagination)
        if stream == GongStreams.SCORECARDS:
            return self.get_scorecards(pagination)
        if stream == GongStreams.ANSWERED_SCORECARDS:
            # Not incremental: with no new reviews Gong 404s and the job never commits its cursor. The API may
            # not serve full history, so a refresh can hold fewer rows than an earlier export.
            return self.get_answered_scorecards(pagination)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Gong source")

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        # The lookback applies here, not in get(), so the first run uses start_date verbatim.
        from_dt = self._iso(source_state.last_run - timedelta(hours=self.config.lookback_hours))
        stream = self.config.stream

        if stream == GongStreams.CALLS:
            return self.get_calls(pagination, from_dt=from_dt)
        if stream == GongStreams.CALLS_EXTENSIVE:
            return self.get_calls_extensive(pagination, from_dt=from_dt)
        if stream == GongStreams.TRANSCRIPTS:
            return self.get_transcripts(pagination, from_dt=from_dt)
        if stream == GongStreams.USERS:
            return self.get_users(pagination)
        if stream == GongStreams.USERS_EXTENSIVE:
            return self.get_users_extensive(pagination)
        if stream == GongStreams.SCORECARDS:
            return self.get_scorecards(pagination)

        raise NotImplementedError(f"Incremental sync not supported for stream '{stream}'")
