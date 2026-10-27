"""OpenAI ChatGPT Compliance Platform source (`api.chatgpt.com`).

- chatgpt_costs: ChatGPT Enterprise COSTS compliance log files (org-scoped, needs a compliance-scoped key and
  organization_id), downloaded and parsed as JSON Lines, one file per iteration.
- workspace_daily_usage: per-user-per-day ChatGPT+Codex usage from the Daily Usage analytics endpoint
  (workspace-scoped, needs scope `enterprise.analytics.usage.read` and workspace_id).
"""

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

import requests
from loguru import logger
from pydantic import Field
from requests.auth import AuthBase
from requests.exceptions import HTTPError

from bizon.source.config import HttpConfig, HttpRetryConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

COMPLIANCE_BASE_URL = "https://api.chatgpt.com/v1"

COSTS_EVENT_TYPE = "COSTS"

# Compliance log files for a given hour land 3-5h late and can still be revised shortly after. Rescanning behind
# last_run is safe because records are keyed on event_id.
COSTS_RESCAN_LAG_HOURS = 24

# The compliance log only retains 30 days of files.
COSTS_RETENTION_DAYS = 30

# Daily usage aggregates can be revised for a couple of days after the fact.
ANALYTICS_LAG_DAYS = 2

ANALYTICS_LIMIT = 1000

# Download URLs are pre-signed and fetched outside the session, so they get their own timeout.
SIGNED_URL_TIMEOUT = (10, 60)


class OpenaiStreams(str, Enum):
    CHATGPT_COSTS = "chatgpt_costs"
    WORKSPACE_DAILY_USAGE = "workspace_daily_usage"


ANALYTICS_USAGE_STREAMS = {OpenaiStreams.WORKSPACE_DAILY_USAGE}


class OpenaiApiKeyAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class OpenaiSourceConfig(SourceConfig):
    stream: OpenaiStreams
    api_key: str
    starting_date: Optional[str] = Field(
        None,
        description="Start day (YYYY-MM-DD). Unset starts 30 days back; when set, incremental runs ignore the cursor",
    )
    organization_id: Optional[str] = Field(None, description="Required for the chatgpt_costs stream")
    workspace_id: Optional[str] = Field(None, description="Required for the workspace_daily_usage stream")
    # Errors are raised by hand so the API's 4xx body, which carries the actionable reason, reaches the message.
    # 520-524 are Cloudflare edge errors the API returns transiently on wide-window list calls.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(
            raise_for_status=False,
            retries=HttpRetryConfig(status_forcelist=[429, 500, 502, 503, 504, 520, 521, 522, 523, 524]),
        ),
        description="HTTP timeout/retry policy",
    )


class OpenaiSource(AbstractSource):
    def __init__(self, config: OpenaiSourceConfig):
        super().__init__(config)
        self.config: OpenaiSourceConfig = config
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        if self.config.stream == OpenaiStreams.CHATGPT_COSTS and not self.config.organization_id:
            raise ValueError(
                "organization_id is required for the chatgpt_costs stream "
                "(the org compliance route /compliance/organizations/{organization_id}/logs "
                "serves org-scoped event types such as COSTS)"
            )
        if self.config.stream == OpenaiStreams.WORKSPACE_DAILY_USAGE and not self.config.workspace_id:
            raise ValueError(
                "workspace_id is required for the workspace_daily_usage stream "
                "(Daily Usage endpoint /analytics/workspaces/{workspace_id}/usage)"
            )

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in OpenaiStreams]

    @staticmethod
    def get_config_class() -> type:
        return OpenaiSourceConfig

    def get_authenticator(self) -> AuthBase:
        return OpenaiApiKeyAuth(api_key=self.config.api_key)

    @staticmethod
    def _to_unix(d: date) -> int:
        return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())

    @staticmethod
    def _as_utc(dt: datetime) -> datetime:
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        if self.config.stream == OpenaiStreams.CHATGPT_COSTS:
            try:
                recent_after = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
                self._list_compliance_log_files(after=recent_after, limit=1)
                return True, None
            except Exception as e:
                return False, str(e)
        try:
            now = datetime.now(timezone.utc)
            params = {
                "start_time": int((now - timedelta(days=1)).timestamp()),
                "end_time": int(now.timestamp()),
                "limit": 1,
            }
            response = self.session.get(
                f"{COMPLIANCE_BASE_URL}/analytics/workspaces/{self.config.workspace_id}/usage",
                params=params,
            )
            self._raise_for_status_with_body(response)
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _compliance_logs_base_url(self) -> str:
        return f"{COMPLIANCE_BASE_URL}/compliance/organizations/{self.config.organization_id}/logs"

    @staticmethod
    def _raise_for_status_with_body(response: requests.Response) -> None:
        """raise_for_status() with the (truncated) response body folded into the error.

        The API puts the actionable reason (missing scope, invalid event_type, disabled feature) in the 4xx body,
        which the stock HTTPError message drops.
        """
        try:
            response.raise_for_status()
        except HTTPError as e:
            body = (response.text or "").strip()[:2000]
            raise HTTPError(f"{e} -- response body: {body}", response=response) from None

    def _list_compliance_log_files(
        self, after: str, before: Optional[str] = None, limit: int = 100
    ) -> Tuple[List[dict], bool, Optional[str]]:
        """List COSTS log files ordered by end_time; returns (files, has_more, last_end_time).

        `after` is required by the API and is exclusive on end_time; paginate with after=last_end_time.
        """
        params: dict = {"event_type": COSTS_EVENT_TYPE, "after": after, "limit": limit}
        if before:
            params["before"] = before

        response = self.session.get(self._compliance_logs_base_url(), params=params)
        self._raise_for_status_with_body(response)
        data = response.json()
        return data.get("data", []) or [], bool(data.get("has_more", False)), data.get("last_end_time")

    def _download_compliance_file(self, log_file_id: str) -> List[dict]:
        """Download and parse one compliance log file (JSON Lines). A failure at either stage skips the file.

        The authed GET answers a 307 to a short-lived pre-signed storage URL, which is fetched without the
        session: the storage host does not expect, and may reject, the API's Authorization header.
        """
        try:
            response = self.session.get(
                f"{self._compliance_logs_base_url()}/{log_file_id}",
                allow_redirects=False,
            )
            response.raise_for_status()
        except HTTPError as e:
            logger.warning(f"Skipping compliance log file={log_file_id}: error requesting download URL: {e}")
            return []

        location = response.headers.get("Location")
        if not location:
            logger.warning(
                f"Skipping compliance log file={log_file_id}: no Location header on {response.status_code} response"
            )
            return []

        try:
            file_response = requests.get(location, stream=True, timeout=SIGNED_URL_TIMEOUT)
            file_response.raise_for_status()
        except HTTPError as e:
            logger.warning(f"Skipping compliance log file={log_file_id}: failed to download signed URL: {e}")
            return []

        events: List[dict] = []
        for line in file_response.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Skipping malformed JSONL line in compliance log file={log_file_id}: {e}")
        return events

    def _costs_event_row(self, entry: dict, log_file_id: str) -> Optional[SourceRecord]:
        event_id = entry.get("event_id")
        if not event_id:
            logger.warning(f"Skipping chatgpt_costs event with missing event_id in file={log_file_id}")
            return None

        payload = entry.get("payload") or {}
        identity = payload.get("identity") or {}
        measures = payload.get("measures") or {}
        usage = measures.get("usage") or {}
        agent = identity.get("agent") or {}

        row = {
            "event_id": str(event_id),
            "timestamp": entry.get("timestamp"),
            "day": payload.get("day"),
            "hour": payload.get("hour"),
            "organization_id": payload.get("organization_id"),
            "user_id": identity.get("user_id"),
            "email": identity.get("email"),
            "name": identity.get("name"),
            "groups": identity.get("groups"),
            "agent_id": agent.get("id"),
            "agent_name": agent.get("name"),
            "product": payload.get("product"),
            "surface": payload.get("surface"),
            "client": payload.get("client"),
            "model": payload.get("model"),
            "service_tier": payload.get("service_tier"),
            "reasoning": payload.get("reasoning"),
            # Token fields vary by plan (detailed vs total-only), so spread whatever is present.
            **usage,
            "billing": measures.get("billing"),
            "payload": payload,
        }
        return SourceRecord(id=str(event_id), data=row)

    def _get_compliance_log(self, pagination: dict) -> SourceIteration:
        """Download one compliance log file per call.

        Pagination: `after` (listing anchor), `files` (remaining file ids of the current listing page),
        `last_end_time` and `has_more` (to seed the next listing page). Callers always seed `after`.
        """
        if "files" not in pagination:
            files_data, has_more, last_end_time = self._list_compliance_log_files(after=pagination["after"])
            file_ids = [f["id"] for f in files_data if f.get("id")]
            pagination = {
                "after": pagination["after"],
                "files": file_ids,
                "last_end_time": last_end_time,
                "has_more": has_more,
            }
            logger.info(
                f"Listed {len(file_ids)} chatgpt_costs log file(s) after={pagination['after']} (has_more={has_more})"
            )

        files = pagination["files"]

        if not files:
            if pagination.get("has_more") and pagination.get("last_end_time"):
                return SourceIteration(records=[], next_pagination={"after": pagination["last_end_time"]})
            return SourceIteration(records=[], next_pagination={})

        log_file_id = files[0]
        remaining = files[1:]

        entries = self._download_compliance_file(log_file_id)

        records: List[SourceRecord] = []
        for entry in entries:
            entry_type = entry.get("type") or entry.get("event_type")
            if entry_type != COSTS_EVENT_TYPE:
                continue

            rec = self._costs_event_row(entry, log_file_id)
            if rec is not None:
                records.append(rec)

        if remaining:
            next_pagination = {
                "after": pagination["after"],
                "files": remaining,
                "last_end_time": pagination.get("last_end_time"),
                "has_more": pagination.get("has_more", False),
            }
        elif pagination.get("has_more") and pagination.get("last_end_time"):
            next_pagination = {"after": pagination["last_end_time"]}
        else:
            next_pagination = {}

        logger.info(
            f"Fetched {len(records)} chatgpt_costs records from file={log_file_id} "
            f"(files_left_after_this={len(remaining)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    _analytics_hash_id_warned = False

    def _analytics_record_id(self, stream: "OpenaiStreams", row: dict) -> str:
        """`<stream>_<start_time>_<user_id or 'workspace'>`, stable across re-fetches of a revised daily row.

        Falls back to a content hash if `start_time` is missing, which is not stable across revisions.
        """
        start = row.get("start_time")
        if start is not None:
            user = row.get("user_id") or "workspace"
            return f"{stream.value}_{start}_{user}"

        if not OpenaiSource._analytics_hash_id_warned:
            logger.warning(
                f"analytics stream '{stream.value}': row missing the required "
                "`start_time` field — falling back to a content-hash record id. This is "
                "UNSAFE for revised daily aggregates (a revision changes the hash, creating a "
                "duplicate row instead of updating in place) and indicates the API schema has "
                "drifted from the documented shape; investigate before relying on this stream."
            )
            OpenaiSource._analytics_hash_id_warned = True

        row_hash = hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
        return f"{stream.value}_{row_hash}"

    def _get_workspace_daily_usage(self, pagination: dict) -> SourceIteration:
        """Cursor-paginated GET over a fixed [start_time, end_time) window; callers always seed the window.

        `group` is never sent, so the API returns per-user rows rather than the workspace aggregate.
        """
        params: dict = {
            "start_time": pagination["start_time"],
            "end_time": pagination["end_time"],
            "limit": ANALYTICS_LIMIT,
        }
        if pagination.get("page"):
            params["page"] = pagination["page"]

        response = self.session.get(
            f"{COMPLIANCE_BASE_URL}/analytics/workspaces/{self.config.workspace_id}/usage",
            params=params,
        )
        self._raise_for_status_with_body(response)
        data = response.json()

        records = [
            SourceRecord(
                id=self._analytics_record_id(OpenaiStreams.WORKSPACE_DAILY_USAGE, row),
                data={
                    **row,
                    "_stream": OpenaiStreams.WORKSPACE_DAILY_USAGE.value,
                    "_window_start_time": pagination["start_time"],
                    "_window_end_time": pagination["end_time"],
                },
            )
            for row in (data.get("data", []) or [])
        ]

        if data.get("has_more") and data.get("next_page"):
            next_pagination = {
                "start_time": pagination["start_time"],
                "end_time": pagination["end_time"],
                "page": data["next_page"],
            }
        else:
            next_pagination = {}

        logger.info(
            f"Fetched {len(records)} 'workspace_daily_usage' records "
            f"(start_time={pagination['start_time']}, end_time={pagination['end_time']}, "
            f"has_more={data.get('has_more', False)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        if self.config.stream == OpenaiStreams.CHATGPT_COSTS:
            if self.config.starting_date:
                return self.get(pagination=pagination)

            # An after=last_run cursor would permanently skip files that land late.
            now = datetime.now(timezone.utc)
            last_run = self._as_utc(source_state.last_run)
            initial_after = max(
                last_run - timedelta(hours=COSTS_RESCAN_LAG_HOURS),
                now - timedelta(days=COSTS_RETENTION_DAYS),
            )
            if not pagination:
                pagination = {"after": initial_after.isoformat()}
            return self._get_compliance_log(pagination)

        if self.config.starting_date:
            return self.get(pagination=pagination)

        if not pagination:
            last_run = self._as_utc(source_state.last_run)
            start_time = int((last_run - timedelta(days=ANALYTICS_LAG_DAYS)).timestamp())
            end_time = int(datetime.now(timezone.utc).timestamp())
            pagination = {"start_time": start_time, "end_time": end_time}
        return self._get_workspace_daily_usage(pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == OpenaiStreams.CHATGPT_COSTS:
            if not pagination:
                if self.config.starting_date:
                    start = date.fromisoformat(self.config.starting_date)
                    initial_after = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
                else:
                    initial_after = datetime.now(timezone.utc) - timedelta(days=COSTS_RETENTION_DAYS)
                pagination = {"after": initial_after.isoformat()}
            return self._get_compliance_log(pagination)

        if not pagination:
            if self.config.starting_date:
                start_date = date.fromisoformat(self.config.starting_date)
                start_time = self._to_unix(start_date)
            else:
                start_time = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
            end_time = int(datetime.now(timezone.utc).timestamp())
            pagination = {"start_time": start_time, "end_time": end_time}
        return self._get_workspace_daily_usage(pagination)
