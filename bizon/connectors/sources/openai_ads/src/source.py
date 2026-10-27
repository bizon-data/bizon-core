"""OpenAI Ads (ChatGPT Ads) account, campaign structure, conversion settings, audit log and daily insights, from
the OpenAI Advertiser API (https://api.ads.openai.com/v1, API-key bearer auth; one key = one ad account).

Insights request every documented metric and metadata field for the row level. The API rejects an unknown
projection with HTTP 400, so a rejected field list is bisected to isolate and drop the field instead of failing
the stream.
"""

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from pydantic import Field, model_validator
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.ads.openai.com/v1"
INSIGHTS_PAGE_SIZE = 2000
CONVERSION_ENTITY_BATCH = 50
# Conversion and sales metrics accept at most 365 days per request.
MAX_WINDOW_DAYS = 365

DELIVERY_METRICS = ["impressions", "clicks", "spend", "ctr", "cpc", "cpm"]
CONVERSION_METRICS = ["conversions", "cpa", "post_click_cvr", "order_created_attributed_sales", "order_created_roas"]
LEVEL_METADATA = {
    "campaign": [
        "campaign.id",
        "campaign.name",
        "campaign.description",
        "campaign.status",
        "campaign.start_time",
        "campaign.end_time",
        "campaign.budget.lifetime",
        "campaign.budget.daily",
    ],
    "ad_group": ["campaign.id", "ad_group.id", "ad_group.name", "ad_group.description", "ad_group.status"],
    "ad": [
        "campaign.id",
        "ad_group.id",
        "ad.id",
        "ad.name",
        "ad.title",
        "ad.copy",
        "ad.link",
        "ad.status",
        "ad.review_status",
    ],
}
SEGMENT_METADATA = {"country": ["country.name"], "device": ["device.type"], "platform": ["platform"]}


class OpenaiAdsStreams(str, Enum):
    # Entity snapshots (full refresh)
    AD_ACCOUNT = "ad_account"
    CAMPAIGNS = "campaigns"
    AD_GROUPS = "ad_groups"
    ADS = "ads"
    CONVERSION_EVENT_SETTINGS = "conversion_event_settings"
    CUSTOM_AUDIENCES = "custom_audiences"
    SPEND_LIMIT_WINDOWS = "spend_limit_windows"
    # Incremental (re-read over lookback_days)
    AUDIT_LOGS = "audit_logs"
    CAMPAIGN_DAILY = "campaign_daily"
    AD_GROUP_DAILY = "ad_group_daily"
    AD_DAILY = "ad_daily"
    CAMPAIGN_COUNTRY_DAILY = "campaign_country_daily"
    CAMPAIGN_DEVICE_DAILY = "campaign_device_daily"
    CAMPAIGN_PLATFORM_DAILY = "campaign_platform_daily"
    CONVERSION_EVENTS_DAILY = "conversion_events_daily"


LIST_ENDPOINTS = {
    OpenaiAdsStreams.CAMPAIGNS: "/campaigns",
    OpenaiAdsStreams.AD_GROUPS: "/ad_groups",
    OpenaiAdsStreams.ADS: "/ads",
    OpenaiAdsStreams.CONVERSION_EVENT_SETTINGS: "/conversions/event_settings",
    OpenaiAdsStreams.CUSTOM_AUDIENCES: "/custom_audiences",
}

# stream -> (aggregation_level, segment)
INSIGHT_SPECS = {
    OpenaiAdsStreams.CAMPAIGN_DAILY: ("campaign", None),
    OpenaiAdsStreams.AD_GROUP_DAILY: ("ad_group", None),
    OpenaiAdsStreams.AD_DAILY: ("ad", None),
    OpenaiAdsStreams.CAMPAIGN_COUNTRY_DAILY: ("campaign", "country"),
    OpenaiAdsStreams.CAMPAIGN_DEVICE_DAILY: ("campaign", "device"),
    OpenaiAdsStreams.CAMPAIGN_PLATFORM_DAILY: ("campaign", "platform"),
}


class OpenaiAdsSourceConfig(SourceConfig):
    stream: OpenaiAdsStreams
    api_key: str
    start_date: Optional[str] = Field(
        None,
        description="First day of a full (non-incremental) backfill. Required for insights, "
        "conversion_events_daily and audit_logs.",
    )
    end_date: Optional[str] = Field(None, description="Last day to extract; defaults to yesterday (UTC)")
    lookback_days: int = Field(30, description="Days re-read on every incremental run (30-day click attribution)")
    window_days: int = Field(30, description="Days per insights request")
    # Error statuses drive field bisection, 413 range splitting and the 403 fallback, so non-2xx must come back.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )

    @model_validator(mode="after")
    def _dated_streams_need_a_start_date(self):
        is_snapshot = self.stream in LIST_ENDPOINTS or self.stream in (
            OpenaiAdsStreams.AD_ACCOUNT,
            OpenaiAdsStreams.SPEND_LIMIT_WINDOWS,
        )
        if not is_snapshot and not self.start_date:
            raise ValueError(f"start_date is required for the {self.stream.value} stream")
        return self


class OpenaiAdsAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class OpenaiAdsApiError(Exception):
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        super().__init__(f"OpenAI Ads API error {status_code}: {body[:1500]}")


class OpenaiAdsSource(AbstractSource):
    def __init__(self, config: OpenaiAdsSourceConfig):
        super().__init__(config)
        self.config: OpenaiAdsSourceConfig = config
        self.session.headers.update({"Content-Type": "application/json"})
        self._rejected_fields: set = set()

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in OpenaiAdsStreams]

    @staticmethod
    def get_config_class() -> type:
        return OpenaiAdsSourceConfig

    def get_authenticator(self) -> AuthBase:
        return OpenaiAdsAuth(self.config.api_key)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self._request("GET", "/ad_account")
            return True, None
        except Exception as e:
            return False, str(e)

    def _request(self, method: str, path: str, params: Any = None, body: Optional[dict] = None) -> dict:
        response = self.session.request(method, f"{BASE_URL}{path}", params=params, json=body, timeout=(10, 120))
        if response.status_code >= 400:
            raise OpenaiAdsApiError(response.status_code, response.text)
        return response.json() if response.content else {}

    def _list_all(self, path: str, params: Optional[Dict[str, Any]] = None, limit: int = 100) -> List[dict]:
        items: List[dict] = []
        query: Dict[str, Any] = {"limit": limit, **(params or {})}
        for _ in range(10000):
            payload = self._request("GET", path, params=query)
            page = payload.get("data") or []
            items.extend(page)
            if not payload.get("has_more"):
                break
            cursor = payload.get("next_cursor") or payload.get("last_id")
            if not cursor:
                break
            query = {**query, ("cursor" if payload.get("next_cursor") else "after"): cursor}
        return items

    def _end_date(self) -> date:
        if self.config.end_date:
            return date.fromisoformat(self.config.end_date)
        # Conversion metrics need complete days in the account's timezone.
        return datetime.now(timezone.utc).date() - timedelta(days=1)

    def _window(self, start: date, end: date, pagination: dict) -> Tuple[date, date]:
        """This iteration's date window. The cursor stores the window's first day,
        not an index: a resumed run recomputes `end`, so an index would shift."""
        window_start = date.fromisoformat(pagination["window_start"]) if pagination.get("window_start") else start
        days = min(self.config.window_days, MAX_WINDOW_DAYS)
        return window_start, min(end, window_start + timedelta(days=days - 1))

    @staticmethod
    def _date_range(start: date, end: date) -> str:
        return json.dumps({"type": "date_range", "since": start.isoformat(), "until": end.isoformat()})

    @staticmethod
    def _record(stream: str, key: Any, data: dict) -> SourceRecord:
        digest = hashlib.sha1(json.dumps([stream, key], sort_keys=True, default=str).encode()).hexdigest()
        return SourceRecord(id=digest, data=data)

    def _entities(self) -> SourceIteration:
        stream = self.config.stream
        if stream == OpenaiAdsStreams.AD_ACCOUNT:
            items = [self._request("GET", "/ad_account")]
        elif stream == OpenaiAdsStreams.SPEND_LIMIT_WINDOWS:
            try:
                items = [self._request("GET", "/ad_account/spend_limit_windows")]
            except OpenaiAdsApiError as e:
                # Spending limits exist only for postpaid invoice-billed accounts;
                # other accounts get 403.
                if e.status_code != 403:
                    raise
                logger.warning(f"spend_limit_windows unavailable for this ad account: {e}")
                items = []
        else:
            items = self._list_all(LIST_ENDPOINTS[stream])
        records = [self._record(stream.value, item.get("id") or item, item) for item in items]
        logger.info(f"{stream.value}: {len(records)} rows")
        return SourceIteration(records=records, next_pagination={})

    def _audit_logs(self, start: date) -> SourceIteration:
        start_time = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
        items = self._list_all("/audit_logs", {"start_time": start_time, "order": "asc"}, limit=100)
        records = [self._record("audit_logs", item.get("id"), item) for item in items]
        logger.info(f"audit_logs: {len(records)} rows since {start}")
        return SourceIteration(records=records, next_pagination={})

    def _insights_page(self, params: List[Tuple[str, str]], fields: List[str]) -> dict:
        """One insights page; bisects a rejected field list to drop the invalid fields."""
        fields = [f for f in fields if f not in self._rejected_fields]
        try:
            return self._request("GET", "/ad_account/insights", params=params + [("fields[]", f) for f in fields])
        except OpenaiAdsApiError as e:
            if e.status_code != 400 or len(fields) <= 1:
                raise
        bad = self._find_rejected(params, fields)
        if not bad:
            raise OpenaiAdsApiError(400, "insights request rejected with no identifiable field")
        logger.warning(f"OpenAI Ads rejected insight fields {sorted(bad)}; dropping them")
        self._rejected_fields |= bad
        return self._insights_page(params, fields)

    def _find_rejected(self, params: List[Tuple[str, str]], fields: List[str]) -> set:
        probe = [(k, v) for k, v in params if k not in ("limit", "after")] + [("limit", "1")]
        try:
            self._request("GET", "/ad_account/insights", params=probe + [("fields[]", f) for f in fields])
            return set()
        except OpenaiAdsApiError as e:
            if e.status_code != 400:
                raise
        if len(fields) == 1:
            return set(fields)
        middle = len(fields) // 2
        return self._find_rejected(params, fields[:middle]) | self._find_rejected(params, fields[middle:])

    def _insights_iteration(self, start: date, pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream
        level, segment = INSIGHT_SPECS[stream]
        end = self._end_date()
        pagination = pagination or {}
        window_start, window_end = self._window(start, end, pagination)
        if window_start > end:
            return SourceIteration(records=[], next_pagination={})

        prefix = segment or level
        # Segmented insights serve delivery metrics only; per-segment conversions
        # come from POST /conversions/insights (conversion_events_daily).
        metrics = DELIVERY_METRICS + ([] if segment else CONVERSION_METRICS)
        fields = ["metadata.readable_time", "metadata.timezone"] + LEVEL_METADATA[level]
        fields += SEGMENT_METADATA.get(segment, []) + [f"{prefix}.{m}" for m in metrics]
        params: List[Tuple[str, str]] = [
            ("aggregation_level", level),
            ("time_granularity", "daily"),
            ("time_ranges[]", self._date_range(window_start, window_end)),
            ("limit", str(INSIGHTS_PAGE_SIZE)),
        ]
        if segment:
            params.append(("segments[]", segment))
        else:
            params.append(("includes[]", "zero_impression_items"))
        if pagination.get("after"):
            params.append(("after", pagination["after"]))

        payload = self._insights_page(params, fields)
        rows = payload.get("data") or []
        # Row ids end in an opaque ":plan=..." query token; the part before it is
        # the row's grain (time bucket + entity + segment value).
        records = [
            self._record(
                stream.value,
                str(row.get("id", "")).split(":plan=")[0],
                {"window_start": window_start.isoformat(), **row},
            )
            for row in rows
        ]
        if payload.get("has_more") and payload.get("last_id"):
            next_pagination = {"window_start": window_start.isoformat(), "after": payload["last_id"]}
        elif window_end < end:
            next_pagination = {"window_start": (window_end + timedelta(days=1)).isoformat()}
        else:
            next_pagination = {}
        logger.info(f"{stream.value}: {len(records)} rows for {window_start}..{window_end}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _conversion_events(self, start: date, pagination: Optional[dict]) -> SourceIteration:
        """Daily goal conversions per campaign with every attributed standard/custom event."""
        end = self._end_date()
        pagination = pagination or {}
        window_start, window_end = self._window(start, end, pagination)
        campaigns = pagination.get("campaigns")
        if campaigns is None:
            campaigns = sorted(c["id"] for c in self._list_all("/campaigns"))
        if window_start > end or not campaigns:
            return SourceIteration(records=[], next_pagination={})

        records = []
        for i in range(0, len(campaigns), CONVERSION_ENTITY_BATCH):
            records += self._conversion_batch(campaigns[i : i + CONVERSION_ENTITY_BATCH], window_start, window_end)
        next_pagination = (
            {"window_start": (window_end + timedelta(days=1)).isoformat(), "campaigns": campaigns}
            if window_end < end
            else {}
        )
        logger.info(f"conversion_events_daily: {len(records)} rows for {window_start}..{window_end}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _conversion_batch(self, campaign_ids: List[str], start: date, end: date) -> List[SourceRecord]:
        body = {
            "aggregation_level": "campaign",
            "time_ranges": [self._date_range(start, end)],
            "time_granularity": "daily",
            "entity_ids": campaign_ids,
            "include": ["attributed_events"],
            "include_zero_rows": False,
        }
        try:
            payload = self._request("POST", "/conversions/insights", body=body)
        except OpenaiAdsApiError as e:
            # 413 = more than 2,000 summary or event rows; split the date range.
            if e.status_code != 413 or start >= end:
                raise
            middle = start + (end - start) // 2
            return self._conversion_batch(campaign_ids, start, middle) + self._conversion_batch(
                campaign_ids, middle + timedelta(days=1), end
            )
        return [
            self._record(
                "conversion_events_daily",
                [row.get("entity_id"), row.get("date")],
                {"account_currency": payload.get("account_currency"), **row},
            )
            for row in payload.get("data") or []
        ]

    def _start_date(self) -> Optional[date]:
        return date.fromisoformat(self.config.start_date) if self.config.start_date else None

    def _dispatch(self, start: Optional[date], pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream
        if stream in INSIGHT_SPECS:
            return self._insights_iteration(start, pagination)
        if stream == OpenaiAdsStreams.CONVERSION_EVENTS_DAILY:
            return self._conversion_events(start, pagination)
        if stream == OpenaiAdsStreams.AUDIT_LOGS:
            return self._audit_logs(start)
        return self._entities()

    def get(self, pagination: dict = None) -> SourceIteration:
        return self._dispatch(self._start_date(), pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        start = source_state.last_run.date() - timedelta(days=self.config.lookback_days)
        if self._start_date():
            start = max(self._start_date(), start)
        return self._dispatch(start, pagination)
