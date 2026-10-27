"""Google Ads account structure and daily performance, from the Google Ads API (REST, GAQL).

Every run selects all attributes/metrics GoogleAdsFieldService reports for the resource, and drops the fields an
error names as invalid for the account or segments, so new metrics appear without a code change.

Auth is an OAuth refresh token. The developer token is optional, since API access can belong to the OAuth client's
Cloud project.
"""

import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from pydantic import Field, model_validator
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

API_VERSION = "v23"
BASE_URL = f"https://googleads.googleapis.com/{API_VERSION}"
TOKEN_URL = "https://oauth2.googleapis.com/token"

# click_view only serves the last 90 days and only one day per query.
CLICK_VIEW_MAX_DAYS = 89
# change_event only serves the last 30 days and caps a query at 10,000 rows.
CHANGE_EVENT_MAX_DAYS = 29
CHANGE_EVENT_LIMIT = 10000

# Each retry removes at least one field, so this bounds the loop.
MAX_FIELD_PRUNING_ROUNDS = 40


class GoogleAdsStreams(str, Enum):
    CUSTOMER_CLIENTS = "customer_clients"
    CUSTOMERS = "customers"
    CAMPAIGNS = "campaigns"
    CAMPAIGN_BUDGETS = "campaign_budgets"
    BIDDING_STRATEGIES = "bidding_strategies"
    AD_GROUPS = "ad_groups"
    ADS = "ads"
    AD_GROUP_CRITERIA = "ad_group_criteria"
    CAMPAIGN_CRITERIA = "campaign_criteria"
    CONVERSION_ACTIONS = "conversion_actions"
    ASSETS = "assets"
    ASSET_GROUPS = "asset_groups"
    LABELS = "labels"
    USER_LISTS = "user_lists"
    GEO_TARGET_CONSTANTS = "geo_target_constants"
    CUSTOMER_DAILY = "customer_daily"
    CAMPAIGN_DAILY = "campaign_daily"
    AD_GROUP_DAILY = "ad_group_daily"
    AD_DAILY = "ad_daily"
    KEYWORD_DAILY = "keyword_daily"
    SEARCH_TERM_DAILY = "search_term_daily"
    CONVERSION_ACTION_DAILY = "conversion_action_daily"
    GEOGRAPHIC_DAILY = "geographic_daily"
    GEOGRAPHIC_REGION_DAILY = "geographic_region_daily"
    GEOGRAPHIC_METRO_DAILY = "geographic_metro_daily"
    USER_LOCATION_DAILY = "user_location_daily"
    AGE_RANGE_DAILY = "age_range_daily"
    GENDER_DAILY = "gender_daily"
    LANDING_PAGE_DAILY = "landing_page_daily"
    ASSET_GROUP_DAILY = "asset_group_daily"
    AD_GROUP_AUDIENCE_DAILY = "ad_group_audience_daily"
    CAMPAIGN_AUDIENCE_DAILY = "campaign_audience_daily"
    CLICK_VIEW = "click_view"
    CHANGE_EVENTS = "change_events"


# customer.id is added so rows join across accounts.
ENTITY_RESOURCES: Dict[str, str] = {
    GoogleAdsStreams.CUSTOMER_CLIENTS: "customer_client",
    GoogleAdsStreams.CUSTOMERS: "customer",
    GoogleAdsStreams.CAMPAIGNS: "campaign",
    GoogleAdsStreams.CAMPAIGN_BUDGETS: "campaign_budget",
    GoogleAdsStreams.BIDDING_STRATEGIES: "bidding_strategy",
    GoogleAdsStreams.AD_GROUPS: "ad_group",
    GoogleAdsStreams.ADS: "ad_group_ad",
    GoogleAdsStreams.AD_GROUP_CRITERIA: "ad_group_criterion",
    GoogleAdsStreams.CAMPAIGN_CRITERIA: "campaign_criterion",
    GoogleAdsStreams.CONVERSION_ACTIONS: "conversion_action",
    GoogleAdsStreams.ASSETS: "asset",
    GoogleAdsStreams.ASSET_GROUPS: "asset_group",
    GoogleAdsStreams.LABELS: "label",
    GoogleAdsStreams.USER_LISTS: "user_list",
}

# Grain fields are never pruned: when a metric conflicts with a grain segment, the metric goes.
REPORT_SPECS: Dict[str, Tuple[str, List[str]]] = {
    GoogleAdsStreams.CUSTOMER_DAILY: (
        "customer",
        ["customer.id", "customer.descriptive_name", "segments.date", "segments.device", "segments.ad_network_type"],
    ),
    GoogleAdsStreams.CAMPAIGN_DAILY: (
        "campaign",
        [
            "customer.id",
            "campaign.id",
            "campaign.name",
            "campaign.advertising_channel_type",
            "segments.date",
            "segments.device",
            "segments.ad_network_type",
        ],
    ),
    GoogleAdsStreams.AD_GROUP_DAILY: (
        "ad_group",
        [
            "customer.id",
            "campaign.id",
            "ad_group.id",
            "ad_group.name",
            "segments.date",
            "segments.device",
            "segments.ad_network_type",
        ],
    ),
    GoogleAdsStreams.AD_DAILY: (
        "ad_group_ad",
        ["customer.id", "campaign.id", "ad_group.id", "ad_group_ad.ad.id", "segments.date", "segments.device"],
    ),
    GoogleAdsStreams.KEYWORD_DAILY: (
        "keyword_view",
        [
            "customer.id",
            "campaign.id",
            "ad_group.id",
            "ad_group_criterion.criterion_id",
            "ad_group_criterion.keyword.text",
            "ad_group_criterion.keyword.match_type",
            "segments.date",
            "segments.device",
        ],
    ),
    GoogleAdsStreams.SEARCH_TERM_DAILY: (
        "search_term_view",
        [
            "customer.id",
            "campaign.id",
            "ad_group.id",
            "search_term_view.search_term",
            "search_term_view.status",
            "segments.keyword.info.text",
            "segments.keyword.info.match_type",
            "segments.search_term_match_type",
            "segments.date",
        ],
    ),
    GoogleAdsStreams.CONVERSION_ACTION_DAILY: (
        "campaign",
        [
            "customer.id",
            "campaign.id",
            "segments.date",
            "segments.conversion_action",
            "segments.conversion_action_name",
            "segments.conversion_action_category",
        ],
    ),
    GoogleAdsStreams.GEOGRAPHIC_DAILY: (
        "geographic_view",
        [
            "customer.id",
            "campaign.id",
            "geographic_view.country_criterion_id",
            "geographic_view.location_type",
            "segments.geo_target_region",
            "segments.geo_target_metro",
            "segments.geo_target_city",
            "segments.date",
        ],
    ),
    # Each geo segment drops traffic Google cannot place at that level, so region and metro alone keep more
    # spend than the city grain.
    GoogleAdsStreams.GEOGRAPHIC_REGION_DAILY: (
        "geographic_view",
        [
            "customer.id",
            "campaign.id",
            "geographic_view.country_criterion_id",
            "geographic_view.location_type",
            "segments.geo_target_region",
            "segments.date",
        ],
    ),
    GoogleAdsStreams.GEOGRAPHIC_METRO_DAILY: (
        "geographic_view",
        [
            "customer.id",
            "campaign.id",
            "geographic_view.country_criterion_id",
            "geographic_view.location_type",
            "segments.geo_target_metro",
            "segments.date",
        ],
    ),
    GoogleAdsStreams.USER_LOCATION_DAILY: (
        "user_location_view",
        [
            "customer.id",
            "campaign.id",
            "user_location_view.country_criterion_id",
            "user_location_view.targeting_location",
            "segments.date",
        ],
    ),
    GoogleAdsStreams.AGE_RANGE_DAILY: (
        "age_range_view",
        ["customer.id", "campaign.id", "ad_group.id", "ad_group_criterion.age_range.type", "segments.date"],
    ),
    GoogleAdsStreams.GENDER_DAILY: (
        "gender_view",
        ["customer.id", "campaign.id", "ad_group.id", "ad_group_criterion.gender.type", "segments.date"],
    ),
    GoogleAdsStreams.LANDING_PAGE_DAILY: (
        "landing_page_view",
        ["customer.id", "campaign.id", "ad_group.id", "landing_page_view.unexpanded_final_url", "segments.date"],
    ),
    GoogleAdsStreams.ASSET_GROUP_DAILY: (
        "asset_group",
        ["customer.id", "campaign.id", "asset_group.id", "asset_group.name", "segments.date"],
    ),
    GoogleAdsStreams.AD_GROUP_AUDIENCE_DAILY: (
        "ad_group_audience_view",
        ["customer.id", "campaign.id", "ad_group.id", "ad_group_criterion.criterion_id", "segments.date"],
    ),
    GoogleAdsStreams.CAMPAIGN_AUDIENCE_DAILY: (
        "campaign_audience_view",
        ["customer.id", "campaign.id", "campaign_criterion.criterion_id", "segments.date"],
    ),
}

CLICK_VIEW_GRAIN = [
    "customer.id",
    "campaign.id",
    "ad_group.id",
    "segments.date",
    "segments.device",
    "segments.ad_network_type",
    "segments.click_type",
    "segments.slot",
    "metrics.clicks",
]


class GoogleAdsSourceConfig(SourceConfig):
    stream: GoogleAdsStreams
    client_id: str
    client_secret: str
    refresh_token: str
    developer_token: Optional[str] = None
    login_customer_id: str = Field(..., description="Manager (MCC) or account id the token signs in as, no dashes")
    customer_ids: List[str] = Field(
        default_factory=list,
        description="Accounts to extract. Empty = every enabled non-manager account under login_customer_id.",
    )
    start_date: Optional[str] = Field(
        None, description="First day of a full (non-incremental) report backfill. Required for report streams."
    )
    end_date: Optional[str] = Field(None, description="Last day to extract; defaults to today (UTC)")
    lookback_days: int = Field(
        30,
        description="Days re-read on every incremental run. Google restates conversions for the length of "
        "each conversion action's window (up to 90 days for clicks); 30 covers the default.",
    )
    window_days: int = Field(31, description="Days per report query window")
    # Error bodies name the fields to prune, so the session must hand non-2xx responses back.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )

    @model_validator(mode="after")
    def _reports_need_a_start_date(self):
        is_report = self.stream not in ENTITY_RESOURCES and self.stream != GoogleAdsStreams.GEO_TARGET_CONSTANTS
        if is_report and not self.start_date:
            raise ValueError("start_date is required for report streams")
        return self


class GoogleAdsAuth(AuthBase):
    """Bearer auth that refreshes the OAuth access token on demand."""

    def __init__(self, source: "GoogleAdsSource"):
        self.source = source

    def __call__(self, request):
        if request.url.startswith(TOKEN_URL):
            return request
        request.headers["Authorization"] = f"Bearer {self.source.access_token()}"
        if self.source.config.developer_token:
            request.headers["developer-token"] = self.source.config.developer_token
        request.headers["login-customer-id"] = self.source.config.login_customer_id.replace("-", "")
        return request


class GoogleAdsSource(AbstractSource):
    def __init__(self, config: GoogleAdsSourceConfig):
        self._token: Optional[str] = None
        self._token_expires_at = 0.0
        super().__init__(config)
        self.config: GoogleAdsSourceConfig = config
        self._customers: Optional[List[str]] = None
        self._field_cache: Dict[str, Any] = {}

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in GoogleAdsStreams]

    @staticmethod
    def get_config_class() -> type:
        return GoogleAdsSourceConfig

    def get_authenticator(self) -> AuthBase:
        return GoogleAdsAuth(self)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def access_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 120:
            return self._token
        response = self.session.post(
            TOKEN_URL,
            data={
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "refresh_token": self.config.refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=(10, 60),
        )
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/customers:listAccessibleCustomers", timeout=(10, 60))
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def _search(self, customer_id: str, query: str, page_token: Optional[str] = None) -> Tuple[int, dict]:
        body: Dict[str, Any] = {"query": query}
        if page_token:
            body["pageToken"] = page_token
        response = self.session.post(
            f"{BASE_URL}/customers/{customer_id}/googleAds:search", json=body, timeout=(10, 300)
        )
        try:
            payload = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        return response.status_code, payload

    def _fields_query(self, query: str) -> List[dict]:
        key = "q:" + query
        if key not in self._field_cache:
            results: List[dict] = []
            page_token = None
            while True:
                body: Dict[str, Any] = {"query": query}
                if page_token:
                    body["pageToken"] = page_token
                response = self.session.post(f"{BASE_URL}/googleAdsFields:search", json=body, timeout=(10, 120))
                response.raise_for_status()
                payload = response.json()
                results.extend(payload.get("results", []))
                page_token = payload.get("nextPageToken")
                if not page_token:
                    break
            self._field_cache[key] = results
        return self._field_cache[key]

    def _resource_metrics(self, resource: str) -> List[str]:
        rows = self._fields_query(f"SELECT name, metrics WHERE name = '{resource}'")
        return list(rows[0].get("metrics", [])) if rows else []

    def _resource_attributes(self, resource: str) -> List[str]:
        rows = self._fields_query(
            f"SELECT name WHERE name LIKE '{resource}.%' AND selectable = true AND category = 'ATTRIBUTE'"
        )
        return sorted(r["name"] for r in rows)

    @staticmethod
    def _rejected_fields(payload: dict, selected: List[str], protected: List[str]) -> List[str]:
        """Fields come from `location.fieldPathElements` or bare names after "metrics:" in the message
        (e.g. METRIC_ACCESS_DENIED). Protected (grain) fields are never returned."""
        rejected: List[str] = []
        details = (payload.get("error") or {}).get("details") or [{}]
        for error in details[0].get("errors", []):
            path = ".".join(
                el.get("fieldName", "") for el in (error.get("location") or {}).get("fieldPathElements", [])
            )
            if path in selected and path not in protected:
                rejected.append(path)
                continue
            message = error.get("message", "")
            listed = re.search(r"\b(metrics|segments|fields): (.*)", message)
            if listed:
                prefix = {"metrics": "metrics.", "segments": "segments."}.get(listed.group(1), "")
                for name in re.findall(r"'([\w.]+)'", listed.group(2)):
                    full = name if "." in name else prefix + name
                    if full in selected and full not in protected:
                        rejected.append(full)
                if rejected:
                    continue
            for field in selected:
                if field not in protected and re.search(r"(?<![\w.])" + re.escape(field) + r"(?![\w.])", message):
                    rejected.append(field)
        return sorted(set(rejected))

    def _run_query(
        self,
        customer_id: str,
        resource: str,
        select: List[str],
        protected: List[str],
        where: str,
        page_token: Optional[str],
        suffix: str = "",
    ) -> Tuple[List[str], dict]:
        select = list(select)
        for _ in range(MAX_FIELD_PRUNING_ROUNDS):
            query = f"SELECT {', '.join(select)} FROM {resource} {where} {suffix}".strip()
            status, payload = self._search(customer_id, query, page_token)
            if status == 200:
                return select, payload
            rejected = self._rejected_fields(payload, select, protected)
            if not rejected:
                raise RuntimeError(f"Google Ads query failed ({status}) for {resource}: {json.dumps(payload)[:2000]}")
            # Expected on every run: some metrics never apply to this account or grain.
            logger.info(f"{resource}: dropping fields the API rejected: {rejected}")
            select = [f for f in select if f not in rejected]
        raise RuntimeError(f"Google Ads query for {resource} still failing after field pruning")

    def _customer_ids(self) -> List[str]:
        if self._customers is None:
            if self.config.customer_ids:
                self._customers = [c.replace("-", "") for c in self.config.customer_ids]
            else:
                login = self.config.login_customer_id.replace("-", "")
                _, payload = self._run_query(
                    login,
                    "customer_client",
                    ["customer_client.id", "customer_client.manager", "customer_client.status"],
                    [],
                    "WHERE customer_client.manager = false AND customer_client.status = 'ENABLED'",
                    None,
                )
                self._customers = sorted(str(r["customerClient"]["id"]) for r in payload.get("results", [])) or [login]
            logger.info(f"Google Ads accounts: {self._customers}")
        return self._customers

    @staticmethod
    def _record_id(stream: str, customer_id: str, row: dict) -> str:
        """Every resource name in the row plus its segments: the row's grain."""
        key = {k: v.get("resourceName") for k, v in row.items() if isinstance(v, dict) and "resourceName" in v}
        key["segments"] = row.get("segments")
        key["_stream"] = stream
        key["_customer"] = customer_id
        return hashlib.sha1(json.dumps(key, sort_keys=True, default=str).encode()).hexdigest()

    def _end_date(self) -> date:
        if self.config.end_date:
            return date.fromisoformat(self.config.end_date)
        return datetime.now(timezone.utc).date()

    def _entity_iteration(self, pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream
        resource = ENTITY_RESOURCES[stream]
        pagination = pagination or {}
        customers = self._customer_ids()
        idx = pagination.get("customer_idx", 0)
        if idx >= len(customers):
            return SourceIteration(records=[], next_pagination={})
        customer_id = customers[idx]

        select = pagination.get("select")
        if not select:
            select = self._resource_attributes(resource)
            if resource != "customer":
                select = ["customer.id"] + select
        select, payload = self._run_query(
            customer_id, resource, select, ["customer.id"], "", pagination.get("page_token")
        )

        records = [
            SourceRecord(id=self._record_id(stream, customer_id, row), data={"customer_id": customer_id, **row})
            for row in payload.get("results", [])
        ]
        next_token = payload.get("nextPageToken")
        if next_token:
            next_pagination = {"customer_idx": idx, "page_token": next_token, "select": select}
        elif idx + 1 < len(customers):
            next_pagination = {"customer_idx": idx + 1}
        else:
            next_pagination = {}
        logger.info(f"{stream.value}: {len(records)} rows for customer {customer_id}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _geo_target_constant_iteration(self, pagination: Optional[dict]) -> SourceIteration:
        """The catalogue is Google's, not an account's: read once via the login account, without customer.id,
        which geo_target_constant cannot select."""
        stream = self.config.stream
        pagination = pagination or {}
        login = self.config.login_customer_id.replace("-", "")
        select = pagination.get("select") or self._resource_attributes("geo_target_constant")
        select, payload = self._run_query(login, "geo_target_constant", select, [], "", pagination.get("page_token"))

        records = [SourceRecord(id=self._record_id(stream, login, row), data=row) for row in payload.get("results", [])]
        next_token = payload.get("nextPageToken")
        next_pagination = {"page_token": next_token, "select": select} if next_token else {}
        logger.info(f"{stream.value}: {len(records)} rows")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _report_iteration(self, start: date, pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream
        end = self._end_date()
        pagination = pagination or {}

        if stream == GoogleAdsStreams.CLICK_VIEW:
            start = max(start, end - timedelta(days=CLICK_VIEW_MAX_DAYS))
            window_days = 1
        elif stream == GoogleAdsStreams.CHANGE_EVENTS:
            start = max(start, end - timedelta(days=CHANGE_EVENT_MAX_DAYS))
            window_days = 1
        else:
            window_days = self.config.window_days

        if start > end:
            logger.info(f"{stream.value}: nothing to extract ({start} > {end})")
            return SourceIteration(records=[], next_pagination={})

        customers = self._customer_ids()
        customer_idx = pagination.get("customer_idx", 0)
        # A resumed run recomputes `end` from today, so the cursor stores a date, not an index.
        window_start = date.fromisoformat(pagination["window_start"]) if pagination.get("window_start") else start
        if customer_idx >= len(customers) or window_start > end:
            return SourceIteration(records=[], next_pagination={})
        customer_id = customers[customer_idx]
        window_end = min(end, window_start + timedelta(days=window_days - 1))

        if stream == GoogleAdsStreams.CLICK_VIEW:
            resource = "click_view"
            protected = CLICK_VIEW_GRAIN
            default_select = CLICK_VIEW_GRAIN + self._resource_attributes("click_view")
            where = f"WHERE segments.date = '{window_start.isoformat()}'"
            suffix = ""
        elif stream == GoogleAdsStreams.CHANGE_EVENTS:
            resource = "change_event"
            protected = ["customer.id", "change_event.change_date_time"]
            default_select = ["customer.id"] + self._resource_attributes("change_event")
            next_day = window_start + timedelta(days=1)
            where = (
                f"WHERE change_event.change_date_time >= '{window_start.isoformat()}' "
                f"AND change_event.change_date_time < '{next_day.isoformat()}'"
            )
            suffix = f"ORDER BY change_event.change_date_time LIMIT {CHANGE_EVENT_LIMIT}"
        else:
            resource, grain = REPORT_SPECS[stream]
            protected = grain
            default_select = grain + [m for m in self._resource_metrics(resource) if m not in grain]
            where = f"WHERE segments.date BETWEEN '{window_start.isoformat()}' AND '{window_end.isoformat()}'"
            suffix = ""

        select = pagination.get("select") or default_select
        select, payload = self._run_query(
            customer_id, resource, select, protected, where, pagination.get("page_token"), suffix
        )
        rows = payload.get("results", [])
        if stream == GoogleAdsStreams.CHANGE_EVENTS and len(rows) >= CHANGE_EVENT_LIMIT:
            logger.warning(
                f"change_events hit the {CHANGE_EVENT_LIMIT}-row cap on {window_start}; later changes that day are dropped"
            )

        records = [
            SourceRecord(
                id=self._record_id(stream, customer_id, row),
                data={
                    "customer_id": customer_id,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    **row,
                },
            )
            for row in rows
        ]

        next_token = payload.get("nextPageToken")
        if next_token:
            next_pagination = {
                "customer_idx": customer_idx,
                "window_start": window_start.isoformat(),
                "page_token": next_token,
                "select": select,
            }
        elif window_end < end:
            # Keep the pruned select for the next window of the same account.
            next_pagination = {
                "customer_idx": customer_idx,
                "window_start": (window_end + timedelta(days=1)).isoformat(),
                "select": select,
            }
        elif customer_idx + 1 < len(customers):
            next_pagination = {"customer_idx": customer_idx + 1}
        else:
            next_pagination = {}
        logger.info(f"{stream.value}: {len(records)} rows for {customer_id} {window_start}..{window_end}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == GoogleAdsStreams.GEO_TARGET_CONSTANTS:
            return self._geo_target_constant_iteration(pagination)
        if self.config.stream in ENTITY_RESOURCES:
            return self._entity_iteration(pagination)
        return self._report_iteration(date.fromisoformat(self.config.start_date), pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        if self.config.stream == GoogleAdsStreams.GEO_TARGET_CONSTANTS:
            return self._geo_target_constant_iteration(pagination)
        if self.config.stream in ENTITY_RESOURCES:
            return self._entity_iteration(pagination)
        start = max(
            date.fromisoformat(self.config.start_date),
            source_state.last_run.date() - timedelta(days=self.config.lookback_days),
        )
        return self._report_iteration(start, pagination)
