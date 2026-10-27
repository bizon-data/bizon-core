"""LinkedIn ad accounts, campaign structure, conversions, lead forms and daily analytics, from the LinkedIn
Marketing API (REST.li, versioned `/rest` endpoints).

Each run exchanges an OAuth refresh token for a fresh access token. The app needs r_ads and r_ads_reporting.

Analytics notes (LinkedIn docs, Reporting, version 202609):
- adAnalytics has no pagination and caps a response at 15,000 elements, so the
  source halves a date window that comes back full.
- A request may name at most 20 fields; every metric is requested in chunks of
  18 (+ dateRange, pivotValues) and the chunks are merged back on pivot + date.
  A chunk LinkedIn rejects is split until the offending metric is isolated and
  dropped, so a metric invalid for one pivot does not fail the stream.
- Professional demographic pivots (MEMBER_*) drop values with fewer than 3
  events, keep the top 100 values per creative per day, are approximate, and
  are retained for two years. MEMBER_COMPANY by campaign by day is the closest
  available proxy for company-level ad engagement; the full company feed
  (Company Intelligence API) is restricted to certified partners.
"""

import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from loguru import logger
from pydantic import Field, model_validator
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

API_BASE = "https://api.linkedin.com"
TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
DEFAULT_API_VERSION = "202609"

MAX_FIELDS_PER_REQUEST = 20
METRICS_PER_CHUNK = MAX_FIELDS_PER_REQUEST - 2  # leave room for dateRange + pivotValues
# Documented as 15,000, but full responses stop at 14,999, so callers treat MAX_ELEMENTS - 1 as "full".
MAX_ELEMENTS = 15000
TARGETING_ENTITY_BATCH = 50

# Every adAnalytics metric in the Reporting schema except revenueAttributionMetrics
# (its own finder, see ATTRIBUTED_REVENUE) and the two projection fields.
# fmt: off
ALL_METRICS = [
    "actionClicks", "adUnitClicks", "approximateMemberReach", "averageDwellTime", "audiencePenetration",
    "cardClicks", "cardImpressions", "clicks", "commentLikes", "comments", "companyPageClicks",
    "conversionValueInLocalCurrency", "costInLocalCurrency", "costInUsd", "costPerQualifiedLead",
    "documentCompletions", "documentFirstQuartileCompletions", "documentMidpointCompletions",
    "documentThirdQuartileCompletions", "downloadClicks", "externalWebsiteConversions",
    "externalWebsitePostClickConversions", "externalWebsitePostViewConversions", "follows", "fullScreenPlays",
    "headlineClicks", "headlineImpressions", "impressions", "jobApplications", "jobApplyClicks",
    "landingPageClicks", "leadGenerationMailContactInfoShares", "leadGenerationMailInterestedClicks", "likes",
    "oneClickLeadFormOpens", "oneClickLeads", "opens", "otherEngagements", "postClickJobApplications",
    "postClickJobApplyClicks", "postClickRegistrations", "postViewJobApplications", "postViewJobApplyClicks",
    "postViewRegistrations", "qualifiedLeads", "reactions", "registrations", "sends", "shares",
    "subscriptionClicks", "talentLeads", "textUrlClicks", "totalEngagements", "validWorkEmailLeads",
    "videoCompletions", "videoFirstQuartileCompletions", "videoMidpointCompletions", "videoStarts",
    "videoThirdQuartileCompletions", "videoViews", "viralCardClicks", "viralCardImpressions", "viralClicks",
    "viralCommentLikes", "viralComments", "viralCompanyPageClicks", "viralDocumentCompletions",
    "viralDocumentFirstQuartileCompletions", "viralDocumentMidpointCompletions",
    "viralDocumentThirdQuartileCompletions", "viralDownloadClicks", "viralExternalWebsiteConversions",
    "viralExternalWebsitePostClickConversions", "viralExternalWebsitePostViewConversions", "viralFollows",
    "viralFullScreenPlays", "viralImpressions", "viralJobApplications", "viralJobApplyClicks",
    "viralLandingPageClicks", "viralLikes", "viralOneClickLeadFormOpens", "viralOneClickLeads",
    "viralOtherEngagements", "viralPostClickJobApplications", "viralPostClickJobApplyClicks",
    "viralPostClickRegistrations", "viralPostViewJobApplications", "viralPostViewJobApplyClicks",
    "viralPostViewRegistrations", "viralReactions", "viralRegistrations", "viralShares",
    "viralSubscriptionClicks", "viralTotalEngagements", "viralVideoCompletions",
    "viralVideoFirstQuartileCompletions", "viralVideoMidpointCompletions", "viralVideoStarts",
    "viralVideoThirdQuartileCompletions", "viralVideoViews", "appointmentsScheduled", "averageEventWatchTime",
    "averageEventWatchTimeOver15Seconds", "averageEventWatchTimeOver2Minutes",
    "averageEventWatchTimeOver30Seconds", "averageVideoWatchTime", "costPerEventView",
    "costPerEventViewOver15Seconds", "costPerEventViewOver2Minutes", "costPerEventViewOver30Seconds",
    "eventViews", "eventViewsOver15Seconds", "eventViewsOver2Minutes", "eventViewsOver30Seconds",
    "eventWatchTime", "memberFollows", "memberProfileClicks", "videoWatchTime",
]

# Not returned for MEMBER_* pivots (Reporting docs, "Professional Demographic Pivot
# Fields Restrictions"), plus carousel metrics, which demographic pivots don't support.
NON_DEMOGRAPHIC_METRICS = {
    "conversionValueInLocalCurrency", "cardImpressions", "cardClicks", "viralCardImpressions",
    "viralCardClicks", "approximateMemberReach",
}
DEMOGRAPHIC_METRICS = [m for m in ALL_METRICS if m not in NON_DEMOGRAPHIC_METRICS]

CONVERSION_METRICS = [
    "externalWebsiteConversions", "externalWebsitePostClickConversions", "externalWebsitePostViewConversions",
    "conversionValueInLocalCurrency", "viralExternalWebsiteConversions",
    "viralExternalWebsitePostClickConversions", "viralExternalWebsitePostViewConversions", "costInUsd",
    "costInLocalCurrency",
]

DEMOGRAPHIC_PIVOTS = [
    "MEMBER_COMPANY_SIZE", "MEMBER_INDUSTRY", "MEMBER_SENIORITY", "MEMBER_JOB_TITLE", "MEMBER_JOB_FUNCTION",
    "MEMBER_COUNTRY_V2", "MEMBER_REGION_V2",
]
# fmt: on


class LinkedinAdsStreams(str, Enum):
    # Entity snapshots (full refresh)
    AD_ACCOUNTS = "ad_accounts"
    CAMPAIGN_GROUPS = "campaign_groups"
    CAMPAIGNS = "campaigns"
    CREATIVES = "creatives"
    CONVERSIONS = "conversions"
    LEAD_FORMS = "lead_forms"
    ORGANIZATIONS = "organizations"
    # Daily analytics (incremental, re-read over lookback_days)
    ACCOUNT_DAILY = "account_daily"
    CAMPAIGN_GROUP_DAILY = "campaign_group_daily"
    CAMPAIGN_DAILY = "campaign_daily"
    CREATIVE_DAILY = "creative_daily"
    CONVERSION_DAILY = "conversion_daily"
    MEMBER_COMPANY_CAMPAIGN_DAILY = "member_company_campaign_daily"
    MEMBER_DEMOGRAPHICS_DAILY = "member_demographics_daily"
    ATTRIBUTED_REVENUE = "attributed_revenue"


PERFORMANCE_PIVOTS = {
    LinkedinAdsStreams.ACCOUNT_DAILY: "ACCOUNT",
    LinkedinAdsStreams.CAMPAIGN_GROUP_DAILY: "CAMPAIGN_GROUP",
    LinkedinAdsStreams.CAMPAIGN_DAILY: "CAMPAIGN",
    LinkedinAdsStreams.CREATIVE_DAILY: "CREATIVE",
}

ENTITY_STREAMS = {
    LinkedinAdsStreams.AD_ACCOUNTS,
    LinkedinAdsStreams.CAMPAIGN_GROUPS,
    LinkedinAdsStreams.CAMPAIGNS,
    LinkedinAdsStreams.CREATIVES,
    LinkedinAdsStreams.CONVERSIONS,
    LinkedinAdsStreams.LEAD_FORMS,
    LinkedinAdsStreams.ORGANIZATIONS,
}


class LinkedinAdsSourceConfig(SourceConfig):
    stream: LinkedinAdsStreams
    client_id: str
    client_secret: str
    refresh_token: str
    api_version: str = DEFAULT_API_VERSION
    account_ids: List[str] = Field(default_factory=list, description="Empty = every non-test account the token sees")
    start_date: Optional[str] = Field(
        None, description="First day of a full (non-incremental) analytics backfill. Required for daily streams."
    )
    end_date: Optional[str] = Field(None, description="Last day to extract; defaults to today (UTC)")
    lookback_days: int = Field(30, description="Days re-read on every incremental run (conversion restatements)")
    window_days: int = Field(30, description="Days per analytics request; halved automatically when a response is full")
    demographic_max_days: int = Field(
        700, description="Oldest day requested for MEMBER_* pivots (LinkedIn keeps demographics for two years)"
    )
    organization_lookback_days: int = Field(365, description="Window used to collect MEMBER_COMPANY urns to resolve")
    # Error statuses drive metric isolation and the attributed_revenue fallback, so non-2xx must come back.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )

    @model_validator(mode="after")
    def _analytics_need_a_start_date(self):
        is_analytics = self.stream not in ENTITY_STREAMS and self.stream != LinkedinAdsStreams.ATTRIBUTED_REVENUE
        if is_analytics and not self.start_date:
            raise ValueError("start_date is required for the daily analytics streams")
        return self


class LinkedinAdsAuth(AuthBase):
    def __init__(self, source: "LinkedinAdsSource"):
        self.source = source

    def __call__(self, request):
        if request.url.startswith(TOKEN_URL):
            return request
        request.headers["Authorization"] = f"Bearer {self.source.access_token()}"
        request.headers["LinkedIn-Version"] = self.source.config.api_version
        request.headers["X-Restli-Protocol-Version"] = "2.0.0"
        return request


def build_query_string(params: Dict[str, Any]) -> str:
    """REST.li query encoding.

    Lists become List(...) with url-encoded members; `fields`, `dateRange` and
    `owner` carry REST.li syntax and pass through raw; everything else is
    url-encoded.
    """
    parts = []
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, list):
            parts.append(f"{key}=List({','.join(quote(str(v), safe='') for v in value)})")
        elif key in ("fields", "dateRange", "owner"):
            parts.append(f"{key}={value}")
        else:
            parts.append(f"{key}={quote(str(value), safe='')}")
    return "?" + "&".join(parts) if parts else ""


def date_range_literal(start: date, end: date) -> str:
    return (
        f"(start:(year:{start.year},month:{start.month},day:{start.day}),"
        f"end:(year:{end.year},month:{end.month},day:{end.day}))"
    )


def urn(kind: str, value: str) -> str:
    return value if str(value).startswith("urn:") else f"urn:li:{kind}:{value}"


def element_date(element: dict) -> Optional[str]:
    start = (element.get("dateRange") or {}).get("start")
    if not start:
        return None
    return date(start["year"], start["month"], start["day"]).isoformat()


class LinkedinApiError(Exception):
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        super().__init__(f"LinkedIn API error {status_code}: {body[:1500]}")


class LinkedinAdsSource(AbstractSource):
    def __init__(self, config: LinkedinAdsSourceConfig):
        self._token: Optional[str] = None
        self._token_expires_at = 0.0
        super().__init__(config)
        self.config: LinkedinAdsSourceConfig = config
        self._accounts: Optional[List[str]] = None
        self._rejected_metrics: Dict[str, set] = {}

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in LinkedinAdsStreams]

    @staticmethod
    def get_config_class() -> type:
        return LinkedinAdsSourceConfig

    def get_authenticator(self) -> AuthBase:
        return LinkedinAdsAuth(self)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def access_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 300:
            return self._token
        response = self.session.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.config.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
            timeout=(10, 60),
        )
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, restli_method: Optional[str] = None) -> dict:
        headers = {"X-RestLi-Method": restli_method} if restli_method else {}
        response = self.session.get(
            f"{API_BASE}{path}{build_query_string(params or {})}", headers=headers, timeout=(10, 120)
        )
        if response.status_code >= 400:
            raise LinkedinApiError(response.status_code, response.text)
        return response.json() if response.content else {}

    def _get_all(self, path: str, params: Dict[str, Any], restli_method: Optional[str] = None) -> List[dict]:
        """Follow cursor (pageToken) or offset (start/count) pagination."""
        elements: List[dict] = []
        params = dict(params)
        seen = set()
        for _ in range(1000):
            payload = self._get(path, params, restli_method)
            page = payload.get("elements") or []
            elements.extend(page)
            token = (payload.get("metadata") or {}).get("nextPageToken")
            if token:
                if token in seen:
                    break
                seen.add(token)
                params["pageToken"] = token
                continue
            paging = payload.get("paging") or {}
            if any(link.get("rel") == "next" for link in paging.get("links") or []) and page:
                params["start"] = int(paging.get("start", 0)) + len(page)
                params["count"] = int(paging.get("count", len(page)) or len(page))
                continue
            break
        return elements

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self._get("/rest/adAccounts", {"q": "search", "pageSize": 1})
            return True, None
        except Exception as e:
            return False, str(e)

    def _account_ids(self) -> List[str]:
        if self._accounts is None:
            if self.config.account_ids:
                self._accounts = [str(a) for a in self.config.account_ids]
            else:
                accounts = self._get_all("/rest/adAccounts", {"q": "search"})
                self._accounts = sorted(str(a["id"]) for a in accounts if not a.get("test"))
            logger.info(f"LinkedIn ad accounts: {self._accounts}")
        return self._accounts

    def _entity_records(self, account_id: str) -> List[dict]:
        stream = self.config.stream
        account_urn = urn("sponsoredAccount", account_id)
        if stream == LinkedinAdsStreams.AD_ACCOUNTS:
            return [self._get(f"/rest/adAccounts/{account_id}")]
        if stream == LinkedinAdsStreams.CAMPAIGN_GROUPS:
            return self._get_all(f"/rest/adAccounts/{account_id}/adCampaignGroups", {"q": "search"})
        if stream == LinkedinAdsStreams.CAMPAIGNS:
            return self._get_all(f"/rest/adAccounts/{account_id}/adCampaigns", {"q": "search"})
        if stream == LinkedinAdsStreams.CREATIVES:
            return self._get_all(
                f"/rest/adAccounts/{account_id}/creatives", {"q": "criteria", "pageSize": 100}, restli_method="FINDER"
            )
        if stream == LinkedinAdsStreams.CONVERSIONS:
            return self._get_all("/rest/conversions", {"q": "account", "account": account_urn})
        if stream == LinkedinAdsStreams.LEAD_FORMS:
            return self._get_all(
                "/rest/leadForms", {"q": "owner", "owner": f"(sponsoredAccount:{quote(account_urn, safe='')})"}
            )
        if stream == LinkedinAdsStreams.ORGANIZATIONS:
            return self._organizations(account_id)
        raise NotImplementedError(stream)

    def _organizations(self, account_id: str) -> List[dict]:
        """Resolve the MEMBER_COMPANY urns seen in the last organization_lookback_days to names."""
        end = self._end_date()
        start = end - timedelta(days=self.config.organization_lookback_days)
        urns = sorted(self._member_company_urns(urn("sponsoredAccount", account_id), start, end))
        resolved: List[dict] = []
        for i in range(0, len(urns), TARGETING_ENTITY_BATCH):
            batch = urns[i : i + TARGETING_ENTITY_BATCH]
            payload = self._get("/rest/adTargetingEntities", {"q": "urns", "urns": batch})
            found = {e.get("urn"): e for e in payload.get("elements") or []}
            resolved.extend(found.get(u) or {"urn": u} for u in batch)
        logger.info(f"organizations: resolved {len(resolved)} member-company urns for account {account_id}")
        return resolved

    def _member_company_urns(self, account_urn: str, start: date, end: date) -> set:
        """Organization urns with MEMBER_COMPANY activity in [start, end].

        One ALL-granularity request returns at most MAX_ELEMENTS companies, which a year of activity for a large
        advertiser can exceed, so a full response is split into two halves until each half fits.
        """
        params = {
            "q": "analytics",
            "pivot": "MEMBER_COMPANY",
            "accounts": [account_urn],
            "timeGranularity": "ALL",
            "dateRange": date_range_literal(start, end),
            "fields": "impressions,pivotValues",
        }
        elements = self._get("/rest/adAnalytics", params).get("elements") or []
        if len(elements) >= MAX_ELEMENTS - 1 and start < end:
            middle = start + (end - start) // 2
            return self._member_company_urns(account_urn, start, middle) | self._member_company_urns(
                account_urn, middle + timedelta(days=1), end
            )
        if len(elements) >= MAX_ELEMENTS - 1:
            logger.warning(
                f"MEMBER_COMPANY returned the element cap for the single day {start}; some companies are missing"
            )
        return {v for e in elements for v in e.get("pivotValues") or [] if v.startswith("urn:li:organization:")}

    def _entity_iteration(self, pagination: Optional[dict]) -> SourceIteration:
        accounts = self._account_ids()
        idx = (pagination or {}).get("account_idx", 0)
        if idx >= len(accounts):
            return SourceIteration(records=[], next_pagination={})
        account_id = accounts[idx]
        records = []
        for element in self._entity_records(account_id):
            key = (
                element.get("id")
                or element.get("urn")
                or hashlib.sha1(json.dumps(element, sort_keys=True).encode()).hexdigest()
            )
            records.append(SourceRecord(id=f"{account_id}|{key}", data={"account_id": account_id, **element}))
        logger.info(f"{self.config.stream.value}: {len(records)} rows for account {account_id}")
        next_pagination = {"account_idx": idx + 1} if idx + 1 < len(accounts) else {}
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _request_chunk(
        self, base: Dict[str, Any], metrics: List[str], start: date, end: date, granularity: str
    ) -> List[dict]:
        """One adAnalytics call; halves the window if the 15,000-element cap is hit."""
        params = dict(base)
        params["timeGranularity"] = granularity
        params["dateRange"] = date_range_literal(start, end)
        params["fields"] = ",".join(metrics + ["pivotValues"] + (["dateRange"] if granularity != "ALL" else []))
        elements = self._get("/rest/adAnalytics", params).get("elements") or []
        if len(elements) >= MAX_ELEMENTS - 1 and start < end and granularity != "ALL":
            middle = start + (end - start) // 2
            logger.warning(f"adAnalytics returned {len(elements)} elements for {start}..{end}; splitting the window")
            return self._request_chunk(base, metrics, start, middle, granularity) + self._request_chunk(
                base, metrics, middle + timedelta(days=1), end, granularity
            )
        if len(elements) >= MAX_ELEMENTS - 1:
            logger.warning(
                f"adAnalytics returned the {MAX_ELEMENTS}-element cap for {start}..{end}; rows may be missing"
            )
        return elements

    def _request_metrics(
        self, base: Dict[str, Any], metrics: List[str], start: date, end: date, granularity: str
    ) -> List[dict]:
        """Request a metric chunk, splitting it to isolate and drop metrics LinkedIn rejects."""
        cache_key = json.dumps({k: base[k] for k in sorted(base) if k in ("q", "pivot", "pivots")}, default=str)
        rejected = self._rejected_metrics.setdefault(cache_key, set())
        metrics = [m for m in metrics if m not in rejected]
        if not metrics:
            return []
        try:
            return self._request_chunk(base, metrics, start, end, granularity)
        except LinkedinApiError as e:
            if e.status_code != 400:
                raise
            if len(metrics) == 1:
                # A 400 can also mean a bad date range or facet. Only blame the
                # metric if the same query succeeds with impressions alone.
                if metrics[0] != "impressions":
                    self._request_chunk(base, ["impressions"], start, end, granularity)
                logger.warning(f"LinkedIn rejected metric {metrics[0]} for {cache_key}; dropping it: {e}")
                rejected.add(metrics[0])
                return []
            middle = len(metrics) // 2
            return self._request_metrics(base, metrics[:middle], start, end, granularity) + self._request_metrics(
                base, metrics[middle:], start, end, granularity
            )

    def _analytics(
        self, base: Dict[str, Any], metrics: List[str], start: date, end: date, granularity: str
    ) -> List[dict]:
        """All metrics for one query, chunked to the 20-field limit and merged on pivot + date."""
        merged: Dict[str, dict] = {}
        for i in range(0, len(metrics), METRICS_PER_CHUNK):
            for element in self._request_metrics(base, metrics[i : i + METRICS_PER_CHUNK], start, end, granularity):
                key = json.dumps([element.get("pivotValues"), element.get("dateRange")], sort_keys=True)
                merged.setdefault(key, {}).update(element)
        return list(merged.values())

    def _end_date(self) -> date:
        if self.config.end_date:
            return date.fromisoformat(self.config.end_date)
        return datetime.now(timezone.utc).date()

    def _work_units(self, account_id: str, window: Tuple[date, date]) -> List[Dict[str, Any]]:
        """The adAnalytics queries one (account, window) needs for this stream."""
        stream = self.config.stream
        account_urn = urn("sponsoredAccount", account_id)
        if stream in PERFORMANCE_PIVOTS:
            return [{"q": "analytics", "pivot": PERFORMANCE_PIVOTS[stream], "accounts": [account_urn]}]
        if stream == LinkedinAdsStreams.CONVERSION_DAILY:
            return [{"q": "statistics", "pivots": ["CAMPAIGN", "CONVERSION"], "accounts": [account_urn]}]
        if stream == LinkedinAdsStreams.MEMBER_DEMOGRAPHICS_DAILY:
            return [{"q": "analytics", "pivot": pivot, "accounts": [account_urn]} for pivot in DEMOGRAPHIC_PIVOTS]
        if stream == LinkedinAdsStreams.MEMBER_COMPANY_CAMPAIGN_DAILY:
            # MEMBER_* pivots can't be combined with CAMPAIGN in one request, so
            # facet by campaign: one query per campaign that served in the window.
            served = self._analytics(
                {"q": "analytics", "pivot": "CAMPAIGN", "accounts": [account_urn]},
                ["impressions"],
                window[0],
                window[1],
                "ALL",
            )
            campaigns = sorted({v for e in served if e.get("impressions") for v in e.get("pivotValues") or []})
            return [{"q": "analytics", "pivot": "MEMBER_COMPANY", "campaigns": [c]} for c in campaigns]
        raise NotImplementedError(stream)

    def _metrics_for(self, unit: Dict[str, Any]) -> List[str]:
        if self.config.stream == LinkedinAdsStreams.CONVERSION_DAILY:
            return CONVERSION_METRICS
        if str(unit.get("pivot", "")).startswith("MEMBER_"):
            return DEMOGRAPHIC_METRICS
        return ALL_METRICS

    def _analytics_iteration(self, start: date, pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream
        end = self._end_date()
        if stream in (LinkedinAdsStreams.MEMBER_DEMOGRAPHICS_DAILY, LinkedinAdsStreams.MEMBER_COMPANY_CAMPAIGN_DAILY):
            start = max(start, end - timedelta(days=self.config.demographic_max_days))
        accounts = self._account_ids()
        pagination = pagination or {}
        account_idx = pagination.get("account_idx", 0)
        # The cursor stores the window's first day, not an index: a resumed run
        # recomputes `end` from today, so an index would point at a different window.
        window_start = date.fromisoformat(pagination["window_start"]) if pagination.get("window_start") else start
        if account_idx >= len(accounts) or window_start > end:
            return SourceIteration(records=[], next_pagination={})
        account_id = accounts[account_idx]
        window = (window_start, min(end, window_start + timedelta(days=self.config.window_days - 1)))

        units = pagination.get("units")
        if units is None:
            units = self._work_units(account_id, window)
        unit_idx = pagination.get("unit_idx", 0)

        records = []
        if unit_idx < len(units):
            unit = units[unit_idx]
            for element in self._analytics(unit, self._metrics_for(unit), window[0], window[1], "DAILY"):
                data = {
                    "account_id": account_id,
                    "date": element_date(element),
                    "pivot": unit.get("pivot") or ",".join(unit.get("pivots", [])),
                    "campaign_facet": (unit.get("campaigns") or [None])[0],
                    **element,
                }
                key = json.dumps(
                    [
                        stream.value,
                        account_id,
                        data["pivot"],
                        data["campaign_facet"],
                        element.get("pivotValues"),
                        data["date"],
                    ]
                )
                records.append(SourceRecord(id=hashlib.sha1(key.encode()).hexdigest(), data=data))

        if unit_idx + 1 < len(units):
            next_pagination = {
                "account_idx": account_idx,
                "window_start": window[0].isoformat(),
                "units": units,
                "unit_idx": unit_idx + 1,
            }
        elif window[1] < end:
            next_pagination = {"account_idx": account_idx, "window_start": (window[1] + timedelta(days=1)).isoformat()}
        elif account_idx + 1 < len(accounts):
            next_pagination = {"account_idx": account_idx + 1}
        else:
            next_pagination = {}
        logger.info(
            f"{stream.value}: {len(records)} rows for {account_id} {window[0]}..{window[1]} "
            f"(query {min(unit_idx + 1, len(units))}/{len(units)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _attributed_revenue(self, pagination: Optional[dict]) -> SourceIteration:
        """CRM-attributed revenue by campaign for the trailing year (needs a CRM linked in Business Manager)."""
        accounts = self._account_ids()
        idx = (pagination or {}).get("account_idx", 0)
        if idx >= len(accounts):
            return SourceIteration(records=[], next_pagination={})
        account_id = accounts[idx]
        end = self._end_date()
        start = end - timedelta(days=365)
        params = {
            "q": "attributedRevenueMetrics",
            "pivots": ["CAMPAIGN"],
            "account": urn("sponsoredAccount", account_id),
            "dateRange": date_range_literal(start, end),
            "fields": "revenueAttributionMetrics,dateRange,pivotValues",
        }
        try:
            elements = self._get("/rest/adAnalytics", params).get("elements") or []
        except LinkedinApiError as e:
            if e.status_code not in (400, 403):
                raise
            logger.warning(f"attributed_revenue unavailable for account {account_id} (no CRM connected?): {e}")
            elements = []
        records = [
            SourceRecord(
                id=hashlib.sha1(json.dumps([account_id, e.get("pivotValues"), end.isoformat()]).encode()).hexdigest(),
                data={"account_id": account_id, "as_of": end.isoformat(), **e},
            )
            for e in elements
        ]
        next_pagination = {"account_idx": idx + 1} if idx + 1 < len(accounts) else {}
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _start_date(self) -> Optional[date]:
        return date.fromisoformat(self.config.start_date) if self.config.start_date else None

    def _dispatch(self, start: Optional[date], pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream
        if stream in ENTITY_STREAMS:
            return self._entity_iteration(pagination)
        if stream == LinkedinAdsStreams.ATTRIBUTED_REVENUE:
            return self._attributed_revenue(pagination)
        return self._analytics_iteration(start, pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        return self._dispatch(self._start_date(), pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        start = source_state.last_run.date() - timedelta(days=self.config.lookback_days)
        if self._start_date():
            start = max(self._start_date(), start)
        return self._dispatch(start, pagination)
