"""Customer.io Journeys App API source: customers, campaigns, newsletters, segments and their metrics/messages.

US base URL only. Bearer auth (App API key). The 10 req/s rate limit is handled by the session's retry policy.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

import requests
from loguru import logger
from pydantic import Field, model_validator
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.customer.io"


class CustomerioStreams(str, Enum):
    CUSTOMERS = "customers"
    CAMPAIGNS = "campaigns"
    CAMPAIGN_ACTIONS = "campaign_actions"
    CAMPAIGN_METRICS = "campaign_metrics"
    CAMPAIGN_LINK_METRICS = "campaign_link_metrics"
    CAMPAIGN_MESSAGES = "campaign_messages"
    CAMPAIGN_JOURNEY_METRICS = "campaign_journey_metrics"
    CAMPAIGN_ACTION_METRICS = "campaign_action_metrics"
    CAMPAIGN_ACTION_LINK_METRICS = "campaign_action_link_metrics"
    NEWSLETTERS = "newsletters"
    NEWSLETTER_METRICS = "newsletter_metrics"
    NEWSLETTER_LINK_METRICS = "newsletter_link_metrics"
    NEWSLETTER_MESSAGES = "newsletter_messages"
    NEWSLETTER_VARIANTS = "newsletter_variants"
    NEWSLETTER_TEST_GROUPS = "newsletter_test_groups"
    NEWSLETTER_VARIANT_METRICS = "newsletter_variant_metrics"
    NEWSLETTER_VARIANT_LINK_METRICS = "newsletter_variant_link_metrics"
    SEGMENTS = "segments"
    SEGMENT_USED_BY = "segment_used_by"
    SEGMENT_CUSTOMER_COUNT = "segment_customer_count"
    SUBSCRIPTION_TOPICS = "subscription_topics"


class CustomerioSourceConfig(SourceConfig):
    stream: CustomerioStreams
    api_key: str = Field(..., description="Customer.io App API key")
    segment_id: Optional[int] = Field(None, description="Segment whose members the customers stream lists")
    lookback_days: int = Field(90, description="Rolling window, in days, for the time-series metrics streams")
    timezone: str = Field("UTC", description="IANA timezone the daily metrics buckets are computed in")
    # Non-2xx responses are logged with their body before raising; check_connection reports them instead.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )

    @model_validator(mode="after")
    def _customers_need_a_segment(self):
        if self.stream == CustomerioStreams.CUSTOMERS and self.segment_id is None:
            raise ValueError("segment_id is required for the customers stream")
        return self


class CustomerioAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class CustomerioSource(AbstractSource):
    def __init__(self, config: CustomerioSourceConfig):
        super().__init__(config)
        self.config: CustomerioSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in CustomerioStreams]

    @staticmethod
    def get_config_class() -> type:
        return CustomerioSourceConfig

    def get_authenticator(self) -> AuthBase:
        return CustomerioAuth(self.config.api_key)

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/v1/segments")
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _get_json(self, url: str, params: Optional[dict] = None) -> dict:
        response = self.session.get(url, params=params)
        if not response.ok:
            logger.error(f"Customer.io GET {url} {response.status_code}: {response.text}")
            response.raise_for_status()
        return response.json()

    def _post_json(self, url: str, json_body: dict, params: Optional[dict] = None) -> dict:
        response = self.session.post(url, params=params, json=json_body)
        if not response.ok:
            logger.error(f"Customer.io POST {url} {response.status_code}: {response.text}")
            response.raise_for_status()
        return response.json()

    def _list_campaigns(self) -> list:
        """Return all campaigns.

        Spec-verified against the Journeys App OpenAPI spec: GET /v1/campaigns
        takes no query params and its 200 response has no `next` field — there
        is nothing to paginate.
        """
        return self._get_json(f"{BASE_URL}/v1/campaigns").get("campaigns", [])

    def _list_newsletters(self) -> list:
        """Return all newsletters, following `next` to exhaustion.

        Per the Journeys App OpenAPI spec, GET /v1/newsletters accepts `limit`,
        `sort`, `start` and its 200 response carries both `newsletters` and a
        `next` string ("Add ?start=<next_value> to the request to get the next
        page of results."). Not following it silently truncates the parent set
        that seeds all 8 newsletter_* streams to the first page. `limit` is
        deliberately not passed: the spec documents no default/max for it here,
        an out-of-range value would 400, and a newsletter list is typically
        small, so a handful of default-sized pages is cheap.
        """
        newsletters: list = []
        start_token = None
        while True:
            params = {"start": start_token} if start_token else None
            data = self._get_json(f"{BASE_URL}/v1/newsletters", params=params)
            newsletters.extend(data.get("newsletters") or [])
            next_token = data.get("next")
            if not next_token:
                break
            if next_token == start_token:
                logger.warning(
                    f"/v1/newsletters returned a non-advancing next token "
                    f"({next_token!r}); stopping pagination to avoid hanging."
                )
                break
            start_token = next_token
        return newsletters

    def _list_segments(self) -> list:
        return self._get_json(f"{BASE_URL}/v1/segments").get("segments", [])

    def _time_window(self) -> Tuple[int, int]:
        end = int(datetime.now(tz=timezone.utc).timestamp())
        start = end - self.config.lookback_days * 86400
        return start, end

    def _customer_id(self, customer: dict) -> str:
        """Pick a stable id for a customer record."""
        for key in ("email", "cio_id", "id"):
            if customer.get(key):
                return str(customer[key])
        ids = customer.get("ids") or []
        if ids:
            return str(ids[0])
        return ""

    def _seed_parents(self, parents: list, keys: Tuple[str, ...] = ("id",)) -> Optional[dict]:
        """Return the initial multi-resource pagination dict, or None if no parents.

        Only the listed keys are retained per parent. The pagination cursor is
        persisted as JSON by the engine but recovered with ast.literal_eval(),
        which only accepts Python literals — it chokes on JSON null/true/false (and
        on NaN/Infinity) as bare ast.Name nodes. Full API objects carry those
        tokens, so storing them poisons the cursor permanently. The builders only
        ever read the parent's "id" (and "actions" for campaign-action streams),
        so reducing to those keys keeps the cursor literal-safe with no behaviour
        change. Reducing here is the single chokepoint: _advance_parents and the
        re-stored current_parent in get_campaign_actions and the *_messages
        handlers only ever carry these already-reduced dicts.
        """
        if not parents:
            return None
        reduced = [{k: p.get(k) for k in keys} for p in parents]
        return {"remaining_parents": reduced[1:], "current_parent": reduced[0]}

    def _advance_parents(self, remaining: list) -> dict:
        """Build the pagination dict for the next get() call given a remaining-parents list."""
        if not remaining:
            return {}
        return {"remaining_parents": remaining[1:], "current_parent": remaining[0]}

    def get_customers(self, pagination: Optional[dict] = None) -> SourceIteration:
        body = {"filter": {"segment": {"id": self.config.segment_id}}}
        params = {"limit": 50}
        if pagination and pagination.get("start"):
            params["start"] = pagination["start"]

        data = self._post_json(f"{BASE_URL}/v1/customers", json_body=body, params=params)

        customers_raw = data.get("identifiers") or data.get("ids") or []
        records = [SourceRecord(id=self._customer_id(c) or str(i), data=c) for i, c in enumerate(customers_raw)]
        next_token = data.get("next")
        next_pagination = {"start": next_token} if next_token else {}

        logger.info(
            f"Fetched {len(records)} customers from segment {self.config.segment_id} (has_more={bool(next_token)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _entity_updated_ts(entity: dict) -> int:
        return int(entity.get("updated") or entity.get("updated_at") or 0)

    def get_campaigns(
        self, pagination: Optional[dict] = None, updated_after_ts: Optional[int] = None
    ) -> SourceIteration:
        if pagination:
            return SourceIteration(records=[], next_pagination={})
        campaigns = self._list_campaigns()
        if updated_after_ts is not None:
            before = len(campaigns)
            campaigns = [c for c in campaigns if self._entity_updated_ts(c) > updated_after_ts]
            logger.info(f"Incremental filter: {before} -> {len(campaigns)} campaigns with updated > {updated_after_ts}")
        records = [SourceRecord(id=str(c["id"]), data=c) for c in campaigns if c.get("id")]
        logger.info(f"Fetched {len(records)} campaigns")
        return SourceIteration(records=records, next_pagination={})

    def get_campaign_actions(
        self, pagination: Optional[dict] = None, updated_after_ts: Optional[int] = None
    ) -> SourceIteration:
        # /v1/campaigns/{id}/actions paginates 10 at a time and returns a `next`
        # token — NOT following it silently truncated every campaign to its first
        # 10 actions in both sync modes. Pagination state mirrors
        # get_campaign_messages: {remaining_parents, current_parent, start}, where
        # `start` is the intra-campaign cursor. Advance to the next campaign only
        # once the current one's actions are exhausted.
        #
        # /v1/campaigns and /v1/campaigns/{id}/actions have no time filter, so
        # incremental works in two stages:
        #   1. parent-level: drop campaigns whose `updated` <= last_run_ts —
        #      Customer.io bumps the parent campaign's `updated` whenever any
        #      action under it is edited, so unchanged campaigns can't have
        #      changed actions.
        #   2. per-action: filter actions by their own `updated` (belt & braces).
        if not pagination or "current_parent" not in pagination:
            campaigns = self._list_campaigns()
            if updated_after_ts is not None:
                before = len(campaigns)
                campaigns = [c for c in campaigns if self._entity_updated_ts(c) > updated_after_ts]
                logger.info(
                    f"Incremental parent filter: {before} -> {len(campaigns)} campaigns to scan for action changes"
                )
            seeded = self._seed_parents(campaigns)
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        campaign = pagination["current_parent"]
        campaign_id = campaign["id"]
        remaining = pagination.get("remaining_parents", [])
        start_token = pagination.get("start")

        params = {"start": start_token} if start_token else None
        try:
            data = self._get_json(f"{BASE_URL}/v1/campaigns/{campaign_id}/actions", params=params)
            actions = data.get("actions") or []
            next_token = data.get("next")
        except requests.RequestException as e:
            if start_token is not None:
                # Mid-walk failure (page 2+): earlier pages for this campaign are
                # already emitted. Swallowing here would commit a partial campaign
                # and advance the incremental watermark past it, losing the missing
                # pages until the campaign's `updated` changes again. Fail the run
                # instead; the session's retry policy already absorbs transient
                # 429/5xx.
                raise
            # First-page failure: no partial data has been emitted for this
            # campaign yet, so skipping it preserves the original "skip a
            # campaign whose actions endpoint is unavailable" behaviour.
            logger.warning(f"Skipping actions for campaign {campaign_id}: {e}")
            actions = []
            next_token = None

        if updated_after_ts is not None:
            before = len(actions)
            actions = [a for a in actions if self._entity_updated_ts(a) > updated_after_ts]
            logger.info(
                f"Incremental filter: {before} -> {len(actions)} actions for "
                f"campaign {campaign_id} (updated > {updated_after_ts})"
            )

        records = [
            SourceRecord(
                id=f"{campaign_id}:{a.get('id')}",
                data={"campaign_id": campaign_id, **a},
            )
            for a in actions
            if a.get("id") is not None
        ]

        has_more = bool(next_token and next_token != start_token)
        if has_more:
            # More action pages for this campaign — hold the parent, advance `start`.
            next_pagination = {
                "remaining_parents": remaining,
                "current_parent": campaign,
                "start": next_token,
            }
        else:
            if next_token:
                # Defensive: a non-advancing `next` would keep next_pagination
                # non-empty forever — a hard hang.
                logger.warning(
                    f"Non-advancing next token for campaign {campaign_id} actions; moving on to the next campaign."
                )
            next_pagination = self._advance_parents(remaining)

        logger.info(
            f"Fetched {len(records)} actions page for campaign {campaign_id} "
            f"(more_pages={has_more}, remaining_campaigns={len(remaining)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _next_campaign_iteration(
        self,
        pagination: Optional[dict],
        record_builder,
    ) -> SourceIteration:
        """Generic per-campaign iterator: one campaign per get() call, one record out."""
        if not pagination or "current_parent" not in pagination:
            campaigns = self._list_campaigns()
            seeded = self._seed_parents(campaigns)
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        campaign = pagination["current_parent"]
        campaign_id = campaign["id"]
        remaining = pagination.get("remaining_parents", [])

        try:
            record = record_builder(campaign_id, campaign)
            records = [record] if record is not None else []
        except requests.HTTPError as e:
            # Some campaign types (e.g., transactional, broadcast) don't support
            # all metric endpoints — skip with a warning instead of failing the run.
            if e.response is not None and e.response.status_code in (400, 404):
                logger.warning(f"Skipping campaign {campaign_id}: HTTP {e.response.status_code}")
                records = []
            else:
                raise

        next_pagination = self._advance_parents(remaining)
        logger.info(f"Processed campaign {campaign_id} (remaining={len(remaining)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_campaign_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        start, end = self._time_window()

        def builder(campaign_id, _campaign):
            data = self._get_json(
                f"{BASE_URL}/v1/campaigns/{campaign_id}/metrics",
                params={
                    "version": 2,
                    "res": "days",
                    "tz": self.config.timezone,
                    "start": start,
                    "end": end,
                },
            )
            return SourceRecord(
                id=str(campaign_id),
                data={"campaign_id": campaign_id, **data},
            )

        return self._next_campaign_iteration(pagination, builder)

    def get_campaign_link_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        def builder(campaign_id, _campaign):
            data = self._get_json(
                f"{BASE_URL}/v1/campaigns/{campaign_id}/metrics/links",
                params={"period": "days", "steps": 45, "unique": "true"},
            )
            return SourceRecord(
                id=str(campaign_id),
                data={"campaign_id": campaign_id, **data},
            )

        return self._next_campaign_iteration(pagination, builder)

    def get_campaign_journey_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        start, end = self._time_window()

        def builder(campaign_id, _campaign):
            data = self._get_json(
                f"{BASE_URL}/v1/campaigns/{campaign_id}/journey_metrics",
                params={"resolution": "daily", "start": start, "end": end},
            )
            return SourceRecord(
                id=str(campaign_id),
                data={"campaign_id": campaign_id, **data},
            )

        return self._next_campaign_iteration(pagination, builder)

    def get_campaign_messages(
        self, pagination: Optional[dict] = None, start_ts: Optional[int] = None
    ) -> SourceIteration:
        # One PAGE per get() call to keep memory bounded (campaigns can have millions of msgs).
        # Pagination state: {remaining_parents, current_parent, start (intra-campaign cursor)}
        if not pagination or "current_parent" not in pagination:
            campaigns = self._list_campaigns()
            seeded = self._seed_parents(campaigns)
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        campaign = pagination["current_parent"]
        campaign_id = campaign["id"]
        remaining = pagination.get("remaining_parents", [])
        start_token = pagination.get("start")

        params = {"drafts": "false", "limit": 1000}
        if start_token:
            params["start"] = start_token
        if start_ts is not None:
            params["start_ts"] = start_ts
        data = self._get_json(f"{BASE_URL}/v1/campaigns/{campaign_id}/messages", params=params)

        records = [
            SourceRecord(
                id=str(msg.get("id")),
                data={"campaign_id": campaign_id, **msg},
            )
            for msg in data.get("messages", [])
        ]
        next_token = data.get("next")

        has_more = bool(next_token and next_token != start_token)
        if has_more:
            next_pagination = {
                "remaining_parents": remaining,
                "current_parent": campaign,
                "start": next_token,
            }
        else:
            if next_token:
                # Defensive: a non-advancing `next` would keep next_pagination
                # non-empty forever — a hard hang.
                logger.warning(
                    f"Non-advancing next token for campaign {campaign_id} messages; moving on to the next campaign."
                )
            next_pagination = self._advance_parents(remaining)

        logger.info(
            f"Fetched {len(records)} campaign messages page for campaign {campaign_id} "
            f"(more_pages={has_more}, remaining_campaigns={len(remaining)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_campaign_action_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        return self._campaign_action_iteration(pagination, with_links=False)

    def get_campaign_action_link_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        return self._campaign_action_iteration(pagination, with_links=True)

    def _campaign_action_iteration(self, pagination: Optional[dict], with_links: bool) -> SourceIteration:
        if not pagination or "current_parent" not in pagination:
            campaigns = self._list_campaigns()
            # Keep "actions" too: this iterator reads campaign["actions"] below.
            seeded = self._seed_parents(campaigns, keys=("id", "actions"))
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        campaign = pagination["current_parent"]
        campaign_id = campaign["id"]
        remaining = pagination.get("remaining_parents", [])
        actions = campaign.get("actions") or []

        start, end = self._time_window()
        records = []
        for action in actions:
            action_id = action["id"] if isinstance(action, dict) else action
            try:
                if with_links:
                    data = self._get_json(
                        f"{BASE_URL}/v1/campaigns/{campaign_id}/actions/{action_id}/metrics/links",
                        params={"period": "days", "steps": 45},
                    )
                else:
                    data = self._get_json(
                        f"{BASE_URL}/v1/campaigns/{campaign_id}/actions/{action_id}/metrics",
                        params={
                            "version": 2,
                            "res": "days",
                            "tz": self.config.timezone,
                            "start": start,
                            "end": end,
                        },
                    )
            except Exception as e:
                logger.warning(f"Skipping action {action_id} on campaign {campaign_id}: {e}")
                continue
            records.append(
                SourceRecord(
                    id=f"{campaign_id}:{action_id}",
                    data={
                        "campaign_id": campaign_id,
                        "action_id": action_id,
                        **data,
                    },
                )
            )

        next_pagination = self._advance_parents(remaining)
        logger.info(
            f"Fetched {len(records)} action {'link ' if with_links else ''}metrics "
            f"for campaign {campaign_id} (remaining={len(remaining)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_newsletters(
        self, pagination: Optional[dict] = None, updated_after_ts: Optional[int] = None
    ) -> SourceIteration:
        if pagination:
            return SourceIteration(records=[], next_pagination={})
        newsletters = self._list_newsletters()
        if updated_after_ts is not None:
            before = len(newsletters)
            newsletters = [n for n in newsletters if self._entity_updated_ts(n) > updated_after_ts]
            logger.info(
                f"Incremental filter: {before} -> {len(newsletters)} newsletters with updated > {updated_after_ts}"
            )
        records = [SourceRecord(id=str(n["id"]), data=n) for n in newsletters if n.get("id")]
        logger.info(f"Fetched {len(records)} newsletters")
        return SourceIteration(records=records, next_pagination={})

    def _next_newsletter_iteration(
        self,
        pagination: Optional[dict],
        record_builder,
    ) -> SourceIteration:
        if not pagination or "current_parent" not in pagination:
            newsletters = self._list_newsletters()
            seeded = self._seed_parents(newsletters)
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        newsletter = pagination["current_parent"]
        newsletter_id = newsletter["id"]
        remaining = pagination.get("remaining_parents", [])

        try:
            records = record_builder(newsletter_id, newsletter)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 404):
                logger.warning(f"Skipping newsletter {newsletter_id}: HTTP {e.response.status_code}")
                records = []
            else:
                raise

        next_pagination = self._advance_parents(remaining)
        logger.info(f"Processed newsletter {newsletter_id} (remaining={len(remaining)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_newsletter_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        def builder(newsletter_id, _newsletter):
            data = self._get_json(
                f"{BASE_URL}/v1/newsletters/{newsletter_id}/metrics",
                params={"period": "days", "steps": 45},
            )
            return [
                SourceRecord(
                    id=str(newsletter_id),
                    data={"newsletter_id": newsletter_id, **data},
                )
            ]

        return self._next_newsletter_iteration(pagination, builder)

    def get_newsletter_link_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        def builder(newsletter_id, _newsletter):
            data = self._get_json(
                f"{BASE_URL}/v1/newsletters/{newsletter_id}/metrics/links",
                params={"period": "days", "steps": 45, "unique": "true"},
            )
            return [
                SourceRecord(
                    id=str(newsletter_id),
                    data={"newsletter_id": newsletter_id, **data},
                )
            ]

        return self._next_newsletter_iteration(pagination, builder)

    def get_newsletter_variants(self, pagination: Optional[dict] = None) -> SourceIteration:
        def builder(newsletter_id, _newsletter):
            data = self._get_json(f"{BASE_URL}/v1/newsletters/{newsletter_id}/contents")
            variants = data.get("contents") or data.get("variants") or []
            return [
                SourceRecord(
                    id=f"{newsletter_id}:{v.get('id')}",
                    data={"newsletter_id": newsletter_id, **v},
                )
                for v in variants
            ]

        return self._next_newsletter_iteration(pagination, builder)

    def get_newsletter_test_groups(self, pagination: Optional[dict] = None) -> SourceIteration:
        def builder(newsletter_id, _newsletter):
            data = self._get_json(f"{BASE_URL}/v1/newsletters/{newsletter_id}/test_groups")
            groups = data.get("test_groups") or []
            return [
                SourceRecord(
                    id=f"{newsletter_id}:{g.get('id')}",
                    data={"newsletter_id": newsletter_id, **g},
                )
                for g in groups
            ]

        return self._next_newsletter_iteration(pagination, builder)

    def get_newsletter_messages(
        self, pagination: Optional[dict] = None, start_ts: Optional[int] = None
    ) -> SourceIteration:
        # One PAGE per get() call to keep memory bounded.
        if not pagination or "current_parent" not in pagination:
            newsletters = self._list_newsletters()
            seeded = self._seed_parents(newsletters)
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        newsletter = pagination["current_parent"]
        newsletter_id = newsletter["id"]
        remaining = pagination.get("remaining_parents", [])
        start_token = pagination.get("start")

        params = {"limit": 1000}
        if start_token:
            params["start"] = start_token
        if start_ts is not None:
            params["start_ts"] = start_ts
        data = self._get_json(f"{BASE_URL}/v1/newsletters/{newsletter_id}/messages", params=params)

        records = [
            SourceRecord(
                id=str(msg.get("id")),
                data={"newsletter_id": newsletter_id, **msg},
            )
            for msg in data.get("messages", [])
        ]
        next_token = data.get("next")

        has_more = bool(next_token and next_token != start_token)
        if has_more:
            next_pagination = {
                "remaining_parents": remaining,
                "current_parent": newsletter,
                "start": next_token,
            }
        else:
            if next_token:
                # Defensive: a non-advancing `next` would keep next_pagination
                # non-empty forever — a hard hang.
                logger.warning(
                    f"Non-advancing next token for newsletter {newsletter_id} messages; "
                    f"moving on to the next newsletter."
                )
            next_pagination = self._advance_parents(remaining)

        logger.info(
            f"Fetched {len(records)} newsletter messages page for newsletter {newsletter_id} "
            f"(more_pages={has_more}, remaining_newsletters={len(remaining)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_newsletter_variant_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        return self._newsletter_variant_iteration(pagination, with_links=False)

    def get_newsletter_variant_link_metrics(self, pagination: Optional[dict] = None) -> SourceIteration:
        return self._newsletter_variant_iteration(pagination, with_links=True)

    def _newsletter_variant_iteration(self, pagination: Optional[dict], with_links: bool) -> SourceIteration:
        if not pagination or "current_parent" not in pagination:
            newsletters = self._list_newsletters()
            seeded = self._seed_parents(newsletters)
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        newsletter = pagination["current_parent"]
        newsletter_id = newsletter["id"]
        remaining = pagination.get("remaining_parents", [])

        # Discover variants for this newsletter
        try:
            variants_resp = self._get_json(f"{BASE_URL}/v1/newsletters/{newsletter_id}/contents")
            variants = variants_resp.get("contents") or variants_resp.get("variants") or []
        except Exception as e:
            logger.warning(f"Could not list variants for newsletter {newsletter_id}: {e}")
            variants = []

        records = []
        for v in variants:
            content_id = v.get("id")
            if content_id is None:
                continue
            try:
                if with_links:
                    data = self._get_json(
                        f"{BASE_URL}/v1/newsletters/{newsletter_id}/contents/{content_id}/metrics/links",
                        params={"period": "days", "steps": 45},
                    )
                else:
                    data = self._get_json(
                        f"{BASE_URL}/v1/newsletters/{newsletter_id}/contents/{content_id}/metrics",
                        params={"period": "days", "steps": 45},
                    )
            except Exception as e:
                logger.warning(f"Skipping variant {content_id} on newsletter {newsletter_id}: {e}")
                continue
            records.append(
                SourceRecord(
                    id=f"{newsletter_id}:{content_id}",
                    data={
                        "newsletter_id": newsletter_id,
                        "content_id": content_id,
                        **data,
                    },
                )
            )

        next_pagination = self._advance_parents(remaining)
        logger.info(
            f"Fetched {len(records)} variant {'link ' if with_links else ''}metrics "
            f"for newsletter {newsletter_id} (remaining={len(remaining)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_segments(self, pagination: Optional[dict] = None) -> SourceIteration:
        if pagination:
            return SourceIteration(records=[], next_pagination={})
        segments = self._list_segments()
        records = [SourceRecord(id=str(s.get("id")), data=s) for s in segments]
        logger.info(f"Fetched {len(records)} segments")
        return SourceIteration(records=records, next_pagination={})

    def get_segment_used_by(
        self, pagination: Optional[dict] = None, updated_after_ts: Optional[int] = None
    ) -> SourceIteration:
        # Filter parents by `updated_at > updated_after_ts` when running incrementally —
        # the /used_by endpoint has no time filter, so the parent set is shrunk instead.
        if not pagination or "current_parent" not in pagination:
            segments = self._list_segments()
            if updated_after_ts is not None:
                before = len(segments)
                segments = [s for s in segments if (s.get("updated_at") or 0) > updated_after_ts]
                logger.info(
                    f"Incremental filter: {before} -> {len(segments)} segments with updated_at > {updated_after_ts}"
                )
            seeded = self._seed_parents(segments)
            if seeded is None:
                return SourceIteration(records=[], next_pagination={})
            pagination = seeded

        segment = pagination["current_parent"]
        segment_id = segment["id"]
        remaining = pagination.get("remaining_parents", [])

        data = self._get_json(f"{BASE_URL}/v1/segments/{segment_id}/used_by")
        record = SourceRecord(
            id=str(segment_id),
            data={"segment_id": segment_id, **data},
        )

        next_pagination = self._advance_parents(remaining)
        logger.info(f"Fetched used_by for segment {segment_id} (remaining={len(remaining)})")
        return SourceIteration(records=[record], next_pagination=next_pagination)

    def get_segment_customer_count(self, pagination: Optional[dict] = None) -> SourceIteration:
        # Daily snapshot: one bizon iteration emits a snapshot for every segment,
        # stamped with snapshot_date so history can be built over time.
        if pagination:
            return SourceIteration(records=[], next_pagination={})

        snapshot_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        segments = self._list_segments()
        records = []
        for segment in segments:
            segment_id = segment.get("id")
            if segment_id is None:
                continue
            try:
                data = self._get_json(f"{BASE_URL}/v1/segments/{segment_id}/customer_count")
            except Exception as e:
                logger.warning(f"Skipping customer_count for segment {segment_id}: {e}")
                continue
            records.append(
                SourceRecord(
                    id=f"{snapshot_date}:{segment_id}",
                    data={
                        "snapshot_date": snapshot_date,
                        "segment_id": segment_id,
                        **data,
                    },
                )
            )

        logger.info(f"Snapshot {snapshot_date}: {len(records)} segment customer counts")
        return SourceIteration(records=records, next_pagination={})

    def get_subscription_topics(self, pagination: Optional[dict] = None) -> SourceIteration:
        if pagination:
            return SourceIteration(records=[], next_pagination={})
        data = self._get_json(f"{BASE_URL}/v1/subscription_topics")
        topics = data.get("topics") or data.get("subscription_topics") or []
        records = [SourceRecord(id=str(t.get("id")), data=t) for t in topics]
        logger.info(f"Fetched {len(records)} subscription topics")
        return SourceIteration(records=records, next_pagination={})

    def get(self, pagination: Optional[dict] = None) -> SourceIteration:
        stream = self.config.stream
        dispatch = {
            CustomerioStreams.CUSTOMERS: self.get_customers,
            CustomerioStreams.CAMPAIGNS: self.get_campaigns,
            CustomerioStreams.CAMPAIGN_ACTIONS: self.get_campaign_actions,
            CustomerioStreams.CAMPAIGN_METRICS: self.get_campaign_metrics,
            CustomerioStreams.CAMPAIGN_LINK_METRICS: self.get_campaign_link_metrics,
            CustomerioStreams.CAMPAIGN_MESSAGES: self.get_campaign_messages,
            CustomerioStreams.CAMPAIGN_JOURNEY_METRICS: self.get_campaign_journey_metrics,
            CustomerioStreams.CAMPAIGN_ACTION_METRICS: self.get_campaign_action_metrics,
            CustomerioStreams.CAMPAIGN_ACTION_LINK_METRICS: self.get_campaign_action_link_metrics,
            CustomerioStreams.NEWSLETTERS: self.get_newsletters,
            CustomerioStreams.NEWSLETTER_METRICS: self.get_newsletter_metrics,
            CustomerioStreams.NEWSLETTER_LINK_METRICS: self.get_newsletter_link_metrics,
            CustomerioStreams.NEWSLETTER_MESSAGES: self.get_newsletter_messages,
            CustomerioStreams.NEWSLETTER_VARIANTS: self.get_newsletter_variants,
            CustomerioStreams.NEWSLETTER_TEST_GROUPS: self.get_newsletter_test_groups,
            CustomerioStreams.NEWSLETTER_VARIANT_METRICS: self.get_newsletter_variant_metrics,
            CustomerioStreams.NEWSLETTER_VARIANT_LINK_METRICS: self.get_newsletter_variant_link_metrics,
            CustomerioStreams.SEGMENTS: self.get_segments,
            CustomerioStreams.SEGMENT_USED_BY: self.get_segment_used_by,
            CustomerioStreams.SEGMENT_CUSTOMER_COUNT: self.get_segment_customer_count,
            CustomerioStreams.SUBSCRIPTION_TOPICS: self.get_subscription_topics,
        }
        handler = dispatch.get(stream)
        if handler is None:
            raise NotImplementedError(f"Stream '{stream}' is not implemented for Customer.io source")
        return handler(pagination)

    def get_records_after(
        self, source_state: SourceIncrementalState, pagination: Optional[dict] = None
    ) -> SourceIteration:
        """Incremental sync entry point. Bizon calls this when sync_mode=incremental.

        - campaign_messages / newsletter_messages: pass `start_ts` (unix) so the API
          only returns messages sent after the last successful run.
        - segment_used_by: filter parent segments by `updated_at > last_run_ts`.

        Other streams either don't support incremental natively (full refresh) or are
        already bounded (e.g., metrics streams already use a rolling 90-day window).
        """
        last_run_ts = int(source_state.last_run.timestamp())
        stream = self.config.stream

        if stream == CustomerioStreams.CAMPAIGN_MESSAGES:
            return self.get_campaign_messages(pagination, start_ts=last_run_ts)
        if stream == CustomerioStreams.NEWSLETTER_MESSAGES:
            return self.get_newsletter_messages(pagination, start_ts=last_run_ts)
        if stream == CustomerioStreams.SEGMENT_USED_BY:
            return self.get_segment_used_by(pagination, updated_after_ts=last_run_ts)
        if stream == CustomerioStreams.CAMPAIGNS:
            return self.get_campaigns(pagination, updated_after_ts=last_run_ts)
        if stream == CustomerioStreams.CAMPAIGN_ACTIONS:
            return self.get_campaign_actions(pagination, updated_after_ts=last_run_ts)
        if stream == CustomerioStreams.NEWSLETTERS:
            return self.get_newsletters(pagination, updated_after_ts=last_run_ts)

        # Streams without an incremental path: just do the full-refresh handler.
        return self.get(pagination)
