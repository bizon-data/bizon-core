"""Stripe source: charges, balance_transactions, events and coupons, incremental on `created` with a lookback."""

import time
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase, HTTPBasicAuth

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.stripe.com/v1"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)

# Stripe sends no X-RateLimit-* headers on 200s -- pace client-side instead.
MIN_REQUEST_INTERVAL_SECONDS = 1.0 / 10


class StripeStreams(str, Enum):
    CHARGES = "charges"
    BALANCE_TRANSACTIONS = "balance_transactions"
    EVENTS = "events"
    COUPONS = "coupons"


class StripeSourceConfig(SourceConfig):
    stream: StripeStreams
    api_key: str = Field(..., description="Stripe secret API key")
    start_date: str = Field(..., description="ISO-8601 lower bound on `created` for the first sync")
    lookback_days: int = Field(2, description="Days to re-read before the last run on incremental syncs")
    # check_connection reports a non-2xx response instead of raising.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class StripeSource(AbstractSource):
    def __init__(self, config: StripeSourceConfig):
        super().__init__(config)
        self.config: StripeSourceConfig = config
        self._last_request_at: float = 0.0

    def get_authenticator(self) -> AuthBase:
        # Stripe uses HTTP Basic auth with the secret key as username, blank password.
        return HTTPBasicAuth(self.config.api_key, "")

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in StripeStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return StripeSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/balance", timeout=REQUEST_TIMEOUT)
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _pace(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)
        self._last_request_at = time.monotonic()

    def _get(self, path: str, params: dict) -> dict:
        self._pace()
        response = self.session.get(f"{BASE_URL}{path}", params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def _list_page(self, path: str, params: dict, starting_after: Optional[str]) -> dict:
        query = dict(params)
        if starting_after:
            query["starting_after"] = starting_after
        return self._get(path, query)

    @staticmethod
    def _epoch(dt: datetime) -> int:
        return int(dt.astimezone(timezone.utc).timestamp())

    # Charges mutate after creation, so stamp fetch time: a refetched charge must sort after its earlier copy
    # when deduplicating on the source timestamp.
    @staticmethod
    def _charge_records(charges: List[dict]) -> List[SourceRecord]:
        fetched_at = datetime.now(timezone.utc)
        return [SourceRecord(id=str(c["id"]), data=c, timestamp=fetched_at) for c in charges]

    # Immutable objects -- no events-phase re-fetch needed, unlike charges below.
    def _get_created_windowed(
        self, path: str, label: str, pagination: dict, created_gte: Optional[int]
    ) -> SourceIteration:
        params: Dict[str, Any] = {"limit": PAGE_SIZE}
        if created_gte is not None:
            params["created[gte]"] = created_gte
        page = self._list_page(path, params, (pagination or {}).get("starting_after"))
        records_raw = page.get("data", [])
        records = [
            SourceRecord(id=str(r["id"]), data=r, timestamp=datetime.fromtimestamp(r["created"], tz=timezone.utc))
            for r in records_raw
        ]
        next_pagination = {"starting_after": records_raw[-1]["id"]} if page.get("has_more") and records_raw else {}
        logger.info(f"{label}: fetched {len(records)} (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    # Coupons mutate (times_redeemed, valid) and a list never returns deleted ones, so every run lists
    # everything and ignores the cursor. Run it incremental so a full_refresh doesn't truncate coupons that
    # have since been deleted in Stripe. `applies_to` is absent from the default response.
    COUPONS_LIST_PARAMS = {"expand[]": "data.applies_to"}

    def _get_coupons(self, pagination: dict) -> SourceIteration:
        params = {"limit": PAGE_SIZE, **self.COUPONS_LIST_PARAMS}
        page = self._list_page("/coupons", params, (pagination or {}).get("starting_after"))
        records_raw = page.get("data", [])
        records = self._charge_records(records_raw)
        next_pagination = {"starting_after": records_raw[-1]["id"]} if page.get("has_more") and records_raw else {}
        logger.info(f"coupons: fetched {len(records)} (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    # get() skips the events phase -- a Charge fetch always returns live state, so a
    # backfill needs no separate refetch. get_records_after() adds it: a created[gte]
    # window alone would never catch a refund or dispute landing on an old charge.
    #
    # expand[]=refunds: an account's default Stripe-Version may omit `refunds` otherwise.
    CHARGES_LIST_PARAMS = {"expand[]": "data.refunds"}
    CHARGES_GET_PARAMS = {"expand[]": "refunds"}

    def _get_charges_backfill(self, pagination: dict, created_gte: int) -> SourceIteration:
        params = {"limit": PAGE_SIZE, "created[gte]": created_gte, **self.CHARGES_LIST_PARAMS}
        page = self._list_page("/charges", params, (pagination or {}).get("starting_after"))
        records_raw = page.get("data", [])
        records = self._charge_records(records_raw)
        next_pagination = {"starting_after": records_raw[-1]["id"]} if page.get("has_more") and records_raw else {}
        logger.info(f"charges[backfill]: fetched {len(records)} (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _charges_created_phase(self, pagination: dict, created_gte: int) -> SourceIteration:
        params = {"limit": PAGE_SIZE, "created[gte]": created_gte, **self.CHARGES_LIST_PARAMS}
        page = self._list_page("/charges", params, (pagination or {}).get("starting_after"))
        records_raw = page.get("data", [])
        records = self._charge_records(records_raw)
        if page.get("has_more") and records_raw:
            next_pagination = {"phase": "created", "starting_after": records_raw[-1]["id"]}
        else:
            next_pagination = {"phase": "events"}
        logger.info(f"charges[created]: fetched {len(records)} new charges, next={next_pagination}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _charges_events_phase(self, pagination: dict, created_gte: int) -> SourceIteration:
        params = {"limit": PAGE_SIZE, "created[gte]": created_gte}
        page = self._list_page("/events", params, (pagination or {}).get("starting_after"))
        events_raw = page.get("data", [])

        # charge.dispute.* events embed a Dispute, not a Charge -- its `.charge` field
        # is the id. Re-fetch by id either way: a snapshot can't carry expand[]=refunds,
        # and re-fetching dedups N events on one charge into one GET.
        charge_ids: set = set()
        for event in events_raw:
            obj = (event.get("data") or {}).get("object") or {}
            obj_type = obj.get("object")
            if obj_type == "charge":
                charge_ids.add(obj["id"])
            elif obj_type == "dispute" and obj.get("charge"):
                charge_ids.add(obj["charge"])

        charges = [self._get(f"/charges/{charge_id}", self.CHARGES_GET_PARAMS) for charge_id in charge_ids]
        records = self._charge_records(charges)
        next_pagination = (
            {"phase": "events", "starting_after": events_raw[-1]["id"]} if page.get("has_more") and events_raw else {}
        )
        logger.info(
            f"charges[events]: scanned {len(events_raw)} events -> {len(records)} charge updates, "
            f"next={next_pagination}"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_charges_incremental(self, pagination: dict, created_gte: int) -> SourceIteration:
        phase = (pagination or {}).get("phase", "created")
        if phase == "created":
            return self._charges_created_phase(pagination, created_gte)
        return self._charges_events_phase(pagination, created_gte)

    def get(self, pagination: dict = None) -> SourceIteration:
        start_dt = datetime.fromisoformat(self.config.start_date.replace("Z", "+00:00"))
        created_gte = self._epoch(start_dt)

        if self.config.stream == StripeStreams.CHARGES:
            return self._get_charges_backfill(pagination, created_gte)
        if self.config.stream == StripeStreams.BALANCE_TRANSACTIONS:
            return self._get_created_windowed("/balance_transactions", "balance_transactions", pagination, created_gte)
        if self.config.stream == StripeStreams.EVENTS:
            return self._get_created_windowed("/events", "events", pagination, created_gte)
        if self.config.stream == StripeStreams.COUPONS:
            return self._get_coupons(pagination)

        raise NotImplementedError(f"Stream '{self.config.stream}' is not implemented for Stripe source")

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        created_gte = self._epoch(source_state.last_run - timedelta(days=self.config.lookback_days))

        if self.config.stream == StripeStreams.CHARGES:
            return self._get_charges_incremental(pagination, created_gte)
        if self.config.stream == StripeStreams.BALANCE_TRANSACTIONS:
            return self._get_created_windowed("/balance_transactions", "balance_transactions", pagination, created_gte)
        if self.config.stream == StripeStreams.EVENTS:
            return self._get_created_windowed("/events", "events", pagination, created_gte)
        if self.config.stream == StripeStreams.COUPONS:
            return self._get_coupons(pagination)

        raise NotImplementedError(f"Stream '{self.config.stream}' is not implemented for Stripe source")
