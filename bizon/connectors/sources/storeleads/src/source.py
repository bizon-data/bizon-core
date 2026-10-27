"""StoreLeads source: domains, cart-link domains, apps, technologies and Shopify app reviews. Full refresh only.

app_reviews records carry the parent `app_token`/`app_platform`; every other stream lands the raw object.
"""

import hashlib
import json
import math
import time
from enum import Enum
from typing import Any, Callable, List, Optional, Tuple

import requests
from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE = "https://storeleads.app/json/api/v1/"
TIMEOUT = (10, 300)

# List endpoints are documented at 2 req/s but 429 on back-to-back calls, so pace to <=1 req/s.
MIN_LIST_INTERVAL_SECONDS = 1.05

# Retrieve endpoints (per-app reviews) are documented at 5 req/s.
MIN_REVIEWS_INTERVAL_SECONDS = 0.55

GET_ATTEMPTS = 6
GET_BACKOFF_SECONDS = 5
TRANSIENT_NETWORK_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)

DOMAINS_PAGE_SIZE = 2000  # domain objects are large; keeps one page well under buffer_size.
CART_LINK_PAGE_SIZE = 5000  # fields=name,platform keeps this payload small.
CART_LINK_PLATFORMS = "shopify,magento,bigcommerce,woocommerce"

APPS_TECH_PAGE_SIZE = 50

# all/app 422s ("invalid page value") past this page index, and its `sort` order is unstable
# within created_at ties, so the catalog is walked as filtered partitions in default order.
MAX_PAGE_INDEX = 1000

# `or` and `not` over the same set are complements, so the partitions stay disjoint and complete
# whatever this list contains; only their balance drifts.
SHOPIFY_CATEGORIES = (
    "analytics,discounts,upsell and cross-sell,seo,chat,store design,shipping,marketing,dropshipping,"
    "inventory sync,inventory optimization,delivery and pickup,workflow automation,cart customization,"
    "bulk editor,product reviews,product bundles,product variants,pop-ups,shipping rates,"
    "product display - other,banners,legal,product content,invoices and receipts,orders - other"
)
SHOPIFY_PARTITIONS: Tuple[Tuple[str, dict], ...] = (
    ("shopify_in_categories", {"f:p": "shopify", "f:categories": SHOPIFY_CATEGORIES, "f:categories:op": "or"}),
    ("shopify_not_in_categories", {"f:p": "shopify", "f:categories": SHOPIFY_CATEGORIES, "f:categories:op": "not"}),
)
APPS_PARTITIONS: Tuple[Tuple[str, dict], ...] = (
    *SHOPIFY_PARTITIONS,
    ("not_shopify", {"f:p": "shopify", "f:p:op": "not"}),
)

# The reviews endpoint caps page_size at 50 server-side.
REVIEWS_PLATFORM = "shopify"
REVIEWS_PAGE_SIZE = 50


class StoreleadsStreams(str, Enum):
    DOMAINS = "domains"
    HAS_CART_LINK_DOMAINS = "has_cart_link_domains"
    APPS = "apps"
    TECHNOLOGIES = "technologies"
    APP_REVIEWS = "app_reviews"


class StoreleadsSourceConfig(SourceConfig):
    api_token: Optional[str] = Field(None, description="StoreLeads API token (required)")

    # Testing only: caps how many all/app pages the app_reviews scan walks.
    max_review_scan_pages: Optional[int] = None
    # app_reviews treats a 404 on an app's reviews as no reviews.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class StoreleadsAuth(AuthBase):
    def __init__(self, token: str):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r


class StoreleadsSource(AbstractSource):
    def __init__(self, config: StoreleadsSourceConfig):
        super().__init__(config)
        self.config: StoreleadsSourceConfig = config
        self._next_list_request_at = 0.0
        self._next_reviews_request_at = 0.0
        # Process-local: the cursor carries only small ints, never the token list.
        self._scan_cursor: dict = {}
        self._scan_done = False
        self._app_tokens: List[str] = []
        self._app_tokens_seen: set[str] = set()
        self._scan_duplicates = 0
        self._apps_seen: set[str] = set()
        self._apps_duplicates = 0
        self._apps_pages = 0
        self._apps_part_totals: dict[str, int] = {}

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in StoreleadsStreams]

    @staticmethod
    def get_config_class() -> type:
        return StoreleadsSourceConfig

    def get_authenticator(self) -> Optional[AuthBase]:
        if not self.config.api_token:
            raise ValueError("storeleads source requires api_token")
        return StoreleadsAuth(self.config.api_token)

    def _throttle_list(self) -> None:
        now = time.monotonic()
        wait = self._next_list_request_at - now
        if wait > 0:
            time.sleep(wait)
        self._next_list_request_at = max(now, self._next_list_request_at) + MIN_LIST_INTERVAL_SECONDS

    def _throttle_reviews(self) -> None:
        now = time.monotonic()
        wait = self._next_reviews_request_at - now
        if wait > 0:
            time.sleep(wait)
        self._next_reviews_request_at = max(now, self._next_reviews_request_at) + MIN_REVIEWS_INTERVAL_SECONDS

    def _get(self, url: str, params: dict, throttle: Callable[[], None]) -> requests.Response:
        # urllib3 Retry stops at the response headers; a body truncated mid-read surfaces after the adapter returns.
        for attempt in range(1, GET_ATTEMPTS + 1):
            throttle()
            try:
                return self.session.get(url, params=params, timeout=TIMEOUT)
            except TRANSIENT_NETWORK_ERRORS as e:
                if attempt == GET_ATTEMPTS:
                    raise
                delay = GET_BACKOFF_SECONDS * 2 ** (attempt - 1)
                logger.warning(
                    f"storeleads GET {url} failed ({type(e).__name__}: {e}); retry {attempt}/{GET_ATTEMPTS - 1} in {delay}s"
                )
                time.sleep(delay)
        raise AssertionError("unreachable")

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            self._throttle_list()
            resp = self.session.get(f"{BASE}all/domain", params={"page_size": 1}, timeout=TIMEOUT)
            resp.raise_for_status()
            return True, None
        except Exception as e:  # noqa: BLE001 -- surfaced to the caller as (False, reason)
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get(self, pagination: dict = None) -> SourceIteration:
        pagination = pagination or {}
        stream = self.config.stream

        if stream == StoreleadsStreams.DOMAINS.value:
            return self._get_domains_page(pagination)
        if stream == StoreleadsStreams.HAS_CART_LINK_DOMAINS.value:
            return self._get_cart_link_page(pagination)
        if stream == StoreleadsStreams.APPS.value:
            return self._get_apps_page(pagination)
        if stream == StoreleadsStreams.TECHNOLOGIES.value:
            return self._get_apps_or_tech_page("all/technology", "technologies", "name", pagination)
        if stream == StoreleadsStreams.APP_REVIEWS.value:
            return self._get_app_reviews_page(pagination)
        raise ValueError(f"unknown storeleads stream {stream!r}")

    def _get_domains_page(self, pagination: dict) -> SourceIteration:
        params: dict[str, Any] = {"page_size": DOMAINS_PAGE_SIZE}
        cursor = pagination.get("cursor")
        if cursor:
            params["cursor"] = cursor
        resp = self._get(f"{BASE}all/domain", params, self._throttle_list)
        resp.raise_for_status()
        page_bytes = len(resp.content)
        data = resp.json()
        domains = data.get("domains", [])
        logger.info(f"storeleads domains page: {len(domains)} records, {page_bytes} bytes")
        records = [SourceRecord(id=str(d["name"]), data=d) for d in domains]
        next_pagination = {"cursor": data["next_cursor"]} if data.get("has_next_page") else {}
        return SourceIteration(next_pagination=next_pagination, records=records)

    def _get_cart_link_page(self, pagination: dict) -> SourceIteration:
        params: dict[str, Any] = {
            "page_size": CART_LINK_PAGE_SIZE,
            "f:p": CART_LINK_PLATFORMS,
            "f:p:op": "or",
            "fields": "name,platform",
            "f:hcl": "true",
        }
        cursor = pagination.get("cursor")
        if cursor:
            params["cursor"] = cursor
        resp = self._get(f"{BASE}all/domain", params, self._throttle_list)
        resp.raise_for_status()
        page_bytes = len(resp.content)
        data = resp.json()
        domains = data.get("domains", [])  # ignore the `facets` key alongside it
        logger.info(f"storeleads has_cart_link_domains page: {len(domains)} records, {page_bytes} bytes")
        records = [SourceRecord(id=str(d["name"]), data=d) for d in domains]
        next_pagination = {"cursor": data["next_cursor"]} if data.get("has_next_page") else {}
        return SourceIteration(next_pagination=next_pagination, records=records)

    def _get_apps_or_tech_page(self, path: str, container_key: str, id_field: str, pagination: dict) -> SourceIteration:
        page = pagination.get("page", 0)
        resp = self._get(f"{BASE}{path}", {"page": page, "page_size": APPS_TECH_PAGE_SIZE}, self._throttle_list)
        resp.raise_for_status()
        data = resp.json()
        items = data.get(container_key, [])
        total = data.get("total", 0)
        records = [SourceRecord(id=str(item[id_field]), data=item) for item in items]
        total_pages = math.ceil(total / APPS_TECH_PAGE_SIZE) if total else 0
        next_pagination = {"page": page + 1} if page < total_pages - 1 else {}
        return SourceIteration(next_pagination=next_pagination, records=records)

    def _walk_apps_page(
        self, pagination: dict, partitions: Tuple[Tuple[str, dict], ...], max_pages: Optional[int] = None
    ) -> Tuple[List[dict], dict]:
        """One all/app page -> (unseen apps, next pagination). Dedups on `id`; `token` is only unique per platform."""
        part = pagination.get("part", 0)
        page = pagination.get("page", 0)
        if (part or page) and not self._apps_part_totals:
            raise RuntimeError(
                f"storeleads all/app resumed at partition {part} page {page} with no in-memory walk state; "
                "refusing to publish a partial table"
            )

        name, filters = partitions[part]
        params: dict[str, Any] = {"page": page, "page_size": APPS_TECH_PAGE_SIZE, **filters}
        resp = self._get(f"{BASE}all/app", params, self._throttle_list)
        resp.raise_for_status()
        data = resp.json()
        apps = data.get("apps", [])
        total = data.get("total")
        # Ending early would publish a partial table via WRITE_TRUNCATE; a failed run keeps the last one.
        # No partition is legitimately empty, yet the API can return a 200 with `total: 0` mid-walk.
        if not total:
            meta = {k: v for k, v in data.items() if k != "apps"}
            raise RuntimeError(
                f"storeleads all/app partition {name!r} page {page} returned no `total` ({len(apps)} apps, {meta}); "
                "refusing to end the walk early"
            )
        total_pages = math.ceil(total / APPS_TECH_PAGE_SIZE)
        if total_pages > MAX_PAGE_INDEX + 1:
            raise RuntimeError(
                f"storeleads all/app partition {name!r} has {total} apps ({total_pages} pages), past the "
                f"{MAX_PAGE_INDEX + 1}-page API cap; rebalance the category set"
            )
        if not apps and page + 1 < total_pages:
            raise RuntimeError(
                f"storeleads all/app partition {name!r} page {page} was empty but total implies {total_pages} pages; "
                "refusing to end the walk early"
            )
        self._apps_part_totals[name] = total
        self._apps_pages += 1

        new_apps = []
        for a in apps:
            app_id = str(a["id"])
            if app_id in self._apps_seen:
                self._apps_duplicates += 1
                continue
            self._apps_seen.add(app_id)
            new_apps.append(a)

        capped = max_pages is not None and self._apps_pages >= max_pages
        if page + 1 < total_pages and not capped:
            return new_apps, {"part": part, "page": page + 1}
        if part + 1 < len(partitions) and not capped:
            return new_apps, {"part": part + 1, "page": 0}
        logger.info(
            f"storeleads all/app walk done: {len(self._apps_seen)} unique apps in {self._apps_pages} pages, "
            f"partition totals {self._apps_part_totals}, {self._apps_duplicates} duplicates dropped"
        )
        return new_apps, {}

    def _get_apps_page(self, pagination: dict) -> SourceIteration:
        apps, next_pagination = self._walk_apps_page(pagination, APPS_PARTITIONS)
        records = [SourceRecord(id=str(a["token"]), data=a) for a in apps]
        return SourceIteration(next_pagination=next_pagination, records=records)

    # app_reviews scans all/app up front rather than interleaving it with the reviews calls: all/app is
    # page-paginated and shifts under a slow walk, so apps would drift across pages and be skipped.
    # review_count is often NULL and understates when set, so only a short page ends an app.

    def _scan_apps_page(self, pagination: dict) -> SourceIteration:
        scanned = self._scan_cursor or {"part": 0, "page": 0}
        apps, self._scan_cursor = self._walk_apps_page(scanned, SHOPIFY_PARTITIONS, self.config.max_review_scan_pages)
        for a in apps:
            if a.get("platform") != REVIEWS_PLATFORM:
                continue
            token = str(a["token"])
            if token in self._app_tokens_seen:
                self._scan_duplicates += 1
                continue
            self._app_tokens_seen.add(token)
            self._app_tokens.append(token)

        logger.info(
            f"storeleads app_reviews scan: partition {scanned['part']} page {scanned['page']}, "
            f"{len(self._app_tokens)} shopify apps, {self._scan_duplicates} duplicate tokens dropped"
        )
        if not self._scan_cursor:
            self._scan_done = True
            if not self._app_tokens:
                return SourceIteration(next_pagination={}, records=[])

        next_pagination = {"idx": pagination.get("idx", 0), "review_page": pagination.get("review_page", 0)}
        return SourceIteration(next_pagination=next_pagination, records=[])

    def _get_app_reviews_page(self, pagination: dict) -> SourceIteration:
        if not self._scan_done:
            return self._scan_apps_page(pagination)

        idx = pagination.get("idx", 0)
        review_page = pagination.get("review_page", 0)
        if idx >= len(self._app_tokens):
            logger.info(f"storeleads app_reviews: all {len(self._app_tokens)} shopify apps done")
            return SourceIteration(next_pagination={}, records=[])

        token = self._app_tokens[idx]
        resp = self._get(
            f"{BASE}all/app/{REVIEWS_PLATFORM}.{token}/reviews",
            {"page": review_page, "page_size": REVIEWS_PAGE_SIZE},
            self._throttle_reviews,
        )
        if resp.status_code == 404:
            reviews = []
        else:
            resp.raise_for_status()
            reviews = resp.json().get("reviews", [])

        records = []
        for review in reviews:
            rec = dict(review)
            rec["app_token"] = token
            rec["app_platform"] = REVIEWS_PLATFORM
            # Reviews carry no id of their own.
            review_id = hashlib.sha1(f"{token}|{json.dumps(rec, sort_keys=True, default=str)}".encode()).hexdigest()
            records.append(SourceRecord(id=review_id, data=rec))

        done_with_app = len(reviews) < REVIEWS_PAGE_SIZE

        logger.info(
            f"storeleads app_reviews: {token} (app {idx + 1}/{len(self._app_tokens)}) "
            f"page={review_page} got {len(reviews)} reviews, done={done_with_app}"
        )

        if done_with_app:
            next_pagination = {"idx": idx + 1, "review_page": 0}
        else:
            next_pagination = {"idx": idx, "review_page": review_page + 1}
        return SourceIteration(next_pagination=next_pagination, records=records)
