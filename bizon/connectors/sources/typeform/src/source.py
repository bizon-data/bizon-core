"""Typeform source: forms (with full definitions), workspaces, and incremental responses across every form."""

from datetime import timedelta
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field, model_validator
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.typeform.com"
REQUEST_TIMEOUT = (10, 60)

FORMS_PAGE_SIZE = 200
WORKSPACES_PAGE_SIZE = 200
# Typeform's documented cap for /forms/{id}/responses; the docs point at it
# indirectly ("if your typeform has more than 1000 responses, use since/until
# or before/after") rather than stating a literal max.
RESPONSES_PAGE_SIZE = 1000

# The Responses API can take up to ~30 min to surface a just-submitted response (documented latency), so
# without an overlap such a response could fall behind the `since` watermark and never be fetched.
# Responses are appended, so re-fetched duplicates must be deduplicated downstream on the record id.
DEFAULT_LOOKBACK_MINUTES = 60


class TypeformStreams(str, Enum):
    FORMS = "forms"
    WORKSPACES = "workspaces"
    RESPONSES = "responses"


class TypeformAuth(AuthBase):
    def __init__(self, access_token: str):
        self.access_token = access_token

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.access_token}"
        return request


class TypeformSourceConfig(SourceConfig):
    stream: TypeformStreams
    access_token: str = Field(..., description="Typeform personal access token")
    start_date: Optional[str] = Field(
        None, description="ISO-8601 lower bound on submitted_at for the first responses sync"
    )
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES
    # check_connection reports non-2xx responses, and a form deleted between list and detail fetch 404s.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )

    @model_validator(mode="after")
    def _responses_need_a_start_date(self):
        if self.stream == TypeformStreams.RESPONSES and not self.start_date:
            raise ValueError("start_date is required for the responses stream")
        return self


class TypeformSource(AbstractSource):
    def __init__(self, config: TypeformSourceConfig):
        super().__init__(config)
        self.config: TypeformSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return TypeformAuth(self.config.access_token)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in TypeformStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return TypeformSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/forms", params={"page_size": 1}, timeout=REQUEST_TIMEOUT)
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    # The list endpoint omits fields/logic/welcome_screens/thankyou_screens, so each form is re-fetched from
    # GET /forms/{id}. Sequential, not threaded: Typeform caps at ~2 req/s. A form with no questions has no
    # `fields` key even on the detail endpoint.

    def _get_form_detail(self, form_id: str) -> Optional[dict]:
        response = self.session.get(f"{BASE_URL}/forms/{form_id}", timeout=REQUEST_TIMEOUT)
        if response.status_code == 404:
            logger.warning(f"Form {form_id} returned 404 on detail fetch (deleted since list page); skipping")
            return None
        response.raise_for_status()
        return response.json()

    def get_forms(self, pagination: dict = None) -> SourceIteration:
        page = (pagination or {}).get("page", 1)
        response = self.session.get(
            f"{BASE_URL}/forms",
            params={"page": page, "page_size": FORMS_PAGE_SIZE},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        page_count = data.get("page_count", 1)

        records = []
        for item in items:
            detail = self._get_form_detail(str(item["id"]))
            if detail is None:
                continue
            merged = {**item, **detail}
            records.append(SourceRecord(id=str(item["id"]), data=merged))

        next_pagination = {"page": page + 1} if page < page_count else {}

        logger.info(f"Fetched {len(records)} forms (page {page}/{page_count})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_workspaces(self, pagination: dict = None) -> SourceIteration:
        page = (pagination or {}).get("page", 1)
        response = self.session.get(
            f"{BASE_URL}/workspaces",
            params={"page": page, "page_size": WORKSPACES_PAGE_SIZE},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        page_count = data.get("page_count", 1)

        records = [SourceRecord(id=str(item["id"]), data=item) for item in items]
        next_pagination = {"page": page + 1} if page < page_count else {}

        logger.info(f"Fetched {len(records)} workspaces (page {page}/{page_count})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    # Outer loop: forms (small, full list loaded once). Inner loop: response
    # pages for the form currently being drained, newest-first via `before`
    # cursor. Both loop states live in `next_pagination` so each bizon get()
    # call does bounded work -- one page of one form -- and the whole walk
    # survives a pod restart mid-form.

    def _list_all_form_ids(self) -> List[str]:
        form_ids: List[str] = []
        page = 1
        while True:
            response = self.session.get(
                f"{BASE_URL}/forms",
                params={"page": page, "page_size": FORMS_PAGE_SIZE},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            form_ids.extend(str(item["id"]) for item in items)
            page_count = data.get("page_count", 1)
            if page >= page_count or not items:
                break
            page += 1
        return form_ids

    def _get_responses_page(self, form_id: str, since: Optional[str], before: Optional[str]) -> Tuple[list, bool]:
        """Fetch one page of responses for a form.

        has_more is True only when the page came back full (== page_size) --
        the endpoint has no next-page link, and total_items/page_count describe
        the whole filtered set, not "more after this cursor". A short page
        (including empty) is therefore the only signal this form is exhausted,
        which is what makes the inner loop provably terminate.
        """
        # No explicit `sort`: the API 400s with "can't use before/after param
        # together with sort" once a cursor is present (confirmed against the
        # live API), and the default sort for completed responses is already
        # submitted_at,desc -- exactly the newest-first order this pagination
        # relies on.
        params: dict = {"page_size": RESPONSES_PAGE_SIZE}
        if since:
            params["since"] = since
        if before:
            params["before"] = before

        response = self.session.get(
            f"{BASE_URL}/forms/{form_id}/responses",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        return items, len(items) == RESPONSES_PAGE_SIZE

    def _get_responses(self, pagination: dict, since: Optional[str]) -> SourceIteration:
        pagination = pagination or {}

        if "remaining_form_ids" not in pagination:
            remaining_form_ids = self._list_all_form_ids()
            logger.info(f"Loaded {len(remaining_form_ids)} forms for responses sync")
        else:
            remaining_form_ids = list(pagination["remaining_form_ids"])

        current_form_id = pagination.get("current_form_id")
        before_token = pagination.get("before_token")

        if current_form_id is None:
            if not remaining_form_ids:
                return SourceIteration(records=[], next_pagination={})
            current_form_id = remaining_form_ids.pop(0)
            before_token = None

        items, has_more = self._get_responses_page(current_form_id, since=since, before=before_token)

        records = [SourceRecord(id=str(item.get("response_id") or item["token"]), data=item) for item in items]

        if has_more and items:
            next_pagination = {
                "remaining_form_ids": remaining_form_ids,
                "current_form_id": current_form_id,
                "before_token": items[-1]["token"],
            }
        elif remaining_form_ids:
            next_pagination = {"remaining_form_ids": remaining_form_ids}
        else:
            next_pagination = {}

        logger.info(
            f"Fetched {len(records)} responses for form {current_form_id} "
            f"(has_more={has_more}, remaining_forms={len(remaining_form_ids)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == TypeformStreams.FORMS:
            return self.get_forms(pagination)
        if stream == TypeformStreams.WORKSPACES:
            return self.get_workspaces(pagination)
        if stream == TypeformStreams.RESPONSES:
            return self._get_responses(pagination or {}, since=self.config.start_date)
        raise NotImplementedError(f"Stream '{stream}' is not implemented for Typeform source")

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream != TypeformStreams.RESPONSES:
            raise NotImplementedError(f"Incremental sync not supported for stream '{stream}'")

        floor = source_state.last_run - timedelta(minutes=self.config.lookback_minutes)
        since = floor.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self._get_responses(pagination or {}, since=since)
