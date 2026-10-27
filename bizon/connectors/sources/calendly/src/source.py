"""Calendly source: organization users, scheduled events and invitees, routing forms and event types (API v2)."""

from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.calendly.com"


class CalendlyStreams(str, Enum):
    USERS = "users"
    SCHEDULED_EVENTS = "scheduled_events"
    SCHEDULED_EVENT_INVITEES = "scheduled_event_invitees"
    ROUTING_FORMS = "routing_forms"
    ROUTING_FORM_SUBMISSIONS = "routing_form_submissions"
    EVENT_TYPES = "event_types"


class CalendlySourceConfig(SourceConfig):
    stream: CalendlyStreams
    api_key: str = Field(..., description="Calendly personal access token")
    organization_uri: str = Field(
        ..., description="Organization URI, e.g. https://api.calendly.com/organizations/<uuid>"
    )
    start_date: str = Field(..., description="ISO-8601 lower bound on scheduled event start_time for the first sync")
    # Non-2xx responses are inspected (check_connection, error logging) before raising.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class CalendlyAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


def _extract_uuid(uri: str) -> str:
    """Extract the UUID from a Calendly resource URI (last 36 chars)."""
    return uri[-36:]


class CalendlySource(AbstractSource):
    def __init__(self, config: CalendlySourceConfig):
        super().__init__(config)
        self.config: CalendlySourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return CalendlyAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in CalendlyStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return CalendlySourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/users/me")
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _fetch_paginated(self, url: str, params: dict = None) -> Tuple[list, Optional[str]]:
        """Fetch a single page from a Calendly endpoint.

        Returns (records, next_page_url). next_page_url is None when there are no more pages.
        Calendly's page_token is bound to the exact query params of the first request,
        so the full next_page URL returned by the API for subsequent pages.
        """
        if params is not None:
            params = dict(params)
            params.setdefault("count", 100)
            response = self.session.get(url, params=params)
        else:
            # url is a full next_page URL with params already encoded
            response = self.session.get(url)

        if not response.ok:
            logger.error(f"Calendly API error {response.status_code}: {response.text}")
            response.raise_for_status()

        data = response.json()
        collection = data.get("collection", [])
        next_page_url = data.get("pagination", {}).get("next_page")
        return collection, next_page_url

    def _fetch_all_pages(self, url: str, params: dict = None) -> list:
        """Fetch all pages from a paginated endpoint."""
        all_records = []
        records, next_page_url = self._fetch_paginated(url, params)
        all_records.extend(records)
        while next_page_url:
            records, next_page_url = self._fetch_paginated(next_page_url)
            all_records.extend(records)
        return all_records

    def get_users(self, pagination: dict = None) -> SourceIteration:
        if pagination and pagination.get("next_page_url"):
            records_raw, next_page_url = self._fetch_paginated(pagination["next_page_url"])
        else:
            params = {"organization": self.config.organization_uri}
            records_raw, next_page_url = self._fetch_paginated(f"{BASE_URL}/organization_memberships", params)

        records = [SourceRecord(id=r["uri"], data=r) for r in records_raw]
        next_pagination = {"next_page_url": next_page_url} if next_page_url else {}

        logger.info(f"Fetched {len(records)} users (has_more={bool(next_page_url)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_scheduled_events(self, pagination: dict = None, min_start_time: str = None) -> SourceIteration:
        if pagination and pagination.get("next_page_url"):
            records_raw, next_page_url = self._fetch_paginated(pagination["next_page_url"])
        else:
            params = {
                "organization": self.config.organization_uri,
                "min_start_time": min_start_time or self.config.start_date,
                "sort": "start_time:asc",
            }
            records_raw, next_page_url = self._fetch_paginated(f"{BASE_URL}/scheduled_events", params)

        records = [
            SourceRecord(
                id=r["uri"],
                data={**r, "scheduled_event_id": _extract_uuid(r["uri"])},
            )
            for r in records_raw
        ]
        next_pagination = {"next_page_url": next_page_url} if next_page_url else {}

        logger.info(f"Fetched {len(records)} scheduled events (has_more={bool(next_page_url)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_scheduled_event_invitees(self, pagination: dict = None, min_start_time: str = None) -> SourceIteration:
        # Lazily paginate through parent events using time-based offset instead of
        # page_token (which expires between bizon iterations). Each get() call fetches
        # one page of parents sorted by start_time, processes their invitees, then stores
        # the last event's start_time for the next call.

        effective_min_start_time = (
            pagination.get("next_min_start_time")
            if pagination and pagination.get("next_min_start_time")
            else (min_start_time or self.config.start_date)
        )

        params = {
            "organization": self.config.organization_uri,
            "min_start_time": effective_min_start_time,
            "sort": "start_time:asc",
        }
        parent_records, parent_next_page_url = self._fetch_paginated(f"{BASE_URL}/scheduled_events", params)

        all_records = []
        for event in parent_records:
            event_id = _extract_uuid(event["uri"])
            invitees = self._fetch_all_pages(f"{BASE_URL}/scheduled_events/{event_id}/invitees")
            for inv in invitees:
                all_records.append(
                    SourceRecord(
                        id=inv["uri"],
                        data={**inv, "scheduled_event_id": event_id},
                    )
                )

        # Use last event's start_time as offset for the next batch
        next_pagination = {}
        if parent_next_page_url and parent_records:
            last_start_time = parent_records[-1].get("start_time", effective_min_start_time)
            next_pagination = {"next_min_start_time": last_start_time}

        logger.info(
            f"Fetched {len(all_records)} invitees from {len(parent_records)} events "
            f"(more_parents={bool(parent_next_page_url)})"
        )
        return SourceIteration(records=all_records, next_pagination=next_pagination)

    def get_routing_forms(self, pagination: dict = None) -> SourceIteration:
        if pagination and pagination.get("next_page_url"):
            records_raw, next_page_url = self._fetch_paginated(pagination["next_page_url"])
        else:
            params = {"organization": self.config.organization_uri}
            records_raw, next_page_url = self._fetch_paginated(f"{BASE_URL}/routing_forms", params)

        records = [
            SourceRecord(
                id=r["uri"],
                data={**r, "form_id": _extract_uuid(r["uri"])},
            )
            for r in records_raw
        ]
        next_pagination = {"next_page_url": next_page_url} if next_page_url else {}

        logger.info(f"Fetched {len(records)} routing forms (has_more={bool(next_page_url)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _load_parent_form_uris(self) -> list:
        """Fetch all routing form URIs."""
        params = {"organization": self.config.organization_uri}
        forms = self._fetch_all_pages(f"{BASE_URL}/routing_forms", params)
        return [f["uri"] for f in forms]

    def get_routing_form_submissions(self, pagination: dict = None) -> SourceIteration:
        # First call: load all parent form URIs
        if not pagination or "remaining_forms" not in pagination:
            form_uris = self._load_parent_form_uris()
            logger.info(f"Loaded {len(form_uris)} parent forms for submissions")
            if not form_uris:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_forms": form_uris}

        remaining = pagination["remaining_forms"]
        all_records = []

        # Process one parent form per call
        while remaining:
            form_uri = remaining.pop(0)
            submissions = self._fetch_all_pages(f"{BASE_URL}/routing_form_submissions", params={"form": form_uri})

            for sub in submissions:
                all_records.append(SourceRecord(id=sub["uri"], data=sub))

            if all_records:
                break

        next_pagination = {"remaining_forms": remaining} if remaining else {}

        logger.info(f"Fetched {len(all_records)} routing form submissions (remaining_parents={len(remaining)})")
        return SourceIteration(records=all_records, next_pagination=next_pagination)

    def get_event_types(self, pagination: dict = None) -> SourceIteration:
        if pagination and pagination.get("next_page_url"):
            records_raw, next_page_url = self._fetch_paginated(pagination["next_page_url"])
        else:
            params = {"organization": self.config.organization_uri}
            records_raw, next_page_url = self._fetch_paginated(f"{BASE_URL}/event_types", params)

        records = [SourceRecord(id=r["uri"], data=r) for r in records_raw]
        next_pagination = {"next_page_url": next_page_url} if next_page_url else {}

        logger.info(f"Fetched {len(records)} event types (has_more={bool(next_page_url)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == CalendlyStreams.USERS:
            return self.get_users(pagination)
        if stream == CalendlyStreams.SCHEDULED_EVENTS:
            return self.get_scheduled_events(pagination)
        if stream == CalendlyStreams.SCHEDULED_EVENT_INVITEES:
            return self.get_scheduled_event_invitees(pagination)
        if stream == CalendlyStreams.ROUTING_FORMS:
            return self.get_routing_forms(pagination)
        if stream == CalendlyStreams.ROUTING_FORM_SUBMISSIONS:
            return self.get_routing_form_submissions(pagination)
        if stream == CalendlyStreams.EVENT_TYPES:
            return self.get_event_types(pagination)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Calendly source")

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        min_start_time = source_state.last_run.strftime("%Y-%m-%dT%H:%M:%SZ")
        stream = self.config.stream

        if stream == CalendlyStreams.USERS:
            # No time filtering on /organization_memberships — always full fetch
            return self.get_users(pagination)
        if stream == CalendlyStreams.SCHEDULED_EVENTS:
            return self.get_scheduled_events(pagination, min_start_time=min_start_time)
        if stream == CalendlyStreams.SCHEDULED_EVENT_INVITEES:
            return self.get_scheduled_event_invitees(pagination, min_start_time=min_start_time)

        raise NotImplementedError(f"Incremental sync not supported for stream '{stream}'")
