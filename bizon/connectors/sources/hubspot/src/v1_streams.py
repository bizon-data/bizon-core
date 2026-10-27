"""Streams on HubSpot's legacy v1 APIs: email events and form submissions."""

from typing import List, Optional, Tuple

import pendulum
from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord

from .hubspot_base import URL_BASE, HubSpotBaseSource, HubSpotBaseSourceConfig, to_ms

URL_EMAIL_EVENTS = f"{URL_BASE}/email/public/v1/events"
URL_FORMS_LIST = f"{URL_BASE}/marketing/v3/forms"
URL_FORM_SUBMISSIONS = f"{URL_BASE}/form-integrations/v1/submissions/forms"

EMAIL_EVENTS_PAGE_SIZE = 1000
FORM_SUBMISSIONS_PAGE_SIZE = 50

# The v1 APIs make records visible late; re-reading this much keeps a late arrival from being skipped for good.
V1_OVERLAP_SECONDS = 6 * 3600


class HubSpotEmailEventsSourceConfig(HubSpotBaseSourceConfig):
    start_date: Optional[str] = Field(
        None,
        description="ISO-8601 lower bound on the event time for a full refresh or a first incremental run. "
        "Unset fetches every event.",
    )


class HubSpotEmailEventsSource(HubSpotBaseSource):
    """Marketing email events from `email/public/v1/events`, incremental on `startTimestamp`."""

    def __init__(self, config: HubSpotEmailEventsSourceConfig):
        super().__init__(config)
        self.config: HubSpotEmailEventsSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return ["email_events"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotEmailEventsSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _fetch_page(self, start_ms: Optional[int], offset: Optional[str]) -> Tuple[list, Optional[str]]:
        params = {"limit": EMAIL_EVENTS_PAGE_SIZE}
        if start_ms is not None:
            params["startTimestamp"] = start_ms
        if offset:
            params["offset"] = offset
        data = self._call(method="GET", url=URL_EMAIL_EVENTS, params=params).json()
        next_offset = data.get("offset") if data.get("hasMore") else None
        return data.get("events", []), next_offset

    def _dispatch(self, start_ms: Optional[int], pagination: dict = None) -> SourceIteration:
        events, next_offset = self._fetch_page(start_ms, (pagination or {}).get("offset"))
        records = [SourceRecord(id=str(e["id"]), data=e) for e in events]
        return SourceIteration(next_pagination={"offset": next_offset} if next_offset else {}, records=records)

    def get(self, pagination: dict = None) -> SourceIteration:
        start_ms = to_ms(self.config.start_date) if self.config.start_date else None
        return self._dispatch(start_ms, pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        return self._dispatch(to_ms(source_state.last_run) - V1_OVERLAP_SECONDS * 1000, pagination)


class HubSpotFormSubmissionsSourceConfig(HubSpotBaseSourceConfig):
    start_date: Optional[str] = Field(
        None,
        description="ISO-8601 lower bound on `submittedAt` for a full refresh or a first incremental run. "
        "Unset fetches every submission.",
    )


class HubSpotFormSubmissionsSource(HubSpotBaseSource):
    """The API is newest-first with no timestamp filter, so each form is walked until a page reaches the cutoff."""

    def __init__(self, config: HubSpotFormSubmissionsSourceConfig):
        super().__init__(config)
        self.config: HubSpotFormSubmissionsSourceConfig = config
        self._form_ids: Optional[List[str]] = None

    @staticmethod
    def streams() -> List[str]:
        return ["form_submissions"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotFormSubmissionsSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _list_all_form_ids(self) -> List[str]:
        # Walked in full every run so a newly created form is picked up straight away.
        form_ids: List[str] = []
        after = None
        while True:
            params = {"limit": 100}
            if after:
                params["after"] = after
            data = self._call(method="GET", url=URL_FORMS_LIST, params=params).json()
            form_ids.extend(f["id"] for f in data.get("results", []))
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                return form_ids

    def _resolve_form_ids(self) -> List[str]:
        if self._form_ids is None:
            self._form_ids = self._list_all_form_ids()
        return self._form_ids

    def _fetch_page(self, form_id: str, after: Optional[str]) -> Tuple[list, Optional[str]]:
        params = {"limit": FORM_SUBMISSIONS_PAGE_SIZE}
        if after:
            params["after"] = after
        data = self._call(method="GET", url=f"{URL_FORM_SUBMISSIONS}/{form_id}", params=params).json()
        return data.get("results", []), data.get("paging", {}).get("next", {}).get("after")

    def _dispatch(self, cutoff_ms: Optional[int], pagination: dict = None) -> SourceIteration:
        pagination = pagination or {}
        form_ids = self._resolve_form_ids()
        form_idx = pagination.get("form_idx", 0)
        after = pagination.get("after")

        while form_idx < len(form_ids):
            form_id = form_ids[form_idx]
            results, next_after = self._fetch_page(form_id, after)

            if not results:
                form_idx, after = form_idx + 1, None
                continue

            records = []
            hit_cutoff = False
            for submission in results:
                if cutoff_ms is not None and submission["submittedAt"] <= cutoff_ms:
                    hit_cutoff = True
                    break
                body = dict(submission)
                body["formId"] = form_id
                # Submissions are immutable and the API returns no updatedAt, so submittedAt stands in for it.
                body.setdefault("updatedAt", submission["submittedAt"])
                records.append(
                    SourceRecord(
                        id=str(submission["conversionId"]),
                        data=body,
                        timestamp=pendulum.from_timestamp(submission["submittedAt"] / 1000),
                    )
                )

            if hit_cutoff or not next_after:
                return SourceIteration(next_pagination={"form_idx": form_idx + 1}, records=records)

            return SourceIteration(next_pagination={"form_idx": form_idx, "after": next_after}, records=records)

        return SourceIteration(next_pagination={}, records=[])

    def get(self, pagination: dict = None) -> SourceIteration:
        cutoff_ms = to_ms(self.config.start_date) if self.config.start_date else None
        return self._dispatch(cutoff_ms, pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        return self._dispatch(to_ms(source_state.last_run) - V1_OVERLAP_SECONDS * 1000, pagination)
