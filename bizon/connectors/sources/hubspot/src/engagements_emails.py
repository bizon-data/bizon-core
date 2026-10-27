from typing import List, Optional, Tuple

from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration

from .associations import fetch_id_lists
from .hubspot_base import (
    SEARCH_PAGE_SIZE_MAX,
    HubSpotBaseSource,
    HubSpotBaseSourceConfig,
    PropertiesConfig,
    PropertiesStrategy,
    id_paged_iteration,
    record_with_timestamp,
    to_ms,
)
from .hubspot_objects import SEARCH_OVERLAP_SECONDS

_EMAILS_ASSOCIATION_OBJECTS = ["contacts", "companies", "deals", "tickets"]


class HubSpotEngagementsEmailsSourceConfig(HubSpotBaseSourceConfig):
    properties: PropertiesConfig = PropertiesConfig(strategy=PropertiesStrategy.ALL, selected_properties=None)
    start_date: Optional[str] = Field(
        None,
        description="ISO-8601 lower bound on `hs_lastmodifieddate` for a full refresh or a first incremental run. "
        "Unset fetches every email.",
    )
    page_size: int = Field(SEARCH_PAGE_SIZE_MAX, ge=1, le=SEARCH_PAGE_SIZE_MAX)


class HubSpotEngagementsEmailsSource(HubSpotBaseSource):
    """Email engagements through the search API, windowed on `hs_lastmodifieddate`. Search never returns
    associations, so each page is followed by one v4 batch/read per associated object type, whose id lists are
    merged into the records under the object type's name."""

    def __init__(self, config: HubSpotEngagementsEmailsSourceConfig):
        super().__init__(config)
        self.config: HubSpotEngagementsEmailsSourceConfig = config
        self._properties: Optional[List[str]] = None

    @staticmethod
    def streams() -> List[str]:
        return ["engagements_emails"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotEngagementsEmailsSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _resolve_properties(self) -> List[str]:
        if self._properties is None:
            self._properties = self.resolve_properties("emails", self.config.properties)
        return self._properties

    def _dispatch(self, window: Optional[Tuple[str, int, int]], pagination: dict = None) -> SourceIteration:
        results = self.search_page(
            object_type="emails",
            properties=self._resolve_properties(),
            limit=self.config.page_size,
            last_id=(pagination or {}).get("last_id"),
            window=window,
        )
        if not results:
            return SourceIteration(next_pagination={}, records=[])

        assoc = fetch_id_lists(self, "emails", _EMAILS_ASSOCIATION_OBJECTS, [str(r["id"]) for r in results])
        records = []
        for r in results:
            body = dict(r)
            for to_object in _EMAILS_ASSOCIATION_OBJECTS:
                body[to_object] = assoc[to_object].get(str(r["id"]), [])
            records.append(record_with_timestamp(body))
        return id_paged_iteration(results, records)

    def get(self, pagination: dict = None) -> SourceIteration:
        window = None
        if self.config.start_date:
            window = ("hs_lastmodifieddate", to_ms(self.config.start_date), self.run_start_ms())
        return self._dispatch(window, pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        window = (
            "hs_lastmodifieddate",
            to_ms(source_state.last_run) - SEARCH_OVERLAP_SECONDS * 1000,
            self.run_start_ms(source_state),
        )
        return self._dispatch(window, pagination)
