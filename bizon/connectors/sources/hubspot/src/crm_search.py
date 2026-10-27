from enum import Enum
from typing import List, Optional, Tuple

from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration

from .hubspot_base import (
    SEARCH_PAGE_SIZE_MAX,
    HubSpotBaseSource,
    HubSpotBaseSourceConfig,
    PropertiesConfig,
    PropertiesStrategy,
    id_paged_iteration,
    last_modified_property,
    record_with_timestamp,
    to_ms,
)
from .hubspot_objects import SEARCH_OVERLAP_SECONDS


class HubSpotCrmSearchStreams(str, Enum):
    CONTACTS = "contacts_search"
    COMPANIES = "companies_search"
    DEALS = "deals_search"


_CRM_SEARCH_OBJECT = {
    HubSpotCrmSearchStreams.CONTACTS: "contacts",
    HubSpotCrmSearchStreams.COMPANIES: "companies",
    HubSpotCrmSearchStreams.DEALS: "deals",
}


class HubSpotCrmSearchSourceConfig(HubSpotBaseSourceConfig):
    properties: PropertiesConfig = PropertiesConfig(strategy=PropertiesStrategy.ALL, selected_properties=None)
    start_date: Optional[str] = Field(
        None,
        description="ISO-8601 lower bound on the last-modified date for a full refresh or a first incremental run. "
        "Unset fetches every record.",
    )
    page_size: int = Field(SEARCH_PAGE_SIZE_MAX, ge=1, le=SEARCH_PAGE_SIZE_MAX, description="Search page size")
    full_scan: bool = Field(
        False,
        description="Ignore the date window and re-fetch every record on every run. Independent of sync_mode: "
        "with full_refresh it replaces the table, with incremental it accumulates snapshots.",
    )


class HubSpotCrmSearchSource(HubSpotBaseSource):
    """Contacts, companies and deals through the search API, windowed on their last-modified property and paged by
    id so neither a full sweep nor a large window hits the search result cap."""

    def __init__(self, config: HubSpotCrmSearchSourceConfig):
        super().__init__(config)
        self.config: HubSpotCrmSearchSourceConfig = config
        self.object = _CRM_SEARCH_OBJECT[HubSpotCrmSearchStreams(config.stream)]
        self._properties: Optional[List[str]] = None

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in HubSpotCrmSearchStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotCrmSearchSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _resolve_properties(self) -> List[str]:
        if self._properties is None:
            self._properties = self.resolve_properties(self.object, self.config.properties)
        return self._properties

    def _dispatch(self, window: Optional[Tuple[str, int, int]], pagination: dict = None) -> SourceIteration:
        results = self.search_page(
            object_type=self.object,
            properties=self._resolve_properties(),
            limit=self.config.page_size,
            last_id=(pagination or {}).get("last_id"),
            window=None if self.config.full_scan else window,
        )
        return id_paged_iteration(results, [record_with_timestamp(r) for r in results])

    def get(self, pagination: dict = None) -> SourceIteration:
        window = None
        if self.config.start_date:
            window = (last_modified_property(self.object), to_ms(self.config.start_date), self.run_start_ms())
        return self._dispatch(window, pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        window = (
            last_modified_property(self.object),
            to_ms(source_state.last_run) - SEARCH_OVERLAP_SECONDS * 1000,
            self.run_start_ms(source_state),
        )
        return self._dispatch(window, pagination)
