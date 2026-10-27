from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration

from .hubspot_base import URL_BASE, HubSpotBaseSource, HubSpotBaseSourceConfig, record_with_timestamp, to_ms

ARCHIVED_OVERLAP_SECONDS = 300


class HubSpotArchivedStreams(str, Enum):
    ARCHIVED_CONTACTS = "archived_contacts"
    ARCHIVED_DEALS = "archived_deals"
    ARCHIVED_COMPANIES = "archived_companies"


_ARCHIVED_OBJECT = {
    HubSpotArchivedStreams.ARCHIVED_CONTACTS: "contacts",
    HubSpotArchivedStreams.ARCHIVED_DEALS: "deals",
    HubSpotArchivedStreams.ARCHIVED_COMPANIES: "companies",
}


class HubSpotArchivedSourceConfig(HubSpotBaseSourceConfig):
    page_size: int = Field(100, ge=1, le=100)
    start_date: Optional[str] = Field(
        None,
        description="ISO-8601 lower bound on `archivedAt` for a full refresh or a first incremental run. "
        "Unset emits every archived record.",
    )


class HubSpotArchivedSource(HubSpotBaseSource):
    """`archived=true` has no date filter and isn't ordered by `archivedAt`, so every run walks the whole list. The
    list only carries recently archived records: use incremental to accumulate them."""

    def __init__(self, config: HubSpotArchivedSourceConfig):
        super().__init__(config)
        self.config: HubSpotArchivedSourceConfig = config
        self.object = _ARCHIVED_OBJECT[HubSpotArchivedStreams(config.stream)]

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in HubSpotArchivedStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotArchivedSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _dispatch(self, cutoff_ms: Optional[int], pagination: dict = None) -> SourceIteration:
        params: Dict[str, Any] = {"limit": self.config.page_size, "archived": "true"}
        after = (pagination or {}).get("after")
        if after:
            params["after"] = after

        data = self._call(method="GET", url=f"{URL_BASE}/crm/v3/objects/{self.object}", params=params).json()
        # A record without `archivedAt` is kept rather than silently dropped.
        records = [
            record_with_timestamp(r, timestamp_field="archivedAt")
            for r in data.get("results", [])
            if cutoff_ms is None or not r.get("archivedAt") or to_ms(r["archivedAt"]) >= cutoff_ms
        ]
        next_after = data.get("paging", {}).get("next", {}).get("after")
        return SourceIteration(next_pagination={"after": next_after} if next_after else {}, records=records)

    def get(self, pagination: dict = None) -> SourceIteration:
        cutoff_ms = to_ms(self.config.start_date) if self.config.start_date else None
        return self._dispatch(cutoff_ms, pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        return self._dispatch(to_ms(source_state.last_run) - ARCHIVED_OVERLAP_SECONDS * 1000, pagination)
