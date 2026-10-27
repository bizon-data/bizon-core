from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord

from .hubspot_base import URL_BASE, HubSpotBaseSource, HubSpotBaseSourceConfig


class HubSpotSimpleStreams(str, Enum):
    PROPERTIES_CONTACTS = "properties_contacts"
    PROPERTIES_COMPANIES = "properties_companies"
    PROPERTIES_DEALS = "properties_deals"
    DEAL_PIPELINES = "deal_pipelines"
    ENGAGEMENTS_NOTES = "engagements_notes"
    ENGAGEMENTS_CALLS = "engagements_calls"
    ENGAGEMENTS_TASKS = "engagements_tasks"
    ENGAGEMENTS_MEETINGS = "engagements_meetings"
    LINE_ITEMS = "line_items"


_PROPERTIES_OBJECT = {
    HubSpotSimpleStreams.PROPERTIES_CONTACTS: "contacts",
    HubSpotSimpleStreams.PROPERTIES_COMPANIES: "companies",
    HubSpotSimpleStreams.PROPERTIES_DEALS: "deals",
}
_ENGAGEMENT_OBJECT = {
    HubSpotSimpleStreams.ENGAGEMENTS_NOTES: "notes",
    HubSpotSimpleStreams.ENGAGEMENTS_CALLS: "calls",
    HubSpotSimpleStreams.ENGAGEMENTS_TASKS: "tasks",
    HubSpotSimpleStreams.ENGAGEMENTS_MEETINGS: "meetings",
}
_ENGAGEMENT_ASSOCIATIONS = "contacts,companies,deals"


class HubSpotSimpleSourceConfig(HubSpotBaseSourceConfig):
    page_size: int = Field(100, ge=1, le=100)


class HubSpotSimpleSource(HubSpotBaseSource):
    """Full-refresh v3 streams: property catalogs, deal pipelines, line items, and the notes, calls, tasks and
    meetings engagement objects with their contact, company and deal associations inlined."""

    def __init__(self, config: HubSpotSimpleSourceConfig):
        super().__init__(config)
        self.config: HubSpotSimpleSourceConfig = config
        self.stream_enum = HubSpotSimpleStreams(config.stream)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in HubSpotSimpleStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotSimpleSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _get_properties(self, obj: str) -> SourceIteration:
        results = self._call(method="GET", url=f"{URL_BASE}/crm/v3/properties/{obj}").json().get("results", [])
        records = [SourceRecord(id=f"{obj}:{p['name']}", data=p) for p in results]
        return SourceIteration(next_pagination={}, records=records)

    def _get_deal_pipelines(self) -> SourceIteration:
        results = self._call(method="GET", url=f"{URL_BASE}/crm/v3/pipelines/deals").json().get("results", [])
        records = [SourceRecord(id=str(p["id"]), data=p) for p in results]
        return SourceIteration(next_pagination={}, records=records)

    def _get_list(self, obj: str, pagination: dict, associations_param: Optional[str] = None) -> SourceIteration:
        # Without `properties` the list API returns only a handful of default properties.
        params: Dict[str, Any] = {
            "limit": self.config.page_size,
            "properties": ",".join(self.get_properties_catalog(obj)),
        }
        if associations_param:
            params["associations"] = associations_param
        after = pagination.get("after")
        if after:
            params["after"] = after

        data = self._call(method="GET", url=f"{URL_BASE}/crm/v3/objects/{obj}", params=params).json()
        results = data.get("results", [])
        if associations_param:
            for r in results:
                self._complete_associations(obj, r)
        records = [SourceRecord(id=str(r["id"]), data=r) for r in results]
        next_after = data.get("paging", {}).get("next", {}).get("after")
        return SourceIteration(next_pagination={"after": next_after} if next_after else {}, records=records)

    def _complete_associations(self, obj: str, record: dict) -> None:
        """Inline associations stop at 100 per type; page through the rest so the record carries all of them."""
        for to_object, association in (record.get("associations") or {}).items():
            next_page = association.pop("paging", {}).get("next")
            while next_page:
                page = self._call(
                    method="GET",
                    url=f"{URL_BASE}/crm/v3/objects/{obj}/{record['id']}/associations/{to_object}",
                    params={"after": unquote(next_page["after"]), "limit": 500},
                ).json()
                association["results"].extend(page.get("results", []))
                next_page = page.get("paging", {}).get("next")

    def get(self, pagination: dict = None) -> SourceIteration:
        pagination = pagination or {}
        stream = self.stream_enum

        if stream in _PROPERTIES_OBJECT:
            return self._get_properties(_PROPERTIES_OBJECT[stream])
        if stream == HubSpotSimpleStreams.DEAL_PIPELINES:
            return self._get_deal_pipelines()
        if stream in _ENGAGEMENT_OBJECT:
            return self._get_list(_ENGAGEMENT_OBJECT[stream], pagination, associations_param=_ENGAGEMENT_ASSOCIATIONS)
        if stream == HubSpotSimpleStreams.LINE_ITEMS:
            return self._get_list("line_items", pagination)
        raise NotImplementedError(f"Stream '{stream}' is not implemented for HubSpotSimpleSource")
