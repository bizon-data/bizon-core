from typing import Any, Dict, List, Optional

from loguru import logger
from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord

from .hubspot_base import (
    URL_BASE,
    URL_GRANTED_SCOPES,  # noqa: F401
    URL_TOKEN_REFRESH,  # noqa: F401
    HubSpotBaseSource,
    HubSpotBaseSourceConfig,
    PropertiesConfig,
    PropertiesStrategy,
    id_paged_iteration,
    last_modified_property,
    record_with_timestamp,
    to_ms,
)
from .models.hs_object import AllObjectProperties, HubSpotProperty

# Re-fetches records modified right on the previous window's edge; duplicates resolve on the record id.
SEARCH_OVERLAP_SECONDS = 300


class HubSpotSourceConfig(HubSpotBaseSourceConfig):
    properties: PropertiesConfig = PropertiesConfig(strategy=PropertiesStrategy.ALL, selected_properties=None)
    associations_types: List[str] = Field(
        [],
        description="Object types whose associated ids are inlined on each record. Full refresh only: the search "
        "API behind incremental syncs does not return associations.",
    )


class HubSpotObjectsSource(HubSpotBaseSource):
    api_version = "v3"

    object_path = f"crm/{api_version}/objects"
    properties_path = f"crm/{api_version}/properties"

    page_size = 100

    def __init__(self, config: HubSpotSourceConfig):
        super().__init__(config)
        self.config: HubSpotSourceConfig = config
        self.object = self.get_object_type()
        self.selected_properties = []

        if config.init_pipeline:
            self.selected_properties = self.get_selected_properties()

    @staticmethod
    def streams() -> List[str]:
        return ["contacts", "companies", "deals"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotSourceConfig

    def get_object_type(self) -> str:
        return self.config.stream

    @property
    def url_list(self) -> str:
        return f"{URL_BASE}/{self.object_path}/{self.object}"

    @property
    def url_list_properties(self) -> str:
        return f"{URL_BASE}/{self.properties_path}/{self.object}"

    @property
    def url_search(self) -> str:
        return f"{URL_BASE}/{self.object_path}/{self.object}/search"

    def _request_api(
        self,
        method: str,
        url: str,
        params: Optional[dict] = None,
        payload: Optional[dict] = None,
        headers=None,
    ) -> dict:
        kwargs: Dict[str, Any] = {"params": params, "headers": headers}
        if payload is not None:
            kwargs["json"] = payload
        return self._call(method=method, url=url, **kwargs).json()

    def get_selected_properties(self) -> List[str]:
        return self.resolve_properties(self.object, self.config.properties)

    def _get(self, after: str = None) -> Optional[dict]:
        params = {
            "limit": self.page_size,
            "properties": ",".join(self.selected_properties),
        }
        if self.config.associations_types:
            params["associations"] = ",".join(self.config.associations_types)
        if after:
            params["after"] = after

        return self._request_api(method="GET", url=self.url_list, params=params)

    def get(
        self,
        pagination: dict = None,
    ) -> SourceIteration:
        """Return the next page of the v3 list API."""
        if not pagination:
            response = self._get()
        else:
            response = self._get(after=pagination["after"])

        return self.parse_response(response)

    def parse_response(self, response: dict) -> SourceIteration:
        if not response or len(response.get("results", [])) == 0:
            return SourceIteration(next_pagination={}, records=[])

        records = [SourceRecord(id=str(record["id"]), data=record) for record in response["results"]]

        if response.get("paging", dict()).get("next", None) is None:
            return SourceIteration(next_pagination={}, records=records)

        next_pagination_dict = {
            "link": response["paging"]["next"]["link"],
            "after": response["paging"]["next"]["after"],
        }
        return SourceIteration(next_pagination=next_pagination_dict, records=records)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """Records whose last-modified property falls in `[last_run - overlap, run start)`, through the search API."""
        cursor_field = source_state.cursor_field
        if not cursor_field or cursor_field == "updatedAt":
            cursor_field = last_modified_property(self.object)
        window = (
            cursor_field,
            to_ms(source_state.last_run) - SEARCH_OVERLAP_SECONDS * 1000,
            self.run_start_ms(source_state),
        )
        results = self.search_page(
            object_type=self.object,
            properties=self.selected_properties,
            limit=self.page_size,
            last_id=(pagination or {}).get("last_id"),
            window=window,
        )
        return id_paged_iteration(results, [record_with_timestamp(r) for r in results])

    def get_total_records_count(self) -> Optional[int]:
        search_response = self._request_api(
            method="POST",
            url=self.url_search,
            payload={"filterGroups": [{"filters": [{"operator": "HAS_PROPERTY", "propertyName": "hs_object_id"}]}]},
        )
        total = search_response["total"]
        logger.info(f"Number of {self.object} in HubSpot: {f'{total:,}'.replace(',', ' ')}")

        # The cursor divides by the total to report progress, so an empty object reports it as unknown.
        if total == 0:
            return None

        return total

    def list_properties(self) -> AllObjectProperties:
        response = self._request_api(method="GET", url=self.url_list_properties)
        proprties = response["results"]
        return AllObjectProperties(
            properties=[
                HubSpotProperty(
                    name=hs_property["name"],
                    label=hs_property["label"],
                    field_type=hs_property["fieldType"],
                    type=hs_property["type"],
                    description=hs_property.get("description"),
                )
                for hs_property in proprties
            ]
        )
