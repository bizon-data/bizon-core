import json
from enum import Enum
from typing import Any, Generator, List, Optional

from loguru import logger
from pydantic import BaseModel, Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord

from .hubspot_base import HubSpotBaseSource
from .models.hs_object import AllObjectProperties, HubSpotProperty

URL_BASE = "https://api.hubapi.com"
URL_GRANTED_SCOPES = f"{URL_BASE}/oauth/v1/access-tokens"
URL_TOKEN_REFRESH = f"{URL_BASE}/oauth/v1/token"


class PropertiesStrategy(str, Enum):
    ALL = "all"
    SELECTED = "selected"


class PropertiesConfig(BaseModel):
    strategy: PropertiesStrategy = Field(PropertiesStrategy.ALL, description="Properties strategy")
    selected_properties: Optional[List[str]] = Field([], description="List of selected properties")


class HubSpotSourceConfig(SourceConfig):
    properties: PropertiesConfig = PropertiesConfig(strategy=PropertiesStrategy.ALL, selected_properties=None)


class HubSpotObjectsSource(HubSpotBaseSource):

    api_version = "v3"

    object_path = f"crm/{api_version}/objects"
    properties_path = f"crm/{api_version}/properties"

    def __init__(self, config: HubSpotSourceConfig):
        super().__init__(config)
        self.config: HubSpotSourceConfig = config
        self.object = self.config.stream_name
        self.selected_properties = []  # Initialize properties to empty list

        # If we are initializing the pipeline, we retrieve the selected properties from HubSpot
        if config.init_pipeline:
            self.selected_properties = self.get_selected_properties()

    @staticmethod
    def streams() -> List[str]:
        return ["contacts", "companies", "deals"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotSourceConfig

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
    ) -> Generator[dict, None, None]:

        # Call HubSpot API
        response = self.session.call(
            method=method,
            url=url,
            params=params,
            data=json.dumps(payload),
            headers=headers,
        )
        return response.json()

    def get_selected_properties(self) -> List[str]:
        all_properties = self.list_properties()
        assert (
            len(all_properties.properties) > 0
        ), "No properties found in HubSpot. Which is likely an error with the API."
        properties = []

        if self.config.properties.strategy == "all":
            properties = all_properties.property_names()

        if self.config.properties.strategy == "selected":
            # We check that all properties slected are present in the list of all properties
            for prop in self.config.properties.selected_properties:
                assert prop in all_properties.property_names(), f"Property {prop} is not present in HubSpot."
            properties = self.config.properties.selected_properties

        assert len(properties) > 0, "No properties selected to sync"
        logger.info(f"{len(properties)} selected properties for sync.")
        return properties

    def _get(self, after: str = None) -> Optional[dict]:
        params = {
            "limit": 100,
            "properties": ",".join(self.selected_properties),
        }
        if after:
            params["after"] = after

        return self._request_api(method="GET", url=self.url_list, params=params)

    def get(
        self,
        pagination: dict = None,
    ) -> SourceIteration:
        """Return the next page of data from HubSpot
        Returns:
            dict, Optional[List[dict]]]: Next pagination dict and data
        """

        if not pagination:
            response = self._get()
        else:
            response = self._get(after=pagination["after"])

        return self.parse_response(response)

    def parse_response(self, response: dict) -> SourceIteration:
        # If no response or no results, we return empty dict
        if not response or len(response.get("results", [])) == 0:
            return dict(), []

        # If no next page, we set is_finished to True
        if response.get("paging", dict()).get("next", None) is None:
            return dict(), response["results"]

        # If there is a next page, we set the paging object
        next_pagination_dict = {
            "link": response["paging"]["next"]["link"],
            "after": response["paging"]["next"]["after"],
        }
        return SourceIteration(
            next_pagination=next_pagination_dict,
            records=[
                SourceRecord(
                    id=record["id"],
                    data=record,
                )
                for record in response["results"]
            ],
        )

    def get_total_records_count(self) -> Optional[int]:
        search_response = self._request_api(
            method="POST",
            url=self.url_search,
            payload={"filterGroups": [{"filters": [{"operator": "HAS_PROPERTY", "propertyName": "hs_object_id"}]}]},
        )
        total = search_response["total"]
        logger.info(f"Number of {self.object} in HubSpot: {'{:,}'.format(total).replace(',', ' ')}")
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
