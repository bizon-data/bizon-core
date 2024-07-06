import json
from http import HTTPStatus
from typing import Any, Generator, List, Optional, Tuple

from loguru import logger
from requests import HTTPError
from requests.adapters import HTTPAdapter, Retry

from bizon.api.auth.auth import Auth
from bizon.api.client import APIClient
from bizon.api.models.state import FetchStatus
from bizon.api.session import Session

from .auth import HubSpotAuth
from .constants import URL_BASE, URL_GRANTED_SCOPES
from .errors import HubspotInvalidAuth
from .models.config import HubSpotAuthType, HubSpotObjects, HubSpotSourceConfig
from .models.hs_object import AllObjectProperties, HubSpotProperty
from .models.state import HubSpotPaging, HubSpotSyncState


class HubSpotClient(APIClient):

    api_version = "v3"

    object_path = f"crm/{api_version}/objects"
    properties_path = f"crm/{api_version}/properties"

    def __init__(self, config: HubSpotSourceConfig, object: HubSpotObjects):
        super().__init__(config)
        self.object = object
        self.selected_properties = []  # Initialize properties to empty list

        # If we are initializing the pipeline, we retrieve the selected properties from HubSpot
        if config.init_pipeline:
            self.selected_properties = self.get_selected_properties()

    def __del__(self):
        self.session.close()

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

    def get_session(self) -> Session:
        """Apply custom strategy for HubSpot"""
        session = Session()

        # Retry policy if rate-limited by HubSpot
        retries = Retry(
            total=50,
            backoff_factor=1,
            raise_on_status=True,
            status_forcelist=[429, 500, 502, 503, 504],
            status=30,
            allowed_methods=["GET", "POST"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=64))
        return session

    def get_auth(self) -> Auth:
        return HubSpotAuth(config=self.config.authentication)

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
        state: HubSpotSyncState = None,
    ) -> Tuple[Optional[List[dict]], HubSpotSyncState]:

        if state is None:
            response = self._get()
        else:
            response = self._get(after=state.paging.after)

        next_state = self.get_next_state(response)

        return response["results"], next_state

    def get_next_state(self, response: dict) -> HubSpotSyncState:
        paging = None
        is_finished = False

        # If no response or no results, we return None
        if not response or len(response.get("results", [])) == 0:
            return None, HubSpotSyncState(
                stream_name=self.object, records_fetched=0, fetch_status=FetchStatus.FAILED, is_finished=is_finished
            )

        # If no next page, we set is_finished to True
        if response.get("paging", {}).get("next", None) is None:
            is_finished = True
        else:
            # If there is a next page, we set the paging object
            paging = HubSpotPaging(
                link=response["paging"]["next"]["link"],
                after=response["paging"]["next"]["after"],
            )

        return HubSpotSyncState(
            stream_name=self.object,
            records_fetched=len(response["results"]),
            fetch_status=FetchStatus.SUCCESS,
            paging=paging,
            is_finished=is_finished,
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

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        """Check connection"""
        alive = True
        error_msg = None
        try:
            objects, state = self.get()
        except HTTPError as error:
            alive = False
            error_msg = repr(error)
            if error.response.status_code == HTTPStatus.BAD_REQUEST:
                response_json = error.response.json()
                error_msg = (
                    f"400 Bad Request: {response_json['message']}, please check if provided credentials are valid."
                )
        except HubspotInvalidAuth as e:
            alive = False
            error_msg = repr(e)
        return alive, error_msg

    def get_granted_scopes(self) -> List[str]:
        try:
            if self.auth.type == HubSpotAuthType.OAUTH:
                response = self.session.get(url=f"{URL_GRANTED_SCOPES}/{self.auth.authenticator.access_token}")
            else:
                raise NotImplementedError("Scope endpoint for API Key are not supported.")
            response.raise_for_status()
            response_json = response.json()
            granted_scopes = response_json["scopes"]
            return granted_scopes
        except Exception as e:
            return False, repr(e)

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
