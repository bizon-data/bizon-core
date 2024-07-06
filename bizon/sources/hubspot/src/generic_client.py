from http import HTTPStatus
from requests import HTTPError
from bizon.api.auth.auth import Auth
from bizon.api.client import APIClient
from .errors import HubspotInvalidAuth
from .session import HubspotSession
from .models.config import HubSpotSourceConfig, HubSpotObjects, HubSpotAuthType
from .models.hs_object import HubSpotProperty, AllObjectProperties
from .constants import URL_BASE, URL_GRANTED_SCOPES
from .auth import HubSpotAuth
from typing import Generator, List, Optional, Any, Tuple
import json


class HubSpotClient(APIClient):

    api_version = "v3"

    object_path = f"crm/{api_version}/objects"
    properties_path = f"crm/{api_version}/properties"

    def __init__(self, config: HubSpotSourceConfig, object: HubSpotObjects):
        super().__init__(config)
        self.object = object
        self.session = HubspotSession()

       # Set auth to session
        self.auth = self.get_auth()
        self.session.auth = self.auth.authenticator

    def __del__(self):
        self.session.close()

    @property
    def url_list(self) -> str:
        return f"{URL_BASE}/{self.object_path}/{self.object}"

    @property
    def url_list_properties(self) -> str:
        return f"{URL_BASE}/{self.properties_path}/{self.object}"

    def get_auth(self) -> Auth:
        return HubSpotAuth(config=self.config.authentication)

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

    def get(
        self,
    ) -> List[dict]:
        params = {
            "limit": 100,
        }
        return self._request_api(method="GET", url=self.url_list, params=params)

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        """Check connection"""
        alive = True
        error_msg = None
        try:
            objects = self.get()
        except HTTPError as error:
            alive = False
            error_msg = repr(error)
            if error.response.status_code == HTTPStatus.BAD_REQUEST:
                response_json = error.response.json()
                error_msg = f"400 Bad Request: {response_json['message']}, please check if provided credentials are valid."
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
