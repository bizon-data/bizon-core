import json
from http import HTTPStatus
from typing import Any, Generator, List, Optional, Tuple

from requests import HTTPError
from requests.adapters import HTTPAdapter, Retry

from bizon.api.auth.auth import Auth
from bizon.api.client import APIClient
from bizon.api.session import Session

from .auth import HubSpotAuth
from .constants import URL_BASE, URL_GRANTED_SCOPES
from .errors import HubspotInvalidAuth
from .models.config import HubSpotAuthType, HubSpotObjects, HubSpotSourceConfig
from .models.hs_object import AllObjectProperties, HubSpotProperty


class HubSpotClient(APIClient):

    api_version = "v3"

    object_path = f"crm/{api_version}/objects"
    properties_path = f"crm/{api_version}/properties"

    def __init__(self, config: HubSpotSourceConfig, object: HubSpotObjects):
        super().__init__(config)
        self.object = object

    def __del__(self):
        self.session.close()

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
