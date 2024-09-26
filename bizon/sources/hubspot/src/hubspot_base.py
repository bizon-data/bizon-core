from abc import ABC
from http import HTTPStatus
from typing import Any, List, Optional, Tuple

from requests import HTTPError
from requests.adapters import HTTPAdapter, Retry
from requests.auth import AuthBase

from bizon.source.auth.authenticators.oauth import Oauth2AuthParams
from bizon.source.auth.authenticators.token import TokenAuthParams
from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.session import Session
from bizon.source.source import AbstractSource

URL_BASE = "https://api.hubapi.com"
URL_GRANTED_SCOPES = f"{URL_BASE}/oauth/v1/access-tokens"
URL_TOKEN_REFRESH = f"{URL_BASE}/oauth/v1/token"


class HubSpotBaseSource(AbstractSource, ABC):

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

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value == AuthType.OAUTH.value:
            return AuthBuilder.oauth2(
                params=Oauth2AuthParams(
                    token_refresh_endpoint=URL_TOKEN_REFRESH,
                    client_id=self.config.authentication.params.client_id,
                    client_secret=self.config.authentication.params.client_secret,
                    refresh_token=self.config.authentication.params.refresh_token,
                )
            )

        elif self.config.authentication.type.value == AuthType.API_KEY.value:
            return AuthBuilder.token(
                params=TokenAuthParams(
                    token=self.config.authentication.params.token,
                )
            )

        raise NotImplementedError(f"Auth type {self.config.authentication.type} not implemented for HubSpot")

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
        except Exception as e:
            alive = False
            error_msg = repr(e)
        return alive, error_msg

    def get_granted_scopes(self) -> List[str]:
        try:
            if self.config.authentication.type == AuthType.OAUTH.value:
                response = self.session.get(url=f"{URL_GRANTED_SCOPES}/{self.session.auth.access_token}")
            else:
                raise NotImplementedError("Scope endpoint for API Key are not supported.")
            response.raise_for_status()
            response_json = response.json()
            granted_scopes = response_json["scopes"]
            return granted_scopes
        except Exception as e:
            return False, repr(e)
