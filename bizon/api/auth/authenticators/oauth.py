from typing import Any, List, Mapping, Sequence, Tuple, Union
import dpath
import pendulum
from .abstract_oauth import AbstractOauth2Authenticator
from .models.oauth import OAuthConfig

class Oauth2Authenticator(AbstractOauth2Authenticator):
    """
    Generates OAuth2.0 access tokens from an OAuth2.0 refresh token and client credentials.
    The generated access token is attached to each request via the Authorization header.
    If a connector_config is provided any mutation of it's value in the scope of this class will emit AirbyteControlConnectorConfigMessage.
    """

    def __init__(self, oauth_config: OAuthConfig):
        self._token_refresh_endpoint = oauth_config.token_refresh_endpoint
        self._client_secret = oauth_config.client_secret
        self._client_id = oauth_config.client_id
        self._refresh_token = oauth_config.refresh_token
        self._scopes = oauth_config.scopes
        self._access_token_name = oauth_config.access_token_name
        self._expires_in_name = oauth_config.expires_in_name
        self._refresh_request_body = oauth_config.refresh_request_body
        self._grant_type = oauth_config.grant_type

        self._token_expiry_date = oauth_config.token_expiry_date or pendulum.now().subtract(days=1)
        self._token_expiry_date_format = oauth_config.token_expiry_date_format
        self._access_token = None

    def get_token_refresh_endpoint(self) -> str:
        return self._token_refresh_endpoint

    def get_client_id(self) -> str:
        return self._client_id

    def get_client_secret(self) -> str:
        return self._client_secret

    def get_refresh_token(self) -> str:
        return self._refresh_token

    def get_access_token_name(self) -> str:
        return self._access_token_name

    def get_scopes(self) -> List[str]:
        return self._scopes

    def get_expires_in_name(self) -> str:
        return self._expires_in_name

    def get_refresh_request_body(self) -> Mapping[str, Any]:
        return self._refresh_request_body

    def get_grant_type(self) -> str:
        return self._grant_type

    def get_token_expiry_date(self) -> pendulum.DateTime:
        return self._token_expiry_date

    def set_token_expiry_date(self, value: Union[str, int]):
        if self._token_expiry_date_format:
            self._token_expiry_date = pendulum.from_format(value, self._token_expiry_date_format)
        else:
            self._token_expiry_date = pendulum.now().add(seconds=value)

    @property
    def access_token(self) -> str:
        return self._access_token

    @access_token.setter
    def access_token(self, value: str):
        self._access_token = value
