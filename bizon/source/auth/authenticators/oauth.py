from collections.abc import Mapping
from typing import Any, List, Optional, Union

import pendulum
from pydantic import BaseModel, Field
from pydantic_extra_types.pendulum_dt import DateTime

from .abstract_oauth import AbstractOauth2Authenticator


class Oauth2AuthParams(BaseModel):
    token_refresh_endpoint: str = Field(..., description="URL to refresh the token")
    client_id: str = Field(..., description="Client ID")
    client_secret: str = Field(..., description="Client Secret")
    refresh_token: Union[str, None] = Field(None, description="Refresh Token")
    scopes: List[str] = Field(None, description="Scopes")
    token_expiry_date: DateTime = Field(None, description="Token expiry date")
    token_expiry_date_format: str = Field(None, description="Token expiry date format")
    access_token_name: str = Field("access_token", description="Name of the access token")
    expires_in_name: str = Field("expires_in", description="Name of the expires in")
    refresh_request_body: Mapping[str, Any] = Field(None, description="Request body to refresh the token")
    grant_type: str = Field("refresh_token", description="Grant type")
    response_field_path: Optional[str] = Field(None, description="Path in dpath to the response field")
    expiry_skew_seconds: Optional[float] = Field(
        None,
        description="Refresh the token this many seconds before it expires. "
        "Defaults to 10% of the token's lifetime, capped at 60 seconds.",
    )


class Oauth2Authenticator(AbstractOauth2Authenticator):
    """
    Generates OAuth2.0 access tokens from an OAuth2.0 refresh token and client credentials.
    The generated access token is attached to each request via the Authorization header.
    """

    def __init__(self, params: Oauth2AuthParams):
        self._token_refresh_endpoint = params.token_refresh_endpoint
        self._client_secret = params.client_secret
        self._client_id = params.client_id
        self._refresh_token = params.refresh_token
        self._scopes = params.scopes
        self._access_token_name = params.access_token_name
        self._expires_in_name = params.expires_in_name
        self._refresh_request_body = params.refresh_request_body
        self._grant_type = params.grant_type
        self._response_field_path = params.response_field_path
        self._token_expiry_date = params.token_expiry_date or pendulum.now().subtract(days=1)
        self._token_expiry_date_format = params.token_expiry_date_format
        self._expiry_skew_seconds = params.expiry_skew_seconds
        self._token_lifetime_seconds: Optional[float] = None
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

    def get_response_field_path(self) -> str:
        return self._response_field_path

    def get_token_expiry_date(self) -> DateTime:
        return self._token_expiry_date

    def set_token_expiry_date(self, value: Union[str, int]):
        now = pendulum.now()
        if self._token_expiry_date_format:
            self._token_expiry_date = pendulum.from_format(value, self._token_expiry_date_format)
        else:
            self._token_expiry_date = now.add(seconds=value)
        self._token_lifetime_seconds = (self._token_expiry_date - now).total_seconds()

    def get_expiry_skew_seconds(self) -> float:
        if self._expiry_skew_seconds is not None:
            return self._expiry_skew_seconds
        # Bounded by the lifetime so a short-lived token is not refreshed before every request.
        if self._token_lifetime_seconds is not None:
            return max(0.0, min(60.0, 0.1 * self._token_lifetime_seconds))
        return 60.0

    @property
    def access_token(self) -> str:
        return self._access_token

    @access_token.setter
    def access_token(self, value: str):
        self._access_token = value
