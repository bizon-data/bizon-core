from pydantic import BaseModel, Field

from .abstract_token import AbstractHeaderAuthenticator


class TokenAuthParams(BaseModel):
    token: str = Field(..., description="Token to be attached to the request")
    auth_method: str = Field("Bearer", description="Auth method for token auth")
    auth_header: str = Field("Authorization", description="Auth header for token auth")


class TokenAuthenticator(AbstractHeaderAuthenticator):
    """
    Builds auth header, based on the token provided.
    The token is attached to each request via the `auth_header` header.
    """

    @property
    def auth_header(self) -> str:
        return self._auth_header

    @property
    def token(self) -> str:
        return f"{self._auth_method} {self._token}"

    def __init__(
        self,
        params: TokenAuthParams,
    ):
        self._auth_header = params.auth_header
        self._auth_method = params.auth_method
        self._token = params.token
