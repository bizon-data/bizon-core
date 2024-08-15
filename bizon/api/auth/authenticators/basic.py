import base64

from pydantic import BaseModel, Field

from .abstract_token import AbstractHeaderAuthenticator


class BasicHttpAuthParams(BaseModel):
    username: str = Field(..., description="Username for basic auth")
    password: str = Field("", description="Password for basic auth")
    auth_method: str = Field("Basic", description="Auth method for basic auth")
    auth_header: str = Field("Authorization", description="Auth header for basic auth")


class BasicHttpAuthenticator(AbstractHeaderAuthenticator):
    """
    Builds auth based off the basic authentication scheme as defined by RFC 7617, which transmits credentials as USER ID/password pairs, encoded using bas64
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme
    """

    @property
    def auth_header(self) -> str:
        return self._auth_header

    @property
    def token(self) -> str:
        return f"{self._auth_method} {self._token}"

    def __init__(self, params: BasicHttpAuthParams):
        auth_string = f"{params.username}:{params.password}".encode("utf8")
        b64_encoded = base64.b64encode(auth_string).decode("utf8")
        self._auth_header = params.auth_header
        self._auth_method = params.auth_method
        self._token = b64_encoded
