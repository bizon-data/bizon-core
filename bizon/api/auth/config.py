from enum import Enum
from typing import Union

from pydantic import BaseModel, Field

from .authenticators.basic import BasicHttpAuthParams
from .authenticators.cookies import CookiesAuthParams
from .authenticators.oauth import Oauth2AuthParams
from .authenticators.token import TokenAuthParams


class AuthType(str, Enum):
    OAUTH = "oauth"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    COOKIES = "cookies"


class AuthConfig(BaseModel):
    type: AuthType = Field(..., description="Type of authentication", example="oauth")
    params: Union[
        Oauth2AuthParams,
        BasicHttpAuthParams,
        TokenAuthParams,
        CookiesAuthParams,
    ] = Field(..., description="Configuration for the authentication")
