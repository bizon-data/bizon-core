from enum import Enum
from typing import Literal, Union

from pydantic import Field

from bizon.source.auth.authenticators.oauth import Oauth2AuthParams
from bizon.source.auth.authenticators.token import TokenAuthParams
from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig


class DummyStreams(str, Enum):
    CREATURES = "creatures"
    PLANTS = "plants"


class DummyAuthConfig(AuthConfig):
    type: Literal[AuthType.API_KEY, AuthType.OAUTH]
    params: Union[TokenAuthParams, Oauth2AuthParams] = Field(None, description="OAuth or API configuration")


class DummySourceConfig(SourceConfig):
    source_name: str = Field("dummy", description="Name of the source")
    stream_name: DummyStreams
    authentication: DummyAuthConfig
