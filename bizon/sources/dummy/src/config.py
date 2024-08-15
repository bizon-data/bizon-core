from enum import Enum
from typing import List, Literal, Optional

from pydantic import Field

from bizon.api.auth.config import AuthConfig, AuthType, OAuthConfig
from bizon.common.config import SourceConfig


class DummyStreams(str, Enum):
    CREATURES = "creatures"
    PLANTS = "plants"


class DummyAuthConfig(AuthConfig):
    type: Literal[AuthType.API_KEY, AuthType.OAUTH]
    api_key: Optional[str] = Field(None, description="API Key that you can get at https://mydummyapi.com/api-key")
    oauth: Optional[OAuthConfig] = Field(None, description="OAuth configuration")


class DummySourceConfig(SourceConfig):
    source_name: str = Field("dummy", description="Name of the source")
    stream_name: DummyStreams
    authentication: DummyAuthConfig
