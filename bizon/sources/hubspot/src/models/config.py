from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

from bizon.api.auth.config import AuthConfig, OAuthConfig
from bizon.common.config import SourceConfig
from bizon.sources.hubspot.src.constants import URL_TOKEN_REFRESH


class HubSpotAuthType(str, Enum):
    API_KEY = "api_key"
    OAUTH = "oauth"


class HubSpotOAuthConfig(OAuthConfig):
    token_refresh_endpoint: str = Field(URL_TOKEN_REFRESH, description="HubSpot Token refresh endpoint")


class HubSpotAuthConfig(AuthConfig):
    type: HubSpotAuthType = Field(..., description="Type of authentication", example="API_KEY")
    oauth: Optional[HubSpotOAuthConfig] = Field(None, description="HubSpot OAuth configuration")
    api_key: Optional[str] = Field(None, description="API Key")


class HubSpotObjects(str, Enum):
    CONTACTS = "contacts"
    COMPANIES = "companies"
    DEALS = "deals"


class PropertiesStrategy(str, Enum):
    ALL = "all"
    SELECTED = "selected"


class PropertiesConfig(BaseModel):
    strategy: PropertiesStrategy
    selected_properties: Optional[List[str]] = Field([], comment="List of selected properties")


class HubSpotSourceConfig(SourceConfig):
    name: str = "hubspot"
    stream_name: HubSpotObjects
    properties: PropertiesConfig = PropertiesConfig(strategy=PropertiesStrategy.ALL, selected_properties=None)
    authentication: HubSpotAuthConfig
