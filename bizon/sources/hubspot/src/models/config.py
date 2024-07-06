from pydantic import Field
from enum import Enum
from typing import Optional
from bizon.api.models.config import SourceConfig, AuthConfig
from bizon.api.auth.authenticators.models.oauth_config import OAuthConfig
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
    CONTACTS = 'contacts'
    COMPANIES = 'companies'
    DEALS = 'deals'

class HubSpotSourceConfig(SourceConfig):
    name: str = "hubspot"
    stream_name: HubSpotObjects
    authentication: HubSpotAuthConfig
