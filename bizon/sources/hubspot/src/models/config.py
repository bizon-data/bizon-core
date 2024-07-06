from pydantic import Field
from enum import StrEnum
from typing import Optional
from bizon.api.models.config import SourceConfig, AuthConfig
from bizon.api.auth.authenticators.models.oauth_config import OAuthConfig

class HubSpotAuthType(StrEnum):
    API_KEY = "API_KEY"
    OAUTH = "OAUTH"

class HubSpotAuthConfig(AuthConfig):
    type: HubSpotAuthType = Field(..., description="Type of authentication", const=True, example="API_KEY")
    oauth: Optional[OAuthConfig] = Field(None, description="OAuth configuration")
    api_key: Optional[str] = Field(None, description="API Key")

class HubSpotObjects(StrEnum):
    CONTACTS = 'contacts'
    COMPANIES = 'companies'
    DEALS = 'deals'

class HubSpotSourceConfig(SourceConfig):
    name = "HubSpot"
    stream_name: HubSpotObjects
    auth_config: HubSpotAuthConfig
