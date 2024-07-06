from pydantic import Field
from enum import Enum
from typing import Optional
from bizon.api.models.config import SourceConfig, AuthConfig
from bizon.api.auth.authenticators.models.oauth_config import OAuthConfig

class HubSpotAuthType(str, Enum):
    API_KEY = "API_KEY"
    OAUTH = "OAUTH"

class HubSpotAuthConfig(AuthConfig):
    type: HubSpotAuthType = Field(..., description="Type of authentication", example="API_KEY")
    oauth: Optional[OAuthConfig] = Field(None, description="OAuth configuration")
    api_key: Optional[str] = Field(None, description="API Key")

class HubSpotObjects(str, Enum):
    CONTACTS = 'contacts'
    COMPANIES = 'companies'
    DEALS = 'deals'

class HubSpotSourceConfig(SourceConfig):
    name: str = "hubspot"
    stream_name: HubSpotObjects
    authentication: HubSpotAuthConfig
