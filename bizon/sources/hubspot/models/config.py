from pydantic import Field
from enum import StrEnum
from bizon.api.models.config import APIConfig, AuthConfig

class HubSpotAuthType(StrEnum):
    API_KEY = "API_KEY"
    OAUTH = "OAUTH"

class HubSpotAuthConfig(AuthConfig):
    type: HubSpotAuthType = Field(..., description="Type of authentication", const=True, example="API_KEY")

class HubSpotObjects(StrEnum):
    CONTACTS = 'contacts'
    COMPANIES = 'companies'
    DEALS = 'deals'

class HubSpotConfig(APIConfig):
    object: HubSpotObjects
