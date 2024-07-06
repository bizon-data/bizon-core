from pydantic import Field
from enum import StrEnum
from bizon.api.models.config import APIConfig, AuthConfig

class HSAuthType(StrEnum):
    API_KEY = "API_KEY"
    OAUTH = "OAUTH"

class HSAuthConfig(AuthConfig):
    type: HSAuthType = Field(..., description="Type of authentication", const=True, example="API_KEY")

class HSObjects(StrEnum):
    CONTACTS = 'contacts'
    COMPANIES = 'companies'
    DEALS = 'deals'

class HSConfig(APIConfig):
    object: HSObjects
