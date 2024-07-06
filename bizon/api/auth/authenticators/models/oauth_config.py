from pydantic import BaseModel, Field
from typing import List, Mapping, Any
from pydantic_extra_types.pendulum_dt import DateTime

class OAuthConfig(BaseModel):
    token_refresh_endpoint: str = Field(..., description="URL to refresh the token")
    client_id: str = Field(..., description="Client ID")
    client_secret: str = Field(..., description="Client Secret")
    refresh_token: str = Field(..., description="Refresh Token")
    scopes: List[str] = Field(None, description="Scopes")
    token_expiry_date: DateTime = Field(None, description="Token expiry date")
    token_expiry_date_format: str = Field(None, description="Token expiry date format")
    access_token_name: str = Field("access_token", description="Name of the access token")
    expires_in_name: str = Field("expires_in", description="Name of the expires in")
    refresh_request_body: Mapping[str, Any] = Field(None, description="Request body to refresh the token")
    grant_type: str = Field("refresh_token", description="Grant type")