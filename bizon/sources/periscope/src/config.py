from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from bizon.source.auth.authenticators.cookies import CookiesAuthParams
from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig


class PeriscopeStreams(str, Enum):
    DASHBOARDS = "dashboards"
    CHARTS = "charts"
    USERS = "users"
    DATABASES = "databases"
    VIEWS = "views"


class PeriscopeCookies(BaseModel):
    cf_bm: str = Field(..., description="Cloudflare bm cookie")
    periscope_session: str = Field(..., description="Periscope session cookie")


class PeriscopeCookiesAuthParams(CookiesAuthParams):
    cookies: PeriscopeCookies = Field(..., description="Cookies configuration")


class PeriscopeAuthConfig(AuthConfig):
    type: Literal[AuthType.COOKIES]
    params: PeriscopeCookiesAuthParams


class PeriscopeSourceConfig(SourceConfig):
    name: str = Field("periscope", description="Name of the source")
    stream_name: PeriscopeStreams = Field(..., description="Name of the stream")
    authentication: PeriscopeAuthConfig
    workspace_name: str = Field(..., description="Name of the workspace")
    client_site_id: int = Field(..., description="Client site ID")
    database_id: int = Field(..., description="Fetch charts connected to this Database ID")
