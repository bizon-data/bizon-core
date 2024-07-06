from requests import Request
from bizon.api.auth.auth import Auth
from bizon.api.session import Session
from .models.config import HubSpotAuthConfig, HubSpotAuthType
from bizon.api.auth.authenticators.oauth import Oauth2Authenticator
from bizon.api.auth.authenticators.models.oauth import OAuthConfig
from requests import Session
from .constants import URL_TOKEN_REFRESH

class Oauth2Auth(Auth):

    def __init__(self, config: HubSpotAuthConfig):
        super().__init__(config)
        self.oauth_config = OAuthConfig(
            token_refresh_endpoint=URL_TOKEN_REFRESH,
            client_id=config.oauth.client_id,
            client_secret=config.oauth.client_secret,
            refresh_token=config.oauth.refresh_token,
        )

    def authenticate(self, session: Session) -> bool:
        """ Authenticate on HubSpot with OAuth2 """
        return super().authenticate(session)

    def update_session_with_auth(self, session: Session) -> bool:
        return super().update_session_with_auth(session)

class APIKeyAuth(Auth):
    """ Authenticate on HubSpot with API Key"""
    def authenticate(self, session: Session) -> bool:
        return super().authenticate(session)

    def update_session_with_auth(self, session: Session) -> bool:
        return super().update_session_with_auth(session)


class HubSpotAuth(Auth):
    """ General class to Authenticate on HubSpot """
    def __init__(self, config: HubSpotAuthConfig):
        super().__init__(config)

        # Check the type of authentication and return the correct auth class
        if self.config.type == HubSpotAuthType.OAUTH:
            self.auth = Oauth2Auth(config)

        elif self.config.type == HubSpotAuthType.API_KEY:
            self.auth = APIKeyAuth(config)

    def authenticate(self, session: Session) -> bool:
        self.auth.authenticate(session)

    def update_authenticated_session(self, session: Session) -> None:
        self.auth.update_session_with_auth(session)
