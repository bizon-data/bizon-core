from requests.auth import AuthBase
from bizon.api.auth.auth import Auth
from .models.config import HubSpotAuthConfig, HubSpotAuthType
from bizon.api.auth.authenticators.oauth import Oauth2Authenticator
from bizon.api.auth.authenticators.token import TokenAuthenticator
from .constants import URL_TOKEN_REFRESH

class HubSpotAuth(Auth):
    """ General class to Authenticate on HubSpot """
    def __init__(self, config: HubSpotAuthConfig, type: str):
        super().__init__(config, type)
        self.hs_config: HubSpotAuthConfig = config

    def get_authenticator(self) -> AuthBase:
        # Check the type of authentication and return the correct auth class
        if self.config.type == HubSpotAuthType.OAUTH:
            return Oauth2Authenticator(
                token_refresh_endpoint=URL_TOKEN_REFRESH,
                client_id=self.hs_config.oauth.client_id,
                client_secret=self.hs_config.oauth.client_secret,
                refresh_token=self.hs_config.oauth.refresh_token,
            )

        elif self.config.type == HubSpotAuthType.API_KEY:
            return TokenAuthenticator(token=self.hs_config.api_key)

        else:
            raise NotImplementedError(f"Auth type {self.config.type} not implemented")