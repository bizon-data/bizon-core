from requests.auth import AuthBase
from bizon.api.auth.auth import Auth
from .models.config import HubSpotAuthType
from bizon.api.auth.authenticators.oauth import Oauth2Authenticator
from bizon.api.auth.authenticators.token import TokenAuthenticator
from .constants import URL_TOKEN_REFRESH

class HubSpotAuth(Auth):
    """ General class to Authenticate on HubSpot """

    def get_authenticator(self) -> AuthBase:
        # Check the type of authentication and return the correct auth class
        if self.config.type.value == HubSpotAuthType.OAUTH:
            return Oauth2Authenticator(
                token_refresh_endpoint=URL_TOKEN_REFRESH,
                client_id=self.config.oauth.client_id,
                client_secret=self.config.oauth.client_secret,
                refresh_token=self.config.oauth.refresh_token,
            )

        elif self.config.type.value == HubSpotAuthType.API_KEY:
            return TokenAuthenticator(token=self.config.api_key)

        else:
            raise NotImplementedError(f"Auth type {self.config.type} not implemented")