from requests.auth import AuthBase

from bizon.api.auth.auth import Auth
from bizon.api.auth.authenticators.oauth import Oauth2Authenticator
from bizon.api.auth.authenticators.token import TokenAuthenticator
from bizon.api.auth.config import AuthType

URL_TOKEN_REFRESH = "https://api.mydummysaas.com/oauth/v1/token"


class DummyAuth(Auth):
    """General class to Authenticate on HubSpot"""

    def get_authenticator(self) -> AuthBase:
        # Check the type of authentication and return the correct auth class
        if self.config.type.value == AuthType.OAUTH:
            return Oauth2Authenticator(
                token_refresh_endpoint=URL_TOKEN_REFRESH,
                client_id=self.config.oauth.client_id,
                client_secret=self.config.oauth.client_secret,
                refresh_token=self.config.oauth.refresh_token,
            )

        elif self.config.type.value == AuthType.API_KEY:
            return TokenAuthenticator(token=self.config.api_key)

        else:
            raise NotImplementedError(f"Auth type {self.config.type} not implemented for Dummy")
