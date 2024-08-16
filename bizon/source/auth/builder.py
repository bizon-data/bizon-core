from .authenticators.basic import BasicHttpAuthenticator, BasicHttpAuthParams
from .authenticators.cookies import CookiesAuthenticator, CookiesAuthParams
from .authenticators.oauth import Oauth2Authenticator, Oauth2AuthParams
from .authenticators.token import TokenAuthenticator, TokenAuthParams


class AuthBuilder:
    @staticmethod
    def oauth2(params: Oauth2AuthParams) -> Oauth2Authenticator:
        return Oauth2Authenticator(params=params)

    @staticmethod
    def basic(params: BasicHttpAuthParams) -> BasicHttpAuthenticator:
        return BasicHttpAuthenticator(params=params)

    @staticmethod
    def token(params: TokenAuthParams) -> TokenAuthenticator:
        return TokenAuthenticator(params=params)

    @staticmethod
    def cookies(params: CookiesAuthParams) -> CookiesAuthenticator:
        return CookiesAuthenticator(params=params)
