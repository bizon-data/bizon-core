from .abstract_token import AbstractHeaderAuthenticator


class TokenAuthenticator(AbstractHeaderAuthenticator):
    """
    Builds auth header, based on the token provided.
    The token is attached to each request via the `auth_header` header.
    """

    @property
    def auth_header(self) -> str:
        return self._auth_header

    @property
    def token(self) -> str:
        return f"{self._auth_method} {self._token}"

    def __init__(self, token: str, auth_method: str = "Bearer", auth_header: str = "Authorization"):
        self._auth_header = auth_header
        self._auth_method = auth_method
        self._token = token
