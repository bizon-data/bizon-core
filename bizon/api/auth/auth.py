from abc import ABC, abstractmethod
from bizon.api.models.config import AuthConfig
from bizon.api.session import Session
from requests.auth import AuthBase
from typing import Any, Mapping

class Auth(ABC):
    def __init__(self, config: AuthConfig, type: str):
        self.config = config
        self._type = type
        self._authenticator = self.get_authenticator()

    @property
    def type(self) -> str:
        return self._type

    @property
    def authenticator(self) -> AuthBase:
        return self._authenticator

    @abstractmethod
    def get_authenticator(self) -> AuthBase:
        pass

    def get_auth_headers(self) -> Mapping[str, Any]:
        """ Debugging method to get the auth headers """
        if hasattr(self.authenticator, "get_auth_header"):
            return self.authenticator.get_auth_header()
        else:
            raise NotImplementedError("get_auth_header method not implemented in authenticator")

    def set_session_auth(self, session: Session):
        """ Set the session auth to the authenticator """
        session.auth = self.get_authenticator()