from abc import ABC, abstractmethod
from typing import Any, Mapping

from requests.auth import AuthBase

from bizon.api.session import Session

from .config import AuthConfig


class Auth(ABC):
    def __init__(self, config: AuthConfig):
        self.config = config
        self._type = config.type
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
        """Debugging method to get the auth headers"""
        if hasattr(self.authenticator, "get_auth_header"):
            return self.authenticator.get_auth_header()
        else:
            raise NotImplementedError(
                f"get_auth_header method not implemented in authenticator of type {self.type} and class {self.authenticator.__class__.__name__}"
            )

    def set_session_auth(self, session: Session):
        """Set the session auth to the authenticator"""
        session.auth = self.get_authenticator()
