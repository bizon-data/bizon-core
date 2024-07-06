from abc import ABC, abstractmethod
from .models.config import AuthConfig
from requests import Request

class Auth(ABC):
    def __init__(self, config: AuthConfig):
        self.config = config

    @abstractmethod
    def authenticate(self) -> None:
        """Authenticate the client with necessary flow"""
        pass

    @abstractmethod
    def get_authenticated_request(self) -> Request:
        """Return a request object with authentication headers and params"""
        pass