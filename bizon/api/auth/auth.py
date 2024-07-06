from abc import ABC, abstractmethod
from ..models.config import AuthConfig
from ..session import Session

class Auth(ABC):
    def __init__(self, config: AuthConfig):
        self.config = config

    @abstractmethod
    def authenticate(self, session: Session) -> bool:
        """Authenticate the client with necessary flow, return True if successful"""
        pass

    @abstractmethod
    def update_authenticated_session(self, session: Session) -> bool:
        """Check if the session is authenticated and update it if necessary"""
        pass
