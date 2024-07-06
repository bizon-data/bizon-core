from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple

from .auth.auth import Auth
from .cursor import Cursor
from .models.config import SourceConfig
from .session import Session


class APIClient(ABC):

    def __init__(self, config: SourceConfig):
        self.config = config
        self.session = self.get_session()

        # Set authentication in the session
        self.auth = self.get_auth()
        self.session.auth = self.auth.authenticator

    @abstractmethod
    def get_auth(self) -> Auth:
        pass

    @abstractmethod
    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        pass

    @abstractmethod
    def get(self, cursor: Cursor = None) -> Optional[dict]:
        pass

    @abstractmethod
    def get_total_records_count(self) -> Optional[int]:
        """Return the total count of records available in the source"""
        pass

    def get_session(self) -> Session:
        """Return a new session"""
        return Session()
