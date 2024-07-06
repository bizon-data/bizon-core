from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple

from .auth.auth import Auth
from .cursor import Cursor
from .models.config import SourceConfig
from .session import Session


class APIClient(ABC):

    def __init__(self, config: SourceConfig):
        self.config = config
        self.session = Session()

        # Set auth to session
        auth = self.get_auth()
        self.session.auth = auth.authenticator

    @abstractmethod
    def get_auth(self) -> Auth:
        pass

    @abstractmethod
    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        pass

    @abstractmethod
    def get(self, cursor: Cursor = None) -> Optional[dict]:
        pass
