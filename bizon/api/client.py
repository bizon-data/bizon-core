from abc import ABC, abstractmethod
from typing import Optional, Tuple, Any
from .auth.auth import Auth
from .cursor import Cursor
from .models.config import APIConfig

class APIClient(ABC):

    def __init__(self, config: APIConfig, auth: Auth):
        self.config = config
        self.auth = auth

    @abstractmethod
    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        pass

    @abstractmethod
    def get(self, cursor: Cursor = None) -> Optional[dict]:
        pass
