from abc import ABC, abstractmethod
from typing import Optional
from .auth import Auth
from .cursor import Cursor
from .models.config import APIConfig

class APIClient(ABC):

    def __init__(self, config: APIConfig, auth: Auth):
        self.config = config
        self.auth = auth

    @abstractmethod
    def check_connection(self) -> bool:
        pass

    @abstractmethod
    def get(self, cursor: Cursor = None) -> Optional[dict]:
        pass
