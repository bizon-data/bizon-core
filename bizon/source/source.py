from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple, Union

from requests.auth import AuthBase

from bizon.source.models import SourceIteration

from .config import SourceConfig
from .session import Session


class Source(ABC):

    def __init__(self, config: SourceConfig):
        self.config = config
        self.session = self.get_session()

        # Set authentication in the session
        auth = self.get_authenticator()

        if auth:
            self.session.auth = auth

    def __del__(self):
        self.session.close()

    @abstractmethod
    def get_authenticator(self) -> Union[AuthBase, None]:
        """Return an authenticator for the source, it will be set in the session
        If no authenticator is needed, return None
        """
        pass

    @abstractmethod
    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        pass

    @abstractmethod
    def get(self, pagination: dict = None) -> SourceIteration:
        """Perform next API call to retrieve data and return next pagination.

        If pagination is None, it should return the first page of data.
        Otherwise, it should return the next page of data.

        - pagination dict
        - records List[dict]
        """
        pass

    @abstractmethod
    def get_total_records_count(self) -> Optional[int]:
        """Return the total count of records available in the source"""
        pass

    def get_session(self) -> Session:
        """Return a new session"""
        return Session()
