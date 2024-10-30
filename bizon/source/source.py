from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional, Tuple, Type, Union

from requests.auth import AuthBase

from .config import SourceConfig
from .models import SourceIncrementalState, SourceIteration
from .session import Session


class AbstractSource(ABC):

    def __init__(self, config: SourceConfig):
        self.config = config
        self.session = self.get_session()

        # Set authentication in the session
        auth = self.get_authenticator()

        if auth:
            self.session.auth = auth

    def __del__(self):
        self.session.close()

    @staticmethod
    @abstractmethod
    def streams() -> List[str]:
        """Return all the streams that the source supports"""
        pass

    @staticmethod
    @abstractmethod
    def get_config_class() -> Type[SourceConfig]:
        """Return the config class for the source"""
        pass

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

        If no pagination is returned in SourceIteration, it means that there is no more data to fetch.

        - pagination dict
        - records List[dict]
        """
        pass

    @abstractmethod
    def get_total_records_count(self) -> Optional[int]:
        """Return the total count of records available in the source"""
        pass

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """Perform next API call to retrieve data incrementally and return next pagination.

        If pagination is None, it should return the first page of the incremental data.

        Otherwise, it should return the next page of data.

        If no pagination is returned in SourceIteration, it means that:
          - there is no more data to fetch
          -  you need to provide the next state that will be stored.
        """

        # In case we need a special formatting for the date or a non-date state format
        # We can use the `state` dict in SourceIncrementalState

        # In case of HubSpot we only use last_run timestamp from state
        # -> Iterate on all records updated between source_state.last_run and now
        # -> Return the pagination of the search API
        # -> perform new search if needed (more than 10K results from CRM Search API)

        # In case of Kafka
        # -> Iterate on all records after last offset stored in state
        # -> Return the next offset to fetch in pagination to continue syncing data
        # -> When no records are in the topic for a given time (timeout), we consider there is no data to fetch
        # -> return empty pagination and return last offset pulled to put in the state
        pass

    def get_session(self) -> Session:
        """Return a new session"""
        return Session()
