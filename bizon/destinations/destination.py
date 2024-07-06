from abc import ABC, abstractmethod


class Destination(ABC):

    def __init__(self) -> None:
        pass

    @abstractmethod
    @property
    def name(self):
        pass

    @abstractmethod
    def check_connection(self) -> bool:
        pass

    @abstractmethod
    def write(self, data):
        pass

    @abstractmethod
    def get_state_for_execution(self, execution_id: int):
        pass