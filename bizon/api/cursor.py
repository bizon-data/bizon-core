from abc import ABC, abstractmethod

from .models.state import AbstractState


class Cursor(ABC):
    def __init__(self, data: dict):
        self.data = data
        self._state: AbstractState = None

    @property
    def state(self) -> AbstractState:
        if self._state is None:
            raise ValueError("Cursor is not initialized")
        return self._state

    @abstractmethod
    def update_state(self, data: dict) -> None:
        pass
