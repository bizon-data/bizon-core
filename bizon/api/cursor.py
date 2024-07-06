from abc import ABC

from loguru import logger

from .models.state import AbstractState


class Cursor:
    def __init__(self, state: AbstractState = None, total_records: int = None):
        self._state = state
        self.iteration = 0
        self.rows_fetched = 0
        self.is_finished = False
        self.total_records = total_records

    @property
    def state(self) -> AbstractState:
        if self._state is None:
            raise ValueError("Cursor is not initialized")
        return self._state

    def update_state(self, new_state: AbstractState):
        if new_state.fetch_status:
            self.iteration += 1
            self.rows_fetched += new_state.records_fetched
            self._state = new_state

            percentage_str = ""
            if self.total_records is not None:
                percentage_str = f"({self.rows_fetched / self.total_records:.3%})"

            logger.info(
                f"Source: {new_state.stream_name} - Iteration {self.iteration} - Fetched: {self.rows_fetched} {percentage_str}, Successfully."
            )
            if new_state.is_finished:
                self.is_finished = True
                logger.info(
                    f"Source: {new_state.stream_name} - Iteration {self.iteration} - Finished {percentage_str}. Total records fetched: {self.rows_fetched}"
                )
        else:
            logger.error(
                f"Source: {new_state.stream_name} - Iteration {self.iteration} - Failed. A new attempt will be made..."
            )
