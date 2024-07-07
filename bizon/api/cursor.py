from typing import Optional

from loguru import logger

from .models.state import AbstractState, FetchStatus


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

    @property
    def percentage_fetched(self) -> Optional[float]:
        """Return the percentage of records fetched from the total records available in the source"""
        if self.total_records is None:
            return None
        return self.rows_fetched / self.total_records

    def update_state(self, new_state: AbstractState):

        if new_state.fetch_status == FetchStatus.SUCCESS:
            # If the fetch was successful
            # - 1. update the cursor state
            self._state = new_state
            # - 2. update the number of rows fetched
            self.rows_fetched += new_state.records_fetched
            # - 3. update the iteration number
            self.iteration += 1

            # - 4 Log the progress with humanized percentage
            percentage_str = f"({self.percentage_fetched:.3%})" if self.percentage_fetched else ""

            logger.info(
                (
                    f"Source: {new_state.stream_name} - Iteration {self.iteration} - "
                    f"Fetched: {self.rows_fetched} {percentage_str} successfully."
                )
            )

            # - 5. Check if the state is finished and set the cursor as finished
            if new_state.is_finished:
                self.is_finished = True
                logger.info(
                    (
                        f"Source: {new_state.stream_name} - Iteration {self.iteration} - Finished {percentage_str}."
                        f"Total records fetched: {self.rows_fetched}"
                    )
                )
        # TODO: If the fetch was not successful, log the error and retry
        else:
            logger.error(
                f"Source: {new_state.stream_name} - Iteration {self.iteration} - Failed. A new attempt will be made..."
            )
