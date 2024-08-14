from datetime import datetime
from enum import Enum
from typing import Optional

from loguru import logger
from pytz import UTC


class FetchStatus(str, Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class Cursor:
    def __init__(self, stream_name: str, total_records: int = None):

        self.stream_name: str = stream_name
        self.iteration: int = 0
        self.rows_fetched: int = 0
        self.total_records = total_records
        self.fetch_status: FetchStatus = FetchStatus.NOT_STARTED
        self.created_at: datetime = datetime.now(tz=UTC)

        # Instantiate empty pagination
        self._pagination: dict = {}

    @property
    def pagination(self) -> dict:
        if self._pagination:
            return self._pagination

        # If it's the first iteration, we return an empty dict
        if self.iteration == 0:
            return dict()

        raise ValueError("Pagination is empty")

    @property
    def is_finished(self) -> bool:
        if self.fetch_status == FetchStatus.SUCCESS:
            return True
        return False

    @property
    def percentage_fetched(self) -> Optional[float]:
        """Return the percentage of records fetched from the total records available in the source"""
        if self.total_records is None:
            return None
        return self.rows_fetched / self.total_records

    def update_state(self, pagination_dict: dict, nb_records_fetched: int):

        # In case pagination is empty, we consider we finished syncing the source
        if pagination_dict is None or len(pagination_dict) == 0:
            self.fetch_status = FetchStatus.SUCCESS
            logger.info(
                (
                    f"Source: {self.stream_name} - Iteration {self.iteration} - Finished {percentage_str}."
                    f"Total records fetched: {self.rows_fetched}"
                )
            )

            if self.total_records is not None and self.rows_fetched != self.total_records:
                logger.warning(
                    (
                        f"Source: {self.stream_name} - Iteration {self.iteration} - "
                        f"Total records fetched: {self.rows_fetched} "
                        f"does not match the total records available: {self.total_records}"
                    )
                )
            return

        # - 1. update the pagination
        self._pagination = pagination_dict

        # - 2. update the iteration number
        self.iteration += 1

        # - 3. update the number of rows fetched
        self.rows_fetched += nb_records_fetched

        # - 4. update the fetch status
        self.fetch_status = FetchStatus.RUNNING

        # - 5 Log the progress with humanized percentage
        percentage_str = f"({self.percentage_fetched:.3%})" if self.percentage_fetched else ""

        logger.info(
            (
                f"Source: {self.stream_name} - Iteration {self.iteration} - "
                f"Fetched: {self.rows_fetched} {percentage_str} successfully."
            )
        )
