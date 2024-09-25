from datetime import datetime
from typing import Optional

from loguru import logger
from pytz import UTC

from bizon.engine.backend.models import JobStatus


class Cursor:
    def __init__(self, source_name: str, stream_name: str, job_id: str, total_records: int = None):

        self.source_name: str = source_name
        self.stream_name: str = stream_name
        self._job_id: str = job_id
        self.iteration: int = 0
        self.rows_fetched: int = 0
        self.total_records = total_records
        self.job_status: JobStatus = JobStatus.STARTED
        self.created_at: datetime = datetime.now(tz=UTC)

        # Instantiate empty pagination
        self._pagination: dict = {}

    @classmethod
    def from_db(
        cls,
        source_name: str,
        stream_name: str,
        job_id: str,
        total_records: int,
        iteration: int,
        rows_fetched: int,
        pagination: dict,
    ):
        cursor = cls(source_name=source_name, stream_name=stream_name, job_id=job_id, total_records=total_records)
        cursor.iteration = iteration
        cursor.rows_fetched = rows_fetched
        cursor._pagination = pagination
        return cursor

    @property
    def job_id(self) -> str:
        return self._job_id

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
        if self.job_status == JobStatus.SUCCEEDED:
            return True
        return False

    @property
    def percentage_fetched(self) -> Optional[float]:
        """Return the percentage of records fetched from the total records available in the source"""
        if self.total_records is None:
            return None
        return self.rows_fetched / self.total_records

    @property
    def source_full_name(self) -> str:
        return f"{self.source_name}.{self.stream_name}"

    def update_state(self, pagination_dict: dict, nb_records_fetched: int):

        # - 1. update the pagination
        self._pagination = pagination_dict

        # - 2. update the iteration number
        self.iteration += 1

        # - 3. update the number of rows fetched
        self.rows_fetched += nb_records_fetched

        # - 4 Log the progress with humanized percentage
        percentage_str = f"({self.percentage_fetched:.3%})" if self.percentage_fetched else ""

        logger.info(
            (
                f"Source: {self.source_full_name} - Iteration {self.iteration} - "
                f"Fetched: {self.rows_fetched} {percentage_str} successfully."
            )
        )

        # - 5 Handle next status depending on the pagination

        # *** In case pagination is empty, we consider we finished syncing the source ****
        if pagination_dict is None or len(pagination_dict) == 0:
            self.job_status = JobStatus.SUCCEEDED

            if self.total_records is not None and self.rows_fetched != self.total_records:
                logger.warning(
                    (
                        f"Source: {self.source_full_name} - Iteration {self.iteration} - "
                        f"Total records fetched: {self.rows_fetched} "
                        f"does not match the total records available: {self.total_records}"
                    )
                )
            else:
                logger.info(
                    (
                        f"Source: {self.source_full_name} - Iteration {self.iteration} - "
                        f"Total records fetched: {self.rows_fetched} "
                        f"matches the total records available: {self.total_records}"
                    )
                )
            return

        # *** Otherwise, we consider the job is still running ***
        self.job_status = JobStatus.RUNNING
