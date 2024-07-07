from threading import Thread

from loguru import logger

from bizon.api.client import APIClient
from bizon.api.cursor import Cursor
from bizon.api.models.config import SourceConfig

from .constants import QUEUE_TERMINATION
from .queue import PipelineQueue


class SourceThread(Thread):
    def __init__(self, queue: PipelineQueue, client: APIClient):
        super(SourceThread, self).__init__()
        self.name = f"source-{client.config.name.lower()}-{client.config.stream_name.lower()}"
        self.queue = queue
        self.client = client

    def run(self):
        # Get the total number of records
        total_records = self.client.get_total_records_count()

        # First we check if the connection is successful and initialize the cursor
        data, next_state = self.client.get(state=None)
        cursor = Cursor(state=next_state, total_records=total_records)

        # Put the first data in the queue
        self.queue.put(data, index=cursor.iteration)

        while cursor.state.is_finished is False:

            # Break if the max iterations is reached
            if self.client.config.max_iterations is not None and cursor.iteration >= self.client.config.max_iterations:
                break

            # Get the next data
            result, next_state = self.client.get(state=cursor.state)

            # Update the cursor state
            cursor.update_state(next_state)

            # Put the data in the queue
            self.queue.put(result, index=cursor.iteration)

        logger.info("Terminating destination thread ...")
        self.queue.put(QUEUE_TERMINATION, index=cursor.iteration)
        logger.info("Sent termination signal to destination thread ...")
