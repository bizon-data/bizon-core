from threading import Thread

from loguru import logger

from bizon.api.client import APIClient
from bizon.api.cursor import Cursor

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

        # Initialize the cursor
        cursor = Cursor(stream_name=self.client.config.stream_name, total_records=total_records)

        # First we check if the connection is successful and initialize the cursor
        check_connection, connection_error = self.client.check_connection()

        if not check_connection:
            logger.error(f"Error while connecting to source: {connection_error}")
            return

        while not cursor.is_finished and cursor.iteration <= self.client.config.max_iterations:

            # Get the next data
            pagination_dict, records = self.client.get(pagination=cursor.pagination)

            # Put the data in the queue
            self.queue.put(records=records, index=cursor.iteration)

            # Update the cursor state
            cursor.update_state(pagination_dict=pagination_dict, nb_records_fetched=len(records))

        logger.info("Terminating destination thread ...")
        self.queue.put(QUEUE_TERMINATION, index=cursor.iteration)
        logger.info("Sent termination signal to destination thread ...")
