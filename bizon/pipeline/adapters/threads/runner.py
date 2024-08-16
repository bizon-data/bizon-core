import time
from threading import Thread

from loguru import logger

from bizon.api.source import APISource
from bizon.backend.client import BackendClient
from bizon.pipeline.producer import Producer
from bizon.pipeline.runner import AbstractRunner

from .consumer import ThreadedConsumer
from .queue import PythonnQueue

IS_ALIVE_CHECK_INTERVAL = 2  # seconds
CONSUMER_START_DELAY = 2  # seconds


class ThreadRunner(AbstractRunner):
    def __init__(self, config: dict):
        super().__init__(config)

    def get_queue(self) -> PythonnQueue:
        return PythonnQueue(queue_size=1000)

    def run(self) -> bool:
        """Run the pipeline with dedicated threads for source and destination"""

        # Create the producer thread
        producer = self.get_producer()
        threaded_producer = Thread(target=producer.run)

        # Create the destination thread
        threaded_consumer = ThreadedConsumer(queue=self.queue, config=self.config.destination)

        # Start the source thread (producer)
        threaded_producer.start()
        logger.info("Producer has started ...")

        self._is_running = True
        time.sleep(CONSUMER_START_DELAY)

        # Start the destination thread (consumer)
        threaded_consumer.start()
        logger.info("Consumer has started ...")

        while threaded_producer.is_alive():
            time.sleep(IS_ALIVE_CHECK_INTERVAL)
        self._is_running = False
        return True
