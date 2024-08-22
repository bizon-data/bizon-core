import time
from threading import Thread

from loguru import logger

from bizon.common.models import BizonConfig
from bizon.engine.runner import AbstractRunner

IS_ALIVE_CHECK_INTERVAL = 2  # seconds
CONSUMER_START_DELAY = 5  # seconds


class ThreadRunner(AbstractRunner):
    def __init__(self, config: BizonConfig):
        super().__init__(config)

    def run(self) -> bool:
        """Run the pipeline with dedicated threads for source and destination"""

        # Create the producer thread
        producer = self.get_producer()
        threaded_producer = Thread(target=producer.run, args=(self.job.id,))

        # Create the destination thread
        consumer = self.queue.get_consumer(destination=self.destination)
        threaded_consumer = Thread(target=consumer.run)

        # Start the source thread (producer)
        threaded_producer.start()
        logger.info("Producer has started ...")

        self._is_running = True
        time.sleep(CONSUMER_START_DELAY)

        # Start the destination thread (consumer)
        threaded_consumer.start()
        logger.info("Consumer has started ...")

        # TODO: add self-healing mechanism
        while threaded_consumer.is_alive():
            time.sleep(IS_ALIVE_CHECK_INTERVAL)
        self._is_running = False
        return True
