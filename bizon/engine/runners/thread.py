import concurrent.futures
import time

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

        # Create the destination thread
        consumer = self.queue.get_consumer(destination=self.destination)

        # Store the future results
        result_producer = None
        result_consumer = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_producer = executor.submit(producer.run, self.job.id)
            logger.info("Producer has started ...")

            time.sleep(CONSUMER_START_DELAY)

            future_consumer = executor.submit(consumer.run)
            logger.info("Consumer has started ...")

            self._is_running = True

            while future_producer.running() and future_consumer.running():
                logger.debug("Producer and consumer are still running ...")
                self._is_running = True
                time.sleep(IS_ALIVE_CHECK_INTERVAL)

            self._is_running = False

            if not future_producer.running():
                result_producer = future_producer.result()
                logger.info(f"Producer stopped running with result: {result_producer}")

            if not future_consumer.running():
                result_consumer = future_consumer.result()
                logger.info(f"Consumer stopped running with result: {result_consumer}")

        return True
