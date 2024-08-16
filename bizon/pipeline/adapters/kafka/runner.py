import time
from threading import Thread

from loguru import logger

from bizon.pipeline.runner import AbstractRunner

from .consumer import ThreadedKafkaConsumer
from .queue import KafkaQueue

IS_ALIVE_CHECK_INTERVAL = 2  # seconds


class KafkaRunner(AbstractRunner):
    def __init__(self, config: dict):
        super().__init__(config)

    def get_queue(self) -> KafkaQueue:
        return KafkaQueue(config=self.config.pipeline.runner_config)

    def run(self) -> bool:
        """Run the pipeline with dedicated threads for source and destination"""

        # Create the producer thread
        producer = self.get_producer()
        threaded_producer = Thread(target=producer.run)

        # Start the source thread (producer)
        threaded_producer.start()
        logger.info("Producer has started ...")

        self._is_running = True

        # Create the Kafka consumer if needed
        if self.config.consumer.enabled:
            kafka_consumer = ThreadedKafkaConsumer(config=self.config.consumer, topic="bizon")
            kafka_consumer.start()

        while threaded_producer.is_alive():
            time.sleep(IS_ALIVE_CHECK_INTERVAL)
        self._is_running = False
        return True
