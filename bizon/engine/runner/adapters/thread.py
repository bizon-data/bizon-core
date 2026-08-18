import concurrent.futures
import time
from threading import Event

from loguru import logger

from bizon.common.models import BizonConfig
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.runner.config import RunnerStatus
from bizon.engine.runner.runner import AbstractRunner


class ThreadRunner(AbstractRunner):
    def __init__(self, config: BizonConfig):
        super().__init__(config)

    # TODO: refacto this
    def get_kwargs(self):
        extra_kwargs = {}

        if self.bizon_config.engine.queue.type == "python_queue":
            from queue import Queue

            queue = Queue(maxsize=self.bizon_config.engine.queue.config.queue.max_size)
            extra_kwargs["queue"] = queue

        return extra_kwargs

    def run(self) -> RunnerStatus:
        """Run the pipeline with dedicated threads for source and destination"""

        extra_kwargs = self.get_kwargs()
        job = AbstractRunner.init_job(bizon_config=self.bizon_config, config=self.config, **extra_kwargs)

        # Store the future results
        result_producer = None
        result_consumer = None

        # Start the producer and consumer events
        producer_stop_event = Event()
        consumer_stop_event = Event()

        extra_kwargs = self.get_kwargs()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.bizon_config.engine.runner.config.max_workers
        ) as executor:
            future_producer = executor.submit(
                AbstractRunner.instanciate_and_run_producer,
                self.bizon_config,
                self.config,
                job.id,
                producer_stop_event,
                **extra_kwargs,
            )
            logger.info("Producer thread has started ...")

            time.sleep(self.bizon_config.engine.runner.config.consumer_start_delay)

            future_consumer = executor.submit(
                AbstractRunner.instanciate_and_run_consumer,
                self.bizon_config,
                self.config,
                job.id,
                consumer_stop_event,
                **extra_kwargs,
            )
            logger.info("Consumer thread has started ...")

            while future_producer.running() and future_consumer.running():
                logger.debug("Producer and consumer are still running ...")
                self._is_running = True
                time.sleep(self.bizon_config.engine.runner.config.is_alive_check_interval)

            self._is_running = False

            if not future_producer.running():
                result_producer: PipelineReturnStatus = future_producer.result()
                logger.info(f"Producer thread stopped running with result: {result_producer}")

                # NOT `result_producer.SUCCESS` -- that is an attribute access on the
                # enum CLASS, which resolves to the SUCCESS member and is always truthy,
                # so the consumer was never stopped on producer failure.
                if result_producer == PipelineReturnStatus.SUCCESS:
                    logger.info("Producer thread has finished successfully, will wait for consumer to finish ...")
                else:
                    logger.error("Producer thread failed, stopping consumer ...")
                    consumer_stop_event.set()

            if not future_consumer.running():
                result_consumer = future_consumer.result()
                logger.info(f"Consumer thread stopped running with result: {result_consumer}")

                if result_consumer == PipelineReturnStatus.SUCCESS:
                    logger.info("Consumer thread has finished successfully")
                else:
                    logger.error("Consumer thread failed, stopping producer ...")
                    producer_stop_event.set()

        runner_status = RunnerStatus(producer=future_producer.result(), consumer=future_consumer.result())

        if not runner_status.is_success:
            logger.error(runner_status.to_string())

        return runner_status
