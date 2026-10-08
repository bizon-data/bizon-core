import concurrent.futures
import sys
import time
from multiprocessing import Manager

from loguru import logger

from bizon.engine.runner.config import RunnerStatus
from bizon.engine.runner.runner import AbstractRunner


def _configure_worker_logging(log_level: str):
    # Workers do not inherit the parent's sinks under the spawn start method.
    logger.remove()
    logger.add(sys.stderr, level=log_level)


class ProcessRunner(AbstractRunner):
    def __init__(self, config: dict):
        super().__init__(config)

    def run(self) -> RunnerStatus:
        """Run the pipeline with the producer and the consumer in separate processes"""

        # Queues and events handed to pool workers must be manager proxies: plain multiprocessing
        # primitives can only be shared through inheritance and fail to pickle.
        with Manager() as manager:
            extra_kwargs = {}
            if self.bizon_config.engine.queue.type == "python_queue":
                extra_kwargs["queue"] = manager.Queue(maxsize=self.bizon_config.engine.queue.config.queue.max_size)

            job = AbstractRunner.init_job(bizon_config=self.bizon_config, config=self.config, **extra_kwargs)

            producer_stop_event = manager.Event()
            consumer_stop_event = manager.Event()
            runner_config = self.bizon_config.engine.runner.config

            # Producer and consumer must run at the same time: with one worker the consumer would wait
            # behind a producer blocked on a full queue.
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=max(2, runner_config.max_workers or 2),
                initializer=_configure_worker_logging,
                initargs=(self.bizon_config.engine.runner.log_level.value,),
            ) as executor:
                future_producer = executor.submit(
                    AbstractRunner.instanciate_and_run_producer,
                    self.bizon_config,
                    self.config,
                    job.id,
                    producer_stop_event,
                    **extra_kwargs,
                )
                logger.info("Producer process has started ...")

                time.sleep(runner_config.consumer_start_delay)

                future_consumer = executor.submit(
                    AbstractRunner.instanciate_and_run_consumer,
                    self.bizon_config,
                    self.config,
                    job.id,
                    consumer_stop_event,
                    **extra_kwargs,
                )
                logger.info("Consumer process has started ...")

                self._is_running = True
                concurrent.futures.wait(
                    [future_producer, future_consumer], return_when=concurrent.futures.FIRST_COMPLETED
                )

                # A producer that returns a status has already sent the termination signal, so the
                # consumer stops on its own. One that raised never sent it, and a consumer that stopped
                # leaves the producer blocked on a full queue: stop the other side in both cases.
                if future_producer.done() and future_producer.exception() is not None:
                    logger.error("Producer process failed, stopping consumer ...")
                    consumer_stop_event.set()
                if future_consumer.done() and not future_producer.done():
                    logger.error("Consumer process stopped before the producer, stopping producer ...")
                    producer_stop_event.set()

                concurrent.futures.wait([future_producer, future_consumer])
                self._is_running = False

                # Like the thread runner, an exception raised in a worker propagates to the caller.
                runner_status = RunnerStatus(
                    producer=future_producer.result(), consumer=future_consumer.result(), job_id=job.id
                )

        logger.info(f"Producer process stopped running with result: {runner_status.producer}")
        logger.info(f"Consumer process stopped running with result: {runner_status.consumer}")
        if not runner_status.is_success:
            logger.error(runner_status.to_string())

        return runner_status
