import concurrent.futures
import time

from loguru import logger

from bizon.engine.runner.runner import AbstractRunner


class ProcessRunner(AbstractRunner):

    def __init__(self, config: dict):
        super().__init__(config)

    # TODO: refacto this
    def get_kwargs(self):
        if self.bizon_config.engine.queue.type == "python_queue":
            from multiprocessing import Manager

            manager = Manager()
            queue = manager.Queue(maxsize=self.bizon_config.engine.queue.config.queue.max_size)
            return {"queue": queue}

        return {}

    def run(self):
        """Run the pipeline with dedicated threads for source and destination"""

        extra_kwargs = self.get_kwargs()
        job = AbstractRunner.init_job(bizon_config=self.bizon_config, config=self.config, **extra_kwargs)

        # Store the future results
        result_producer = None
        result_consumer = None

        if self.bizon_config.engine.queue.type == "python_queue" and "sqlite" in self.bizon_config.engine.backend.type:
            logger.error("Multiprocessing.Queue is not compatible with sqlite backend")
            raise ValueError("Multiprocessing.Queue is not compatible with sqlite backend")

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=self.bizon_config.engine.runner.config.max_workers
        ) as executor:

            future_producer = executor.submit(
                AbstractRunner.instanciate_and_run_producer,
                self.bizon_config,
                self.config,
                job.id,
                **extra_kwargs,
            )
            logger.info("Producer process has started ...")

            time.sleep(self.bizon_config.engine.runner.config.consumer_start_delay)

            future_consumer = executor.submit(
                AbstractRunner.instanciate_and_run_consumer,
                self.bizon_config,
                job.id,
                **extra_kwargs,
            )
            logger.info("Consumer process has started ...")

            self._is_running = True

            while future_producer.running() and future_consumer.running():
                logger.debug("Producer and consumer are still running ...")
                self._is_running = True
                time.sleep(self.bizon_config.engine.runner.config.is_alive_check_interval)

            self._is_running = False

            if not future_producer.running():
                result_producer = future_producer.result()
                logger.info(f"Producer process stopped running with result: {result_producer}")

            if not future_consumer.running():
                result_consumer = future_consumer.result()
                logger.info(f"Consumer process stopped running with result: {result_consumer}")

        return True
