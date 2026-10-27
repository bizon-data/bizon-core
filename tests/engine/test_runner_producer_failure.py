import os
import threading
from queue import Queue
from unittest.mock import Mock

from bizon.engine.engine import RunnerFactory
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.adapters.python_queue.consumer import PythonQueueConsumer
from bizon.engine.runner.runner import AbstractRunner

RUN_DEADLINE_SECONDS = 15


def _runner():
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    runner.bizon_config.engine.runner.config.consumer_start_delay = 0
    return runner


def _run_with_deadline(run):
    outcome = {}

    def target():
        try:
            outcome["status"] = run()
        except BaseException as e:
            outcome["error"] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(RUN_DEADLINE_SECONDS)
    assert not thread.is_alive(), "runner hung"
    return outcome


def test_thread_runner_stops_the_consumer_when_the_producer_raises(monkeypatch):
    def producer_that_raises(*args, **kwargs):
        raise RuntimeError("producer died before sending a termination signal")

    monkeypatch.setattr(AbstractRunner, "instanciate_and_run_producer", staticmethod(producer_that_raises))
    outcome = _run_with_deadline(_runner().run)

    assert isinstance(outcome.get("error"), RuntimeError)


def test_thread_runner_lets_the_consumer_drain_when_the_producer_returns_a_failure(monkeypatch):
    def producer_that_fails(bizon_config, config, job_id, stop_event, **kwargs):
        queue = AbstractRunner.get_queue(bizon_config=bizon_config, **kwargs)
        queue.terminate(iteration=0, success=False)
        return PipelineReturnStatus.SOURCE_ERROR

    monkeypatch.setattr(AbstractRunner, "instanciate_and_run_producer", staticmethod(producer_that_fails))
    outcome = _run_with_deadline(_runner().run)

    status = outcome["status"]
    assert status.producer == PipelineReturnStatus.SOURCE_ERROR
    # Drained on the error signal rather than killed by the runner.
    assert status.consumer == PipelineReturnStatus.SOURCE_ERROR


def test_python_queue_consumer_sees_the_stop_event_while_the_queue_is_empty():
    consumer = PythonQueueConsumer(config=Mock(), queue=Queue(), destination=Mock(), transform=Mock(), monitor=Mock())
    stop_event = threading.Event()
    threading.Timer(0.2, stop_event.set).start()

    outcome = _run_with_deadline(lambda: consumer.run(stop_event))

    assert outcome["status"] == PipelineReturnStatus.KILLED_BY_RUNNER
