from typing import Optional

from pydantic import BaseModel, Field

from bizon.engine.queue.config_details import AbastractQueueConfigDetails

QUEUE_TERMINATION = "TERMINATION"


class PythonQueueConfigDetails(BaseModel):
    max_size: int = Field(1000, description="Maximum size of the queue")


class PythonQueueConsumerConfig(BaseModel):
    poll_interval: int = Field(1, description="Interval in seconds to poll the queue in seconds")


class PythonQueueConfig(AbastractQueueConfigDetails):
    queue: PythonQueueConfigDetails = Field(
        PythonQueueConfigDetails(max_size=1000), description="Configuration of the queue"
    )

    consumer: PythonQueueConsumerConfig = Field(
        PythonQueueConsumerConfig(poll_interval=1), description="Kafka consumer configuration"
    )
