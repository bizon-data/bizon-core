from pydantic import BaseModel

from bizon.engine.pipeline.consumer import ConsumerReturnStatus
from bizon.engine.pipeline.producer import ProducerReturnStatus


class RunResult(BaseModel):
    producer_status: ProducerReturnStatus
    consumer_status: ConsumerReturnStatus
