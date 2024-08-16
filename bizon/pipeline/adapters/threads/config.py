from typing import Optional

from pydantic import BaseModel, Field


class ThreadsConfig(BaseModel):
    max_queue_size: Optional[int] = Field(1000, description="Maximum size of the queue")
    consumer_start_delay: Optional[int] = Field(
        2, description="Duration in seconds to wait before starting the consumer thread"
    )
