from pydantic import BaseModel

from bizon.api.models.config import SourceConfig
from bizon.destinations.models.config import DestinationConfig
from bizon.pipeline.models.config import PipelineConfig


class SyncConfig(BaseModel):
    source: SourceConfig
    destination: DestinationConfig
    pipeline: PipelineConfig
