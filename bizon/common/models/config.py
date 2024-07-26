from pydantic import BaseModel

from bizon.api.models.config import DestinationConfig, SourceConfig
from bizon.pipeline.models.config import PipelineConfig


class SyncConfig(BaseModel):
    source: SourceConfig
    destination: DestinationConfig
    pipeline: PipelineConfig


class BizonRecord(BaseModel):
    _bizon_id: str
    _bizon_loaded_at: str
    _bizon_data: str
