from typing import List, Type

from pydantic import BaseModel

from bizon.api.source import APISource

# Dummy
from .dummy.src.config import DummySourceConfig, DummyStreams
from .dummy.src.source import DummySource

# HubSpot
from .hubspot.src.models.config import HubSpotObjects, HubSpotSourceConfig
from .hubspot.src.source import HubSpotSource

# Periscope
from .periscope.src.config import PeriscopeSourceConfig, PeriscopeStreams
from .periscope.src.source import PeriscopeSource


class Stream(BaseModel):
    name: str
    client: Type[APISource]
    config: Type[BaseModel]

    def get_instance(self, raw_source_config: dict) -> APISource:
        return self.client(config=self.config.model_validate(raw_source_config))


class Source(BaseModel):
    name: str
    streams: List[Stream]

    @property
    def available_streams(self) -> List[str]:
        return [stream.name for stream in self.streams]

    def get_stream_by_name(self, name: str) -> Stream:
        for stream in self.streams:
            if stream.name == name:
                return stream
        raise ValueError(f"Stream {name} not found. Available streams are {self.available_streams}")


class SourceMap(BaseModel):
    sources: List[Source]

    @property
    def available_sources(self) -> List[str]:
        return [source.name for source in self.sources]

    def get_source_by_name(self, name: str) -> Source:
        for source in self.sources:
            if source.name == name:
                return source
        raise ValueError(f"Source {name} not found. Available sources are {self.available_sources}")

    def get_instance(self, source_name: str, stream_name: str, raw_source_config: dict) -> APISource:
        source = self.get_source_by_name(source_name)
        stream = source.get_stream_by_name(stream_name)
        return stream.get_instance(raw_source_config)


SOURCES = SourceMap(
    sources=[
        Source(
            name="dummy",
            streams=[
                Stream(name=DummyStreams.CREATURES, client=DummySource, config=DummySourceConfig),
                Stream(name=DummyStreams.PLANTS, client=DummySource, config=DummySourceConfig),
            ],
        ),
        Source(
            name="hubspot",
            streams=[
                Stream(name=HubSpotObjects.CONTACTS, client=HubSpotSource, config=HubSpotSourceConfig),
                Stream(name=HubSpotObjects.COMPANIES, client=HubSpotSource, config=HubSpotSourceConfig),
                Stream(name=HubSpotObjects.DEALS, client=HubSpotSource, config=HubSpotSourceConfig),
            ],
        ),
        Source(
            name="periscope",
            streams=[
                Stream(name=PeriscopeStreams.DASHBOARDS, client=PeriscopeSource, config=PeriscopeSourceConfig),
                Stream(name=PeriscopeStreams.CHARTS, client=PeriscopeSource, config=PeriscopeSourceConfig),
            ],
        ),
    ]
)

__all__ = [
    "SOURCES",
    "SourceMap",
]
