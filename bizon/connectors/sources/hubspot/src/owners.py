from typing import Any, Dict, List, Optional

from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord

from .hubspot_base import URL_BASE, HubSpotBaseSource, HubSpotBaseSourceConfig


class HubSpotOwnersSourceConfig(HubSpotBaseSourceConfig):
    page_size: int = Field(100, ge=1, le=100)


class HubSpotOwnersSource(HubSpotBaseSource):
    """Active then archived owners in one stream. An owner can appear in both sweeps, so the id carries the phase."""

    def __init__(self, config: HubSpotOwnersSourceConfig):
        super().__init__(config)
        self.config: HubSpotOwnersSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return ["owners"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotOwnersSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get(self, pagination: dict = None) -> SourceIteration:
        pagination = pagination or {}
        phase = pagination.get("phase", "active")
        params: Dict[str, Any] = {
            "limit": self.config.page_size,
            "archived": "true" if phase == "archived" else "false",
        }
        after = pagination.get("after")
        if after:
            params["after"] = after

        data = self._call(method="GET", url=f"{URL_BASE}/crm/v3/owners", params=params).json()
        records = [
            SourceRecord(id=f"{r['id']}:{phase}", data={**r, "archived": phase == "archived"})
            for r in data.get("results", [])
        ]
        next_after = data.get("paging", {}).get("next", {}).get("after")
        if next_after:
            return SourceIteration(next_pagination={"phase": phase, "after": next_after}, records=records)
        if phase == "active":
            return SourceIteration(next_pagination={"phase": "archived"}, records=records)
        return SourceIteration(next_pagination={}, records=records)
