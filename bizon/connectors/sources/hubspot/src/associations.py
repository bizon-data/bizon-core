"""v4 associations: one row per (object id, associated id, association type), or plain associated-id lists.

`association_type` is derived from the live `/labels` endpoint, so custom association labels are covered:
  - HUBSPOT_DEFINED labelled "Primary", or unlabelled with no Primary sibling -> "{from}_to_{to}"
  - HUBSPOT_DEFINED unlabelled with a Primary sibling                        -> "{from}_to_{to}_unlabeled"
  - any other labelled type                                                  -> slug of its current label
"""

import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord

from .hubspot_base import LONG_TIMEOUT, URL_BASE, HubSpotBaseSource, HubSpotBaseSourceConfig

URL_ASSOCIATIONS_V4 = f"{URL_BASE}/crm/v4/associations"

BATCH_SIZE = 1000  # v4 associations batch/read cap

_SINGULAR = {
    "deals": "deal",
    "companies": "company",
    "contacts": "contact",
    "line_items": "line_item",
    "tickets": "ticket",
}


def _slugify_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")


def _display_object(to_object: str) -> str:
    return to_object.replace("_", " ")


def _chunks(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _build_type_map(source: HubSpotBaseSource, from_object: str, to_object: str) -> Dict[Tuple[str, int], str]:
    response = source._call(method="GET", url=f"{URL_ASSOCIATIONS_V4}/{from_object}/{to_object}/labels")
    types = response.json().get("results", [])
    has_primary = any(t.get("category") == "HUBSPOT_DEFINED" and t.get("label") == "Primary" for t in types)
    from_singular = _SINGULAR.get(from_object, from_object)
    to_singular = _SINGULAR.get(to_object, to_object)

    # typeIds are only unique within a category: a USER_DEFINED 1 and a HUBSPOT_DEFINED 1 can coexist.
    type_map: Dict[Tuple[str, int], str] = {}
    for t in types:
        type_id, label, category = t["typeId"], t.get("label"), t.get("category")
        if category == "HUBSPOT_DEFINED" and label in ("Primary", None):
            suffix = "_unlabeled" if (label is None and has_primary) else ""
            type_map[(category, type_id)] = f"{from_singular}_to_{to_singular}{suffix}"
        elif label:
            type_map[(category, type_id)] = _slugify_label(label)
        # An unlabelled non-HubSpot type has no stable name, so it is left unmapped and its rows dropped.
    return type_map


def _get_type_map(source: HubSpotBaseSource, from_object: str, to_object: str) -> Dict[Tuple[str, int], str]:
    """One /labels call per (from, to) pair per run, not per page."""
    cache: Optional[dict] = getattr(source, "_association_type_map_cache", None)
    if cache is None:
        cache = {}
        source._association_type_map_cache = cache
    key = (from_object, to_object)
    if key not in cache:
        cache[key] = _build_type_map(source, from_object, to_object)
    return cache[key]


def fetch_typed_associations(source: HubSpotBaseSource, from_object: str, to_object: str, ids: List[str]) -> List[dict]:
    """One row per (object_id, associated_id, association_type). A typeId missing from /labels (e.g. deleted
    between the two calls) is skipped."""
    type_map = _get_type_map(source, from_object, to_object)
    url = f"{URL_ASSOCIATIONS_V4}/{from_object}/{to_object}/batch/read"
    rows: List[dict] = []
    for chunk in _chunks(ids, BATCH_SIZE):
        response = source._call(
            method="POST", url=url, json={"inputs": [{"id": i} for i in chunk]}, timeout=LONG_TIMEOUT
        )
        for result in response.json().get("results", []):
            object_id = str(result["from"]["id"])
            for to in result.get("to", []):
                to_id = str(to["toObjectId"])
                for assoc_type in to.get("associationTypes", []):
                    association_type = type_map.get((assoc_type.get("category"), assoc_type.get("typeId")))
                    if association_type is None:
                        continue
                    rows.append(
                        {
                            "id": object_id,
                            "association_id": to_id,
                            "association_type": association_type,
                            "association_object": _display_object(to_object),
                        }
                    )
    return rows


def fetch_id_lists(
    source: HubSpotBaseSource, from_object: str, to_objects: List[str], ids: List[str]
) -> Dict[str, Dict[str, List[str]]]:
    """{to_object: {from_id: [associated_id, ...]}}"""
    out: Dict[str, Dict[str, List[str]]] = {to: {} for to in to_objects}
    for to_object in to_objects:
        url = f"{URL_ASSOCIATIONS_V4}/{from_object}/{to_object}/batch/read"
        for chunk in _chunks(ids, BATCH_SIZE):
            response = source._call(
                method="POST", url=url, json={"inputs": [{"id": i} for i in chunk]}, timeout=LONG_TIMEOUT
            )
            for result in response.json().get("results", []):
                from_id = str(result["from"]["id"])
                out[to_object][from_id] = [str(t["toObjectId"]) for t in result.get("to", [])]
    return out


class HubSpotAssociationsStreams(str, Enum):
    ASSOCIATION_DEALS = "association_deals"
    ASSOCIATION_NEW_CONTACTS = "association_new_contacts"


_ASSOCIATIONS_FROM_OBJECT = {
    HubSpotAssociationsStreams.ASSOCIATION_DEALS: "deals",
    HubSpotAssociationsStreams.ASSOCIATION_NEW_CONTACTS: "contacts",
}
_ASSOCIATIONS_TO_OBJECTS = {
    HubSpotAssociationsStreams.ASSOCIATION_DEALS: ["companies", "contacts", "line_items"],
    HubSpotAssociationsStreams.ASSOCIATION_NEW_CONTACTS: ["companies", "deals"],
}


class HubSpotAssociationsSourceConfig(HubSpotBaseSourceConfig):
    page_size: int = Field(100, ge=1, le=100, description="v3 list page size used to enumerate ids")


class HubSpotAssociationsSource(HubSpotBaseSource):
    """Ids come from the v3 list API, which unlike search has no result cap, buffered so each batch/read is full."""

    def __init__(self, config: HubSpotAssociationsSourceConfig):
        super().__init__(config)
        self.config: HubSpotAssociationsSourceConfig = config
        self.stream_enum = HubSpotAssociationsStreams(config.stream)
        self.from_object = _ASSOCIATIONS_FROM_OBJECT[self.stream_enum]
        self.to_objects = _ASSOCIATIONS_TO_OBJECTS[self.stream_enum]

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in HubSpotAssociationsStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotAssociationsSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _list_page(self, after: Optional[str]) -> Tuple[list, Optional[str]]:
        params: Dict[str, Any] = {"limit": self.config.page_size}
        if after:
            params["after"] = after
        data = self._call(method="GET", url=f"{URL_BASE}/crm/v3/objects/{self.from_object}", params=params).json()
        return data.get("results", []), data.get("paging", {}).get("next", {}).get("after")

    def get(self, pagination: dict = None) -> SourceIteration:
        after = (pagination or {}).get("after")
        id_updated_at: Dict[str, Optional[str]] = {}

        while len(id_updated_at) < BATCH_SIZE:
            results, after = self._list_page(after)
            if not results:
                after = None
                break
            for record in results:
                id_updated_at[str(record["id"])] = record.get("updatedAt")
            if after is None:
                break

        if not id_updated_at:
            return SourceIteration(next_pagination={}, records=[])

        ids = list(id_updated_at)
        records: List[SourceRecord] = []
        for to_object in self.to_objects:
            for row in fetch_typed_associations(self, self.from_object, to_object, ids):
                row["updatedAt"] = id_updated_at.get(row["id"])
                row["properties"] = None
                records.append(
                    SourceRecord(
                        id=f"{row['id']}:{to_object}:{row['association_id']}:{row['association_type']}",
                        data=row,
                    )
                )

        return SourceIteration(next_pagination={"after": after} if after else {}, records=records)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        # Associations carry no modification time, so an incremental run is a full sweep too.
        return self.get(pagination)
