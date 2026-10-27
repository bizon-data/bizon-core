from typing import List, Optional

from loguru import logger
from pydantic import Field

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord

from .hubspot_base import LONG_TIMEOUT, URL_BASE, HubSpotBaseSource, HubSpotBaseSourceConfig

URL_CONTACTS_BATCH_PROFILE = f"{URL_BASE}/contacts/v1/contact/vids/batch/"

# Every contact with a merge audit carries this property, so searching on it avoids sweeping all contacts.
_MERGED_FILTER = {"propertyName": "hs_merged_object_ids", "operator": "HAS_PROPERTY"}


class HubSpotMergedAuditSourceConfig(HubSpotBaseSourceConfig):
    page_size: int = Field(
        100,
        ge=1,
        le=100,
        description="Contacts per search page and vids per batch-profile call (HubSpot caps both at 100)",
    )


class HubSpotContactsMergedAuditSource(HubSpotBaseSource):
    """Merged contacts with their merge-audit trail: a search for contacts carrying `hs_merged_object_ids`, then
    one v1 batch-profile call per page for their audits."""

    def __init__(self, config: HubSpotMergedAuditSourceConfig):
        super().__init__(config)
        self.config: HubSpotMergedAuditSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return ["contacts_merged_audit"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotMergedAuditSourceConfig

    def _search_merged_contacts(self, last_id: Optional[str]) -> List[dict]:
        # Top-level `updatedAt` comes back on every result, so the id is the only property needed.
        return self.search_page(
            object_type="contacts",
            properties=["hs_object_id"],
            limit=self.config.page_size,
            last_id=last_id,
            filters=[_MERGED_FILTER],
        )

    def _fetch_merge_audits(self, vids: List[str]) -> dict:
        """v1 contact profiles keyed by vid, stripped to the merge audits and a single property."""
        params = [("vid", vid) for vid in vids]
        params.extend(
            [
                ("showMergeAudits", "true"),
                ("formSubmissionMode", "none"),
                ("showListMemberships", "false"),
                ("propertyMode", "value_only"),
                ("property", "email"),
            ]
        )
        return self._call(method="GET", url=URL_CONTACTS_BATCH_PROFILE, params=params).json()

    def get(self, pagination: dict = None) -> SourceIteration:
        last_id = (pagination or {}).get("last_id")
        results = self._search_merged_contacts(last_id=last_id)
        if not results:
            return SourceIteration(next_pagination={}, records=[])

        updated_at_by_vid = {result["id"]: result.get("updatedAt") for result in results}

        records = []
        seen_canonical_vids = set()
        for profile in self._fetch_merge_audits(vids=list(updated_at_by_vid)).values():
            merge_audits = profile.get("merge-audits") or []
            # The search filter is a superset of "has merge audits".
            if not merge_audits:
                continue

            # A merged secondary vid resolves to its canonical contact, so two requested vids can return one record.
            canonical_vid = str(profile["canonical-vid"])
            if canonical_vid in seen_canonical_vids:
                continue
            seen_canonical_vids.add(canonical_vid)

            records.append(
                SourceRecord(
                    id=canonical_vid,
                    data={
                        "id": canonical_vid,
                        "updatedAt": updated_at_by_vid.get(canonical_vid),
                        # Verbatim, with the v1 API's hyphenated inner keys.
                        "merge_audits": merge_audits,
                    },
                )
            )

        return SourceIteration(
            next_pagination={"last_id": str(max(int(vid) for vid in updated_at_by_vid))},
            records=records,
        )

    def get_total_records_count(self) -> Optional[int]:
        response = self._call(
            method="POST",
            url=f"{URL_BASE}/crm/v3/objects/contacts/search",
            json={"filterGroups": [{"filters": [_MERGED_FILTER]}], "properties": ["hs_object_id"], "limit": 1},
            timeout=LONG_TIMEOUT,
        )
        total = response.json()["total"]
        logger.info(f"Number of merged contacts in HubSpot: {f'{total:,}'.replace(',', ' ')}")
        return total or None
