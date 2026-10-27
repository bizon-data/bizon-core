"""SpotDraft CLM source: contracts with metadata, key pointers, download URLs and optional PDF text."""

import io
from typing import List, Optional, Tuple

import pymupdf
import requests
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, HttpRetryConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class SpotDraftSourceConfig(SourceConfig):
    client_id: str
    client_secret: str
    base_url: str = "https://api.spotdraft.com/api"
    extract_pdf_text: bool = Field(True, description="Extract the text content of each contract PDF")
    # GET only: the one POST (/download_link) is non-idempotent, and its failure is already non-fatal.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(retries=HttpRetryConfig(allowed_methods=["GET"])),
        description="HTTP timeout/retry policy",
    )


class SpotDraftSource(AbstractSource):
    """API docs: https://api.spotdraft.com/api/docs/"""

    PAGE_SIZE = 50

    def __init__(self, config: SpotDraftSourceConfig):
        super().__init__(config)
        self.config: SpotDraftSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        """Return available streams."""
        return ["contracts"]

    @staticmethod
    def get_config_class() -> type:
        """Return the config class."""
        return SpotDraftSourceConfig

    def get_authenticator(self) -> AuthBase | None:
        """Return authenticator if needed."""
        return None

    def _get_headers(self) -> dict:
        """Return headers for API requests.

        SpotDraft uses header-based authentication with Client-ID and Client-Secret.
        """
        return {
            "Client-ID": self.config.client_id,
            "Client-Secret": self.config.client_secret,
            "Content-Type": "application/json",
        }

    def _make_request(self, method: str, endpoint: str, params: dict = None, json: dict = None) -> requests.Response:
        """Make an authenticated API request."""
        url = f"{self.config.base_url}{endpoint}"
        response = self.session.request(
            method,
            url,
            headers=self._get_headers(),
            params=params,
            json=json,
            timeout=60,
        )
        response.raise_for_status()
        return response

    def check_connection(self) -> Tuple[bool, str | None]:
        """Test the connection by fetching a single contract."""
        try:
            self._make_request("GET", "/v2.1/public/contracts/", params={"limit": 1, "page": 1})
            return True, None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                return False, "Authentication failed. Check client_id and client_secret."
            if e.response.status_code == 403:
                return False, "Access forbidden. Check API permissions."
            return False, f"HTTP error: {e.response.status_code} - {e.response.text}"
        except requests.exceptions.RequestException as e:
            return False, f"Connection failed: {str(e)}"

    def get_total_records_count(self) -> int | None:
        """Return total record count if known."""
        return None

    def _get_contract_key_pointers(self, contract_id: str) -> dict | None:
        """Fetch AI-extracted key pointers for a contract.

        Args:
            contract_id: Contract ID in format T-123 (template) or H-123 (historical)
        """
        try:
            response = self._make_request("GET", f"/v2.1/public/contracts/{contract_id}/key_pointers")
            return response.json()
        except requests.exceptions.RequestException:
            # Key pointers may not be available for all contracts
            return None

    def _get_contract_download_link(self, contract_id: str) -> str | None:
        """Fetch the PDF download URL for a contract.

        Args:
            contract_id: Contract composite ID (e.g., T-123 or H-123)
        """
        try:
            # Download link endpoint uses POST method
            response = self._make_request("POST", f"/v2.1/public/contracts/{contract_id}/download_link")
            if response.status_code == 204:
                return None
            data = response.json()
            return data.get("download_url") or data.get("url") or data.get("link")
        except requests.exceptions.RequestException:
            # Download link may not be available for all contracts
            return None

    def _extract_pdf_text(self, download_url: str) -> str | None:
        """Download PDF and extract text content using PyMuPDF.

        Args:
            download_url: URL to download the PDF from

        Returns:
            Extracted text content or None if extraction fails
        """
        try:
            # Not self.session: download_url is a pre-signed storage URL that must not receive the API credentials.
            response = requests.get(download_url, timeout=120)
            response.raise_for_status()

            # Load PDF from bytes
            pdf_bytes = io.BytesIO(response.content)
            doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")

            # Extract text from all pages
            text_parts = []
            for page in doc:
                text = page.get_text()
                if text.strip():
                    text_parts.append(text)

            doc.close()

            if text_parts:
                return "\n\n".join(text_parts)
            return None

        except Exception:
            # PDF extraction may fail for various reasons (encrypted, corrupted, etc.)
            return None

    def _get_composite_id(self, contract: dict) -> str:
        """Get the composite ID for a contract (e.g., T-123 or H-123)."""
        # Try to get composite_id directly
        composite_id = contract.get("composite_id")
        if composite_id:
            return composite_id

        # Get the contract ID
        contract_id = contract.get("id")
        if not contract_id:
            return ""

        # If the ID already has a prefix (e.g., T-123 or H-123), use it as-is
        contract_id_str = str(contract_id)
        if contract_id_str and len(contract_id_str) > 2 and contract_id_str[1] == "-":
            return contract_id_str

        # Otherwise, construct from id and type prefix
        contract_type_prefix = contract.get("type_prefix", "H")  # H for historical, T for template
        return f"{contract_type_prefix}-{contract_id_str}"

    def _transform_contract(self, contract: dict, enrich: bool = True) -> dict:
        """Transform a contract record with enriched data.

        Args:
            contract: Raw contract data from API
            enrich: Whether to fetch additional data (key_pointers, download_url, pdf_text)
        """
        composite_id = self._get_composite_id(contract)
        contract_id = contract.get("id") or composite_id

        # Fetch additional data if enrichment is enabled
        key_pointers = None
        download_url = None
        pdf_text = None

        if enrich:
            key_pointers = self._get_contract_key_pointers(composite_id)
            download_url = self._get_contract_download_link(composite_id)

            # Extract PDF text if enabled and download URL is available
            if self.config.extract_pdf_text and download_url:
                pdf_text = self._extract_pdf_text(download_url)

        # Extract parties/counterparties info
        parties = contract.get("parties", []) or contract.get("counterparties", []) or []
        counterparty_names = []
        for party in parties:
            if isinstance(party, dict):
                name = party.get("name") or party.get("party_name")
                if name:
                    counterparty_names.append(name)
            elif isinstance(party, str):
                counterparty_names.append(party)

        # Extract owner info
        owner = contract.get("owner")
        owner_email = None
        if isinstance(owner, dict):
            owner_email = owner.get("email") or owner.get("name")
        elif isinstance(owner, str):
            owner_email = owner

        # Extract template info
        template = contract.get("template")
        template_name = None
        if isinstance(template, dict):
            template_name = template.get("name") or template.get("title")
        elif isinstance(template, str):
            template_name = template

        return {
            "id": str(contract_id),
            "composite_id": composite_id,
            "title": contract.get("title") or contract.get("name"),
            "status": contract.get("status"),
            "contract_type": contract.get("contract_type") or contract.get("type"),
            "created_at": contract.get("created_at") or contract.get("created"),
            "updated_at": contract.get("updated_at") or contract.get("modified"),
            "effective_date": contract.get("effective_date") or contract.get("start_date"),
            "expiry_date": contract.get("expiry_date") or contract.get("end_date"),
            "value": contract.get("value") or contract.get("contract_value"),
            "currency": contract.get("currency"),
            "counterparty_names": counterparty_names if counterparty_names else None,
            "owner": owner_email,
            "template_name": template_name,
            "key_pointers": key_pointers,
            "download_url": download_url,
            "pdf_text": pdf_text,
            "metadata": contract.get("metadata") or contract.get("custom_fields"),
        }

    def get(self, pagination: dict = None) -> SourceIteration:
        """Fetch contracts from SpotDraft.

        Uses page-based pagination with limit parameter.
        """
        pagination = pagination or {}
        page = pagination.get("page", 1)

        # Fetch contracts list
        response = self._make_request(
            "GET",
            "/v2.1/public/contracts/",
            params={
                "limit": self.PAGE_SIZE,
                "page": page,
            },
        )

        data = response.json()

        # Handle different response formats
        if isinstance(data, list):
            contracts = data
            total_count = None
        else:
            contracts = data.get("results", data.get("contracts", data.get("data", [])))
            total_count = data.get("total_results") or data.get("count") or data.get("total")

        records = []
        for contract in contracts:
            transformed = self._transform_contract(contract, enrich=True)
            records.append(SourceRecord(id=transformed["id"], data=transformed))

        # Determine next pagination
        next_pagination = {}
        if len(contracts) == self.PAGE_SIZE:
            # More records likely available
            next_pagination = {"page": page + 1}
        elif total_count is not None:
            # Check if we've fetched all records
            fetched_so_far = (page - 1) * self.PAGE_SIZE + len(contracts)
            if fetched_so_far < total_count:
                next_pagination = {"page": page + 1}

        return SourceIteration(next_pagination=next_pagination, records=records)
