"""Tipalti accounts-payable source: payees, bills, invoice statuses, payments and balances over the SOAP APIs.

Banking and tax identifiers are stripped from every record. Docs: https://soap-support.tipalti.com/
"""

import hashlib
import hmac
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, HttpRetryConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

PAYEE_API_URL = "https://api.tipalti.com/v6/PayeeFunctions.asmx"
PAYER_API_URL = "https://api.tipalti.com/v14/PayerFunctions.asmx"

TIPALTI_NS = "http://Tipalti.org/"

V8_NS = "V8"

# Case-insensitive substrings; any matching key is dropped.
SENSITIVE_KEY_PATTERNS = [
    "account",
    "routing",
    "iban",
    "swift",
    "ssn",
    "bank",
    "tax_number",
    "vat_number",
    "taxnumber",
    "vatnumber",
]

SENSITIVE_LINE_FIELDS = set(["BankingMessage", "EWalletMessage", "bankingmessage", "ewalletmessage"])

SENSITIVE_PAYMENT_FIELDS = set(
    [
        "PaymentAmountInWithdrawCurrency",
        "WithdrawCurrency",
        "paymentamountinwithdrawcurrency",
        "withdrawcurrency",
    ]
)

PAYEE_BATCH_SIZE = 50
INVOICE_BATCH_SIZE = 200


class TipaltiStreams(str, Enum):
    PAYEES = "payees"
    BILLS = "bills"
    INVOICE_STATUSES = "invoice_statuses"
    PAYMENTS = "payments"
    BALANCES = "balances"


class TipaltiSourceConfig(SourceConfig):
    stream: TipaltiStreams
    api_key: str = Field(..., description="Master encryption key, used as the HMAC key")
    payer_name: str = Field(..., description="Payer name of the Tipalti account, part of every signed request")
    lookback_days: int = 730
    # SOAP returns faults (bad signature, bad request) as HTTP 500, so a 500 is not retried.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(
            retries=HttpRetryConfig(status_forcelist=[429, 502, 503, 504, 520, 522, 524]),
        ),
        description="HTTP timeout/retry policy",
    )


class TipaltiSource(AbstractSource):
    def __init__(self, config: TipaltiSourceConfig):
        super().__init__(config)
        self.config: TipaltiSourceConfig = config
        self._api_key = self.config.api_key
        self._payer_name = self.config.payer_name

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in TipaltiStreams]

    @staticmethod
    def get_config_class() -> type:
        return TipaltiSourceConfig

    def get_authenticator(self) -> Optional[AuthBase]:
        return None

    def get_total_records_count(self) -> Optional[int]:
        return None

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            resp = self._soap_request(
                url=PAYER_API_URL,
                action="GetBalances",
                params=self._auth_params(),
            )
            error_code = self._get_error_code(resp)
            if error_code and error_code not in ("OK", "0"):
                return False, f"API returned error: {error_code}"
            return True, None
        except Exception as e:
            return False, str(e)

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if pagination and pagination.get("done"):
            return SourceIteration(next_pagination={}, records=[])

        dispatch = {
            TipaltiStreams.PAYEES: self._get_payees,
            TipaltiStreams.BILLS: self._get_bills,
            TipaltiStreams.INVOICE_STATUSES: self._get_invoice_statuses,
            TipaltiStreams.PAYMENTS: self._get_payments,
            TipaltiStreams.BALANCES: self._get_balances,
        }

        if stream not in dispatch:
            raise ValueError(f"Unknown stream: {stream}")

        records = dispatch[stream]()
        return SourceIteration(next_pagination={"done": True}, records=records)

    def _generate_hmac(self, idap: Optional[str] = None, eat_params: Optional[List[str]] = None) -> Tuple[str, str]:
        """Return (hex HMAC-SHA256, timestamp) over payerName [+ idap] + timestamp [+ EAT params, in order]."""
        timestamp = str(int(time.time()))

        parts = [self._payer_name]
        if idap:
            parts.append(idap)
        parts.append(timestamp)
        if eat_params:
            parts.extend(eat_params)

        message = "".join(parts)

        key_hex = hmac.new(
            self._api_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return key_hex, timestamp

    def _auth_params(self, idap: Optional[str] = None, eat_params: Optional[List[str]] = None) -> dict:
        key_hex, timestamp = self._generate_hmac(idap, eat_params)
        params = {"payerName": self._payer_name, "timestamp": timestamp, "key": key_hex}
        if idap:
            params["idap"] = idap
        return params

    def _soap_request(self, url: str, action: str, params: dict) -> ET.Element:
        param_xml = ""
        for k, v in params.items():
            if isinstance(v, list):
                items = "".join(f"<ns:string>{item}</ns:string>" for item in v)
                param_xml += f"<ns:{k}>{items}</ns:{k}>"
            elif isinstance(v, bool):
                param_xml += f"<ns:{k}>{'true' if v else 'false'}</ns:{k}>"
            else:
                param_xml += f"<ns:{k}>{v}</ns:{k}>"

        envelope = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"'
            f' xmlns:ns="{TIPALTI_NS}">'
            "<soap:Body>"
            f"<ns:{action}>{param_xml}</ns:{action}>"
            "</soap:Body>"
            "</soap:Envelope>"
        )

        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f"{TIPALTI_NS}{action}",
        }

        response = self.session.post(url, data=envelope.encode("utf-8"), headers=headers, timeout=120)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        body = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
        if body is None:
            raise ValueError(f"No SOAP Body in response for {action}")

        response_elem = body[0] if len(body) > 0 else None
        if response_elem is None:
            raise ValueError(f"Empty SOAP Body for {action}")

        error_code = self._get_error_code(response_elem)
        if error_code and error_code not in ("OK", "0"):
            raise ValueError(f"Tipalti API error for {action}: {error_code}")

        return response_elem

    @staticmethod
    def _get_error_code(elem: ET.Element) -> Optional[str]:
        xpath = ".//" + "{" + TIPALTI_NS + "}" + "errorCode"
        error_elem = elem.find(xpath)
        if error_elem is not None and error_elem.text:
            return error_elem.text
        return None

    @staticmethod
    def _strip_ns(tag: str) -> str:
        if tag.startswith("{"):
            return tag.split("}", 1)[1]
        return tag

    def _elem_to_dict(self, elem: ET.Element) -> dict:
        """A child list whose elements share one tag becomes a list; anything else a nested dict."""
        result = {}
        for child in elem:
            tag = self._strip_ns(child.tag)
            children = list(child)
            if children:
                child_tags = set(c.tag for c in children)
                if len(child_tags) == 1:
                    result[tag] = [self._elem_to_dict(c) for c in children]
                else:
                    result[tag] = self._elem_to_dict(child)
            else:
                result[tag] = child.text
        return result

    @staticmethod
    def _is_sensitive_key(key: str) -> bool:
        key_lower = key.lower()
        return any(pattern in key_lower for pattern in SENSITIVE_KEY_PATTERNS)

    def _filter_dict(self, data: dict) -> dict:
        filtered = {}
        for k, v in data.items():
            if self._is_sensitive_key(k):
                continue
            if isinstance(v, dict):
                filtered[k] = self._filter_dict(v)
            elif isinstance(v, list):
                filtered[k] = [self._filter_dict(item) if isinstance(item, dict) else item for item in v]
            else:
                filtered[k] = v
        return filtered

    def _filter_line_items(self, data: dict) -> dict:
        if "InvoiceLines" in data and isinstance(data["InvoiceLines"], list):
            for line in data["InvoiceLines"]:
                if isinstance(line, dict):
                    for field in SENSITIVE_LINE_FIELDS:
                        line.pop(field, None)
        return data

    def _filter_payment_fields(self, data: dict) -> dict:
        for field in SENSITIVE_PAYMENT_FIELDS:
            data.pop(field, None)
        return data

    def _changed_since_unix(self) -> str:
        dt = datetime.now(timezone.utc) - timedelta(days=self.config.lookback_days)
        return str(int(dt.timestamp()))

    def _get_payee_ids(self) -> List[str]:
        changed_since = self._changed_since_unix()
        params = self._auth_params(eat_params=[changed_since])
        params["changedSince"] = changed_since

        resp = self._soap_request(PAYEE_API_URL, "GetPayeesChangedSinceTimestamp", params)

        ids = []
        for elem in resp.iter():
            tag = self._strip_ns(elem.tag)
            if tag == "string" and elem.text:
                ids.append(elem.text)

        logger.info(f"Found {len(ids)} payee IDs changed in last {self.config.lookback_days} days")
        return ids

    def _get_payees(self) -> List[SourceRecord]:
        payee_ids = self._get_payee_ids()
        records = []

        for i in range(0, len(payee_ids), PAYEE_BATCH_SIZE):
            batch = payee_ids[i : i + PAYEE_BATCH_SIZE]
            params = self._auth_params()
            params["idaps"] = batch

            resp = self._soap_request(PAYEE_API_URL, "GetExtendedPayeeDetailsList", params)

            for detail_elem in resp.iter():
                tag = self._strip_ns(detail_elem.tag)
                if tag == "TipaltiExtendedPayeeDetailsResponse":
                    data = self._elem_to_dict(detail_elem)
                    data = self._filter_dict(data)

                    idap = self._extract_payee_idap(data, batch, len(records))
                    data["Idap"] = idap
                    records.append(SourceRecord(id=str(idap), data=data))

            logger.info(f"Fetched payee batch {i // PAYEE_BATCH_SIZE + 1}, total records: {len(records)}")

        return records

    @staticmethod
    def _extract_payee_idap(data: dict, batch: list, fallback_idx: int) -> str:
        """Idap, else the `Alias` property, else the batch position."""
        if data.get("Idap"):
            return data["Idap"]
        props = data.get("Properties", [])
        if isinstance(props, list):
            for kv in props:
                if isinstance(kv, dict) and kv.get("Key") == "Alias" and kv.get("Value"):
                    return kv["Value"]
        if fallback_idx < len(batch):
            return batch[fallback_idx]
        return f"payee_{fallback_idx}"

    def _get_bills(self) -> List[SourceRecord]:
        """Changed ref codes come from the Payee API, their details from the Payer API."""
        changed_since = self._changed_since_unix()
        params = self._auth_params(eat_params=[changed_since])
        params["changedSince"] = changed_since

        resp = self._soap_request(PAYEE_API_URL, "GetPayeeInvoicesChangedSinceTimestamp", params)

        ref_codes = []
        for elem in resp.iter():
            tag = self._strip_ns(elem.tag)
            if tag == "string" and elem.text:
                ref_codes.append(elem.text)

        logger.info(f"Found {len(ref_codes)} invoice ref codes")

        records = []
        for i in range(0, max(len(ref_codes), 1), INVOICE_BATCH_SIZE):
            batch = ref_codes[i : i + INVOICE_BATCH_SIZE]
            if not batch:
                break

            params = self._auth_params()
            params["invoicesRefCodes"] = batch

            resp = self._soap_request(PAYER_API_URL, "GetPayeeInvoicesListDetails", params)

            for detail_elem in resp.iter():
                tag = self._strip_ns(detail_elem.tag)
                if tag == "TipaltiInvoiceItemResponse":
                    data = self._elem_to_dict(detail_elem)
                    data = self._filter_dict(data)
                    data = self._filter_line_items(data)

                    ref = data.get("InvoiceRefCode", data.get("RefCode", f"bill_{i}"))
                    records.append(SourceRecord(id=str(ref), data=data))

            logger.info(f"Fetched bill batch {i // INVOICE_BATCH_SIZE + 1}, total records: {len(records)}")

        return records

    def _get_invoice_statuses(self) -> List[SourceRecord]:
        payee_ids = self._get_payee_ids()
        records = []

        for idx, idap in enumerate(payee_ids):
            params = self._auth_params(idap=idap)
            params["includePaidInvoices"] = True

            try:
                resp = self._soap_request(PAYEE_API_URL, "GetPayeeInvoiceList", params)

                for inv_elem in resp.iter():
                    tag = self._strip_ns(inv_elem.tag)
                    if tag in ("PayeeInvoiceStatus", "InvoiceStatus", "TipaltiPayeeInvoiceItem"):
                        data = self._elem_to_dict(inv_elem)
                        data = self._filter_dict(data)
                        data["Idap"] = idap

                        ref = data.get("RefCode", data.get("InvoiceRefCode", f"{idap}_{idx}"))
                        records.append(SourceRecord(id=f"{idap}_{ref}", data=data))

            except Exception as e:
                logger.warning(f"Failed to get invoices for payee {idap}: {e}")

            if (idx + 1) % 50 == 0:
                logger.info(f"Processed invoice statuses for {idx + 1}/{len(payee_ids)} payees")

        logger.info(f"Total invoice status records: {len(records)}")
        return records

    def _get_payments(self) -> List[SourceRecord]:
        payee_ids = self._get_payee_ids()
        records = []

        for idx, idap in enumerate(payee_ids):
            params = self._auth_params(idap=idap)

            try:
                resp = self._soap_request(PAYEE_API_URL, "GetLastPaymentDetailsByIdap", params)

                xpath = ".//" + "{" + TIPALTI_NS + "}" + "GetLastPaymentDetailsByIdapResult"
                result_elem = resp.find(xpath)
                target = result_elem if result_elem is not None else resp

                data = self._elem_to_dict(target)
                data = self._filter_dict(data)
                data = self._filter_payment_fields(data)
                data["Idap"] = idap

                error_code = data.get("errorCode", "")
                if error_code in ("OK", "0") and (data.get("RefCode") or data.get("PaymentStatus")):
                    records.append(SourceRecord(id=f"payment_{idap}", data=data))

            except ValueError as e:
                if "RequestDoesNotExist" in str(e):
                    pass  # Payee has no payment history — skip silently
                else:
                    logger.warning(f"Failed to get payment for payee {idap}: {e}")
            except Exception as e:
                logger.warning(f"Failed to get payment for payee {idap}: {e}")

            if (idx + 1) % 50 == 0:
                logger.info(f"Processed payments for {idx + 1}/{len(payee_ids)} payees")

        logger.info(f"Total payment records: {len(records)}")
        return records

    def _get_balances(self) -> List[SourceRecord]:
        resp = self._soap_request(PAYER_API_URL, "GetBalances", self._auth_params())

        records = []
        for elem in resp.iter():
            tag = self._strip_ns(elem.tag)
            if tag == "TipaltiAccountInfo":
                data = self._elem_to_dict(elem)
                data = self._filter_dict(data)
                balance_id = data.get("Currency", data.get("Provider", f"balance_{len(records)}"))
                records.append(SourceRecord(id=str(balance_id), data=data))

        if not records:
            data = self._elem_to_dict(resp)
            data = self._filter_dict(data)
            if data:
                records.append(SourceRecord(id="balances", data=data))

        logger.info(f"Fetched {len(records)} balance records")
        return records
