#!/usr/bin/env python3
"""Ingest Senate financial disclosure data from the eFD search APIs."""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import hashlib
import io
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse

try:
    import requests
except ImportError as exc:  # pragma: no cover - requests is required in production
    raise SystemExit("The senate ingestion pipeline requires the 'requests' package") from exc


MODULE_ROOT = Path(__file__).resolve().parents[1]
if str(MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(MODULE_ROOT))

from parsing_utils import (  # noqa: E402
    normalize_text,
    parse_amount_range as _parse_amount_range,
    parse_date as _parse_date,
)


LOG = logging.getLogger(__name__)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
)

BASE_URL = "https://efdsearch.senate.gov/search/"
SEARCH_ENDPOINT = urljoin(BASE_URL, "report/data/")
DOWNLOAD_ENDPOINT = urljoin(BASE_URL, "view/download/")
CSV_FALLBACK_ENDPOINT = urljoin(BASE_URL, "report/download/")


DATE_FORMATS: Tuple[str, ...] = (
    "%m/%d/%Y",
    "%m/%d/%y",
    "%Y-%m-%d",
    "%Y/%m/%d",
)


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    return _parse_date(value, formats=DATE_FORMATS, logger=LOG)


def normalize_whitespace(value: Optional[str]) -> str:
    return normalize_text(value)


def select_first(mapping: Dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        if key in mapping and mapping[key]:
            return str(mapping[key])
    return None


def parse_amount_range(amount_text: str) -> Tuple[Optional[int], Optional[int]]:
    return _parse_amount_range(amount_text)


@dataclass
class SenateFiling:
    filing_id: str
    report_id: str
    document_id: str
    log_number: Optional[str]
    filed_at: dt.date
    report_type: str
    filer_type: str
    senator_first_name: str
    senator_last_name: str
    office: Optional[str]
    state: Optional[str]
    pdf_url: Optional[str]
    csv_href: Optional[str]
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def senator_name(self) -> str:
        return normalize_whitespace(f"{self.senator_first_name} {self.senator_last_name}")

    @classmethod
    def from_api(cls, row: Dict[str, Any]) -> "SenateFiling":
        filing_id = str(select_first(row, "filingId", "filing_id", "id") or "")
        report_id = str(select_first(row, "reportId", "report_id") or "")
        document_id = str(select_first(row, "documentId", "docId", "document_id") or "")
        log_number = select_first(row, "logNumber", "logNo", "log_number")
        filed_at = parse_date(select_first(row, "dateFiled", "filedDate", "dateReceived", "adate"))
        report_type = normalize_whitespace(select_first(row, "reportTypeLabel", "reportType", "formType") or "")
        filer_type = normalize_whitespace(select_first(row, "filerType", "filer_type") or "")
        first_name = normalize_whitespace(select_first(row, "firstName", "first_name") or "")
        last_name = normalize_whitespace(select_first(row, "lastName", "last_name") or "")
        office = normalize_whitespace(select_first(row, "office", "officeDescription", "office_title") or "")
        state = normalize_whitespace(select_first(row, "state", "officeState", "state_dst") or "")

        pdf_url = select_first(row, "pdfUrl", "downloadPdfUrl", "summaryPdf")
        csv_href = select_first(
            row,
            "csvUrl",
            "downloadCsvUrl",
            "downloadCsv",
            "transactionsDownloadUrl",
        )

        if filed_at is None:
            raise ValueError(f"Unable to determine filing date for record: {row}")

        return cls(
            filing_id=filing_id,
            report_id=report_id,
            document_id=document_id,
            log_number=log_number,
            filed_at=filed_at,
            report_type=report_type,
            filer_type=filer_type,
            senator_first_name=first_name,
            senator_last_name=last_name,
            office=office or None,
            state=state or None,
            pdf_url=urljoin(BASE_URL, pdf_url) if pdf_url else None,
            csv_href=urljoin(BASE_URL, csv_href) if csv_href else None,
            raw=row,
        )

    def csv_request(self) -> Tuple[str, Dict[str, str]]:
        if self.csv_href:
            parsed = urlparse(self.csv_href)
            params = dict(parse_qsl(parsed.query))
            base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            return base, params

        if self.log_number and self.raw.get("fileName"):
            params = {
                "logno": str(self.log_number),
                "filename": str(self.raw["fileName"]),
                "download": "csv",
            }
            return DOWNLOAD_ENDPOINT, params

        if self.filing_id:
            params = {
                "filingId": str(self.filing_id),
                "fileType": "csv",
            }
            return CSV_FALLBACK_ENDPOINT, params

        raise ValueError("No CSV download information available for filing")


@dataclass
class SenateTransaction:
    event_uid: str
    filing_id: str
    report_id: str
    senator: str
    state: Optional[str]
    transaction_date: Optional[dt.date]
    notification_date: Optional[dt.date]
    asset_name: str
    ticker: str
    transaction_type: str
    owner: Optional[str]
    amount_text: str
    amount_low: Optional[int]
    amount_high: Optional[int]
    asset_type: Optional[str]
    comment: Optional[str]
    raw: Dict[str, str]

    def to_dict(self) -> Dict[str, object]:
        payload = dataclasses.asdict(self)
        for key in ("transaction_date", "notification_date"):
            value = payload.get(key)
            if isinstance(value, dt.date):
                payload[key] = value.isoformat()
        return payload

    @classmethod
    def from_csv_row(cls, filing: SenateFiling, row: Dict[str, str]) -> "SenateTransaction":
        tx_date = parse_date(select_first(row, "transaction_date", "Transaction Date"))
        notif_date = parse_date(select_first(row, "notification_date", "Notification Date"))
        asset_name = normalize_whitespace(
            select_first(row, "asset_name", "Asset Name", "Security") or ""
        )
        ticker = normalize_whitespace(select_first(row, "ticker", "Ticker") or "")
        transaction_type = normalize_whitespace(
            select_first(row, "type", "Type", "Transaction Type") or ""
        )
        owner = normalize_whitespace(select_first(row, "owner", "Owner") or "") or None
        amount_text = normalize_whitespace(select_first(row, "amount", "Amount", "Amount Range") or "")
        amount_low, amount_high = parse_amount_range(amount_text)
        asset_type = normalize_whitespace(select_first(row, "asset_type", "Asset Type") or "") or None
        comment = normalize_whitespace(select_first(row, "comment", "Comments") or "") or None

        fingerprint = "|".join(
            [
                filing.report_id or filing.filing_id,
                filing.senator_name,
                asset_name,
                ticker,
                transaction_type,
                amount_text,
                str(tx_date or ""),
                str(notif_date or ""),
            ]
        )
        event_uid = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()

        return cls(
            event_uid=event_uid,
            filing_id=filing.filing_id,
            report_id=filing.report_id,
            senator=filing.senator_name,
            state=filing.state,
            transaction_date=tx_date,
            notification_date=notif_date,
            asset_name=asset_name,
            ticker=ticker,
            transaction_type=transaction_type,
            owner=owner,
            amount_text=amount_text,
            amount_low=amount_low,
            amount_high=amount_high,
            asset_type=asset_type,
            comment=comment,
            raw=row,
        )


class SenateClient:
    def __init__(self, *, timeout: int = 30, rate_limit: float = 0.3) -> None:
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "application/json,text/csv,application/pdf,*/*;q=0.8",
                "Referer": "https://efdsearch.senate.gov/search/",
            }
        )

    def search_filings(
        self,
        *,
        start_date: dt.date,
        end_date: dt.date,
        report_types: Sequence[str] = ("P",),
        page_size: int = 100,
    ) -> Iterator[SenateFiling]:
        page = 1
        more = True
        while more:
            params = {
                "filerType": "O",
                "submittedStartDate": start_date.strftime("%m/%d/%Y"),
                "submittedEndDate": end_date.strftime("%m/%d/%Y"),
                "page": page,
                "itemsPerPage": page_size,
            }
            if report_types:
                params["reportType"] = ",".join(report_types)

            LOG.debug("Requesting filings page %s", page)
            response = self.session.get(SEARCH_ENDPOINT, params=params, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
            results = payload.get("results", [])

            for row in results:
                try:
                    yield SenateFiling.from_api(row)
                except Exception as exc:  # pragma: no cover - defensive
                    LOG.warning("Skipping filing due to parse error: %s", exc)

            total_pages = int(payload.get("pages") or payload.get("totalPages") or page)
            page += 1
            more = page <= total_pages
            if more:
                time.sleep(self.rate_limit)

    def download_transactions(self, filing: SenateFiling) -> List[SenateTransaction]:
        url, params = filing.csv_request()
        response = self.session.get(url, params=params or None, timeout=self.timeout)
        response.raise_for_status()
        content = response.text
        reader = csv.DictReader(io.StringIO(content))
        transactions: List[SenateTransaction] = []
        for row in reader:
            if not any(value.strip() for value in row.values() if value):
                continue
            transactions.append(SenateTransaction.from_csv_row(filing, row))
        return transactions


def load_date(value: str) -> dt.date:
    parsed = parse_date(value)
    if parsed is None:
        raise argparse.ArgumentTypeError(f"Invalid date value: {value}")
    return parsed


def run_ingest(start: dt.date, end: dt.date, output: Optional[str]) -> Dict[str, object]:
    client = SenateClient()
    filings = list(client.search_filings(start_date=start, end_date=end))
    LOG.info("Discovered %d filings between %s and %s", len(filings), start, end)

    all_transactions: List[SenateTransaction] = []
    for filing in filings:
        try:
            transactions = client.download_transactions(filing)
        except Exception as exc:
            LOG.warning("Failed to download transactions for %s (%s): %s", filing.senator_name, filing.filing_id, exc)
            continue

        all_transactions.extend(transactions)
        time.sleep(client.rate_limit)

    LOG.info("Collected %d transactions", len(all_transactions))

    result = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "filings": [dataclasses.asdict(f) for f in filings],
        "transactions": [tx.to_dict() for tx in all_transactions],
    }

    if output:
        with open(output, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, default=str)
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest Senate PTR filings")
    parser.add_argument("start", type=load_date, help="Start date (MM/DD/YYYY)")
    parser.add_argument("end", type=load_date, help="End date (MM/DD/YYYY)")
    parser.add_argument("--output", "-o", help="Path to write aggregated JSON payload")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default INFO)")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    try:
        run_ingest(args.start, args.end, args.output)
    except requests.HTTPError as exc:  # pragma: no cover - network failures
        LOG.error("HTTP error during ingest: %s", exc)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
