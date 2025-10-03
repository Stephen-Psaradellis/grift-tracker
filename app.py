import datetime as dt
import io
import logging
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import requests
from flask import Flask, jsonify, request
from supabase import Client, create_client

from ingestion.house import ingest as house_ingest
from ingestion.normalization import (
    collect_company_records,
    transaction_event_from_house_trade,
)
from ingestion.db import upsert_companies, upsert_transaction_events

logger = logging.getLogger(__name__)
DEFAULT_XML_BUNDLE_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025FD.zip"
DEFAULT_FILING_TYPES = ["P"]


def create_app() -> Flask:
    """Application factory used by Gunicorn and Flask CLI."""
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    app = Flask(__name__)
    supabase_client = _init_supabase_client()

    @app.get("/health")
    def health_check():
        return jsonify({"status": "ok"})

    @app.post("/ingest")
    def trigger_ingestion():
        payload = request.get_json(silent=True) or {}
        params = {**request.args, **payload}

        xml_url = params.get("xml_url", DEFAULT_XML_BUNDLE_URL)
        try:
            since_date = _parse_date(params.get("since"))
            until_date = _parse_date(params.get("until"))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        filing_types = _parse_list(params.get("filing_types"), DEFAULT_FILING_TYPES)
        states = _parse_list(params.get("states"))
        names = _parse_names(params.get("names"))

        if supabase_client is None:
            return (
                jsonify({"error": "Supabase environment variables are not configured"}),
                500,
            )

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir)
                xml_path = _fetch_xml_bundle(xml_url, tmp_path)
                trades = _run_ingestion(
                    xml_path,
                    since=since_date,
                    until=until_date,
                    names=names,
                    filing_types=filing_types,
                    states=states,
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to ingest disclosures: %s", exc)
            return jsonify({"error": str(exc)}), 500

        inserted = 0
        if trades:
            try:
                inserted = _upsert_trades(supabase_client, trades)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("Failed to write trades to Supabase: %s", exc)
                return jsonify({"error": str(exc), "trades_found": len(trades)}), 500

        return jsonify({"trades_found": len(trades), "inserted": inserted})

    return app


def _init_supabase_client() -> Optional[Client]:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_ANON_KEY")

    if not (url and key):
        logger.warning("Supabase credentials are not fully configured")
        return None

    return create_client(url, key)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError as exc:  # pragma: no cover - simple validation
        raise ValueError(
            f"Invalid date format for '{value}'. Expected YYYY-MM-DD."
        ) from exc


def _parse_list(value: Optional[str], default: Optional[Sequence[str]] = None) -> Optional[List[str]]:
    if value is None:
        if default is None:
            return None
        return list(default)
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    items = [item.strip() for item in str(value).split(";")]
    cleaned = [item for item in items if item]
    return cleaned or (list(default) if default else None)


def _parse_names(value: Optional[str]) -> Optional[List[Tuple[str, str]]]:
    if not value:
        return None
    if isinstance(value, list):
        raw = value
    else:
        raw = value.split(";")
    parsed: List[Tuple[str, str]] = []
    for item in raw:
        if not item:
            continue
        parts = [part.strip() for part in str(item).split(",") if part.strip()]
        if len(parts) == 2:
            parsed.append((parts[0], parts[1]))
    return parsed or None


def _fetch_xml_bundle(url: str, destination: Path) -> Path:
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    content = response.content
    if url.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            archive.extractall(destination)
    else:
        xml_target = destination / Path(url).name
        xml_target.write_bytes(content)

    xml_files = list(destination.glob("*.xml"))
    if not xml_files:
        raise FileNotFoundError("No XML files were found in the downloaded bundle")

    return xml_files[0]


def _run_ingestion(
    xml_path: Path,
    *,
    since: Optional[dt.date],
    until: Optional[dt.date],
    names: Optional[List[Tuple[str, str]]],
    filing_types: Optional[List[str]],
    states: Optional[List[str]],
) -> List[house_ingest.Trade]:
    filings = house_ingest.parse_financial_disclosure_xml(str(xml_path))
    if not filings:
        return []

    filtered = house_ingest.filter_filings(
        filings,
        since=since,
        until=until,
        names=names,
        filing_types=filing_types,
        states=states,
    )

    if not filtered:
        return []

    processor = house_ingest.PDFProcessor(cache_dir=None)
    trades = house_ingest.process_filing_batch(
        filtered,
        processor,
        download_and_parse=True,
        since_date=since,
    )
    return trades


def _upsert_trades(client: Client, trades: Iterable[house_ingest.Trade]) -> int:
    events = [transaction_event_from_house_trade(trade) for trade in trades]
    if not events:
        return 0

    inserted = upsert_transaction_events(client, events)
    company_rows = collect_company_records(events)
    if company_rows:
        upsert_companies(client, company_rows)

    return inserted


app = create_app()
