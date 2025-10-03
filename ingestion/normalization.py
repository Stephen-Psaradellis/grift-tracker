"""Helpers for normalizing trade data into database payloads."""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, TYPE_CHECKING

from .parsing_utils import normalize_text

__all__ = [
    "TransactionEvent",
    "CompanyRecord",
    "transaction_event_from_house_trade",
    "transaction_event_from_senate_transaction",
    "collect_company_records",
    "canonicalize_transaction_type",
]


# Deterministic namespace so we can generate repeatable UUIDs for events.
_EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://grift-tracker/transaction-event")

# Keywords that map transaction actions to the canonical buckets stored in
# the ``transaction_event`` table.
_BUY_KEYWORDS = {
    "buy",
    "purchase",
    "acquire",
    "acquisition",
    "bought",
}
_SELL_KEYWORDS = {
    "sell",
    "sale",
    "dispose",
    "disposition",
    "sold",
}


def canonicalize_transaction_type(raw_value: Optional[str]) -> str:
    """Map a free-form transaction action to ``buy``, ``sell`` or ``other``.

    The Senate CSV exports contain values like ``Purchase`` or ``Sale (Partial)``
    while the House parser may emit ``Buy`` or ``Sell`` tokens.  The database
    schema enforces a small enumeration, so we normalise everything into the
    required buckets.
    """

    if not raw_value:
        return "other"

    text = normalize_text(raw_value).lower()
    if not text:
        return "other"

    for keyword in _BUY_KEYWORDS:
        if keyword in text:
            return "buy"
    for keyword in _SELL_KEYWORDS:
        if keyword in text:
            return "sell"
    return "other"


@dataclass(frozen=True)
class TransactionEvent:
    """Payload that matches the ``transaction_event`` database schema."""

    id: uuid.UUID
    filing_id: Optional[str]
    transaction_date: Optional[dt.date]
    ticker: Optional[str]
    company_name: Optional[str]
    transaction_type: str
    amount_range: Optional[str]
    amount_lo: Optional[int]
    amount_hi: Optional[int]
    owner: Optional[str]
    politician_id: Optional[str] = None

    def to_record(self) -> Dict[str, object]:
        """Serialise into a dictionary that can be upserted into Supabase."""

        return {
            "id": str(self.id),
            "politician_id": self.politician_id,
            "filing_id": self.filing_id,
            "transaction_date": self.transaction_date.isoformat()
            if isinstance(self.transaction_date, dt.date)
            else None,
            "ticker": self.ticker,
            "company_name": self.company_name,
            "transaction_type": self.transaction_type,
            "amount_range": self.amount_range,
            "amount_lo": self.amount_lo,
            "amount_hi": self.amount_hi,
            "owner": self.owner,
        }


@dataclass(frozen=True)
class CompanyRecord:
    """Representation of a company row that can be merged into ``company``."""

    ticker: str
    name: Optional[str]
    sector: Optional[str] = None
    industry: Optional[str] = None

    def to_record(self) -> Dict[str, object]:
        return {
            "ticker": self.ticker,
            "name": self.name,
            "sector": self.sector,
            "industry": self.industry,
        }


def _blank_to_none(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = normalize_text(value)
    return text or None


def _normalize_ticker(value: Optional[str]) -> Optional[str]:
    text = _blank_to_none(value)
    if not text:
        return None
    # Tickers are stored in uppercase; strip common punctuation artefacts.
    cleaned = "".join(ch for ch in text if ch.isalnum() or ch in {".", "-"})
    cleaned = cleaned.upper()
    return cleaned or None


def _deterministic_uuid(source: str, fingerprint: str) -> uuid.UUID:
    token = f"{source}:{fingerprint}".strip()
    return uuid.uuid5(_EVENT_NAMESPACE, token)


def transaction_event_from_house_trade(
    trade: "HouseTrade", politician_id: Optional[str] = None
) -> TransactionEvent:
    """Build a normalised transaction event from a House ``Trade`` instance."""

    ticker = _normalize_ticker(getattr(trade, "ticker", None))
    company = _blank_to_none(getattr(trade, "company", None))
    owner = _blank_to_none(getattr(trade, "owner", None))
    amount_range = _blank_to_none(getattr(trade, "amount_range", None))
    fingerprint = getattr(trade, "event_uid", "house")
    event_uuid = _deterministic_uuid("house", fingerprint)

    return TransactionEvent(
        id=event_uuid,
        filing_id=getattr(trade, "filing_id", None),
        transaction_date=getattr(trade, "date", None),
        ticker=ticker,
        company_name=company,
        transaction_type=canonicalize_transaction_type(getattr(trade, "action", None)),
        amount_range=amount_range,
        amount_lo=getattr(trade, "amount_lo", None),
        amount_hi=getattr(trade, "amount_hi", None),
        owner=owner,
        politician_id=politician_id,
    )


def transaction_event_from_senate_transaction(
    transaction: "SenateTransaction", politician_id: Optional[str] = None
) -> TransactionEvent:
    """Convert a Senate CSV row into a ``TransactionEvent`` payload."""

    ticker = _normalize_ticker(getattr(transaction, "ticker", None))
    company = _blank_to_none(getattr(transaction, "asset_name", None))
    owner = _blank_to_none(getattr(transaction, "owner", None))
    amount_range = _blank_to_none(getattr(transaction, "amount_text", None))
    fingerprint = getattr(transaction, "event_uid", "senate")
    event_uuid = _deterministic_uuid("senate", fingerprint)
    filing_id = getattr(transaction, "filing_id", None) or getattr(
        transaction, "report_id", None
    )
    transaction_date = getattr(transaction, "transaction_date", None) or getattr(
        transaction, "notification_date", None
    )

    return TransactionEvent(
        id=event_uuid,
        filing_id=filing_id,
        transaction_date=transaction_date,
        ticker=ticker,
        company_name=company,
        transaction_type=canonicalize_transaction_type(
            getattr(transaction, "transaction_type", None)
        ),
        amount_range=amount_range,
        amount_lo=getattr(transaction, "amount_low", None),
        amount_hi=getattr(transaction, "amount_high", None),
        owner=owner,
        politician_id=politician_id,
    )


def collect_company_records(events: Sequence[TransactionEvent]) -> List[CompanyRecord]:
    """Build a unique list of company rows from a set of events.

    We only return companies that include a ticker symbol because the ``company``
    table enforces a unique constraint on that column.
    """

    companies: Dict[str, CompanyRecord] = {}
    for event in events:
        if not event.ticker:
            continue

        existing = companies.get(event.ticker)
        name = event.company_name
        if existing is None:
            companies[event.ticker] = CompanyRecord(
                ticker=event.ticker,
                name=name,
                sector=None,
                industry=None,
            )
        elif not existing.name and name:
            companies[event.ticker] = CompanyRecord(
                ticker=event.ticker,
                name=name,
                sector=existing.sector,
                industry=existing.industry,
            )

    return list(companies.values())


# The imports below are only needed for type checking and avoid circular
# dependencies at runtime.
if TYPE_CHECKING:  # pragma: no cover - typing only
    from ingestion.house.ingest import Trade as HouseTrade
    from ingestion.senate.ingest import SenateTransaction
else:
    # Lightweight placeholders so we can annotate helper signatures without
    # importing the heavy ingestion modules at runtime.
    class HouseTrade:  # pragma: no cover - placeholder for static analysis
        pass

    class SenateTransaction:  # pragma: no cover - placeholder for static analysis
        pass
