"""Supabase helpers for writing transaction and company data."""

from __future__ import annotations

import os
from typing import Iterable, List, Sequence

from supabase import Client

from .normalization import CompanyRecord, TransactionEvent

_DEFAULT_TRANSACTION_TABLE = os.environ.get(
    "SUPABASE_TRANSACTION_TABLE", "transaction_event"
)
_DEFAULT_COMPANY_TABLE = os.environ.get("SUPABASE_COMPANY_TABLE", "company")


def _chunk(sequence: Sequence[dict], size: int) -> Iterable[Sequence[dict]]:
    for index in range(0, len(sequence), size):
        yield sequence[index : index + size]


def upsert_transaction_events(
    client: Client,
    events: Sequence[TransactionEvent],
    *,
    table_name: str = _DEFAULT_TRANSACTION_TABLE,
    chunk_size: int = 100,
) -> int:
    """Upsert normalised events into the ``transaction_event`` table."""

    if not events:
        return 0

    payloads: List[dict] = [event.to_record() for event in events]
    inserted = 0

    for chunk in _chunk(payloads, chunk_size):
        response = client.table(table_name).upsert(chunk, on_conflict="id").execute()
        data = getattr(response, "data", None)
        inserted += len(data) if data is not None else len(chunk)

    return inserted


def upsert_companies(
    client: Client,
    companies: Sequence[CompanyRecord],
    *,
    table_name: str = _DEFAULT_COMPANY_TABLE,
    chunk_size: int = 100,
) -> int:
    """Upsert company reference rows keyed by ticker."""

    filtered = [company for company in companies if company.ticker]
    if not filtered:
        return 0

    payloads: List[dict] = [company.to_record() for company in filtered]
    inserted = 0

    for chunk in _chunk(payloads, chunk_size):
        response = client.table(table_name).upsert(chunk, on_conflict="ticker").execute()
        data = getattr(response, "data", None)
        inserted += len(data) if data is not None else len(chunk)

    return inserted
