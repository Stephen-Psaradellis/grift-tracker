"""Shared parsing helpers for House and Senate ingestion scripts."""

from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Optional, Sequence, Tuple

__all__ = [
    "normalize_text",
    "parse_date",
    "parse_amount_range",
]

# Common whitespace and punctuation normalisation
_WHITESPACE_RE = re.compile(r"\s+")
_TEXT_REPLACEMENTS = {
    "\u00a0": " ",  # non-breaking space
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
}

# Date parsing helpers
_DEFAULT_DATE_FORMATS: Tuple[str, ...] = (
    "%m/%d/%Y",
    "%m/%d/%y",
    "%m-%d-%Y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
)
_MONTH_NAME_FORMATS: Tuple[str, ...] = ("%B %d, %Y", "%b %d, %Y")
_DATE_FALLBACK_RE = re.compile(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})")

# Amount parsing helpers
_AMOUNT_RANGE_RE = re.compile(
    r"\$?\s*([\d,]+(?:\.\d+)?)\s*(?:[-\u2013\u2014]|to)\s*\$?\s*([\d,]+(?:\.\d+)?)",
    re.IGNORECASE,
)
_AMOUNT_OVER_RE = re.compile(r"over\s+\$?([\d,]+(?:\.\d+)?)", re.IGNORECASE)
_AMOUNT_UNDER_RE = re.compile(r"(?:less than|under)\s+\$?([\d,]+(?:\.\d+)?)", re.IGNORECASE)
_AMOUNT_SINGLE_RE = re.compile(r"^\$?\s*([\d,]+(?:\.\d+)?)\s*$")


def normalize_text(value: Optional[str]) -> str:
    """Return a consistently formatted text string."""
    if value is None:
        return ""

    text = str(value)
    for original, replacement in _TEXT_REPLACEMENTS.items():
        text = text.replace(original, replacement)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _coerce_int(value: str) -> Optional[int]:
    try:
        return int(float(value.replace(",", "")))
    except ValueError:
        return None


def parse_amount_range(amount_text: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """Parse an amount range like "$1,001 - $15,000" into integer bounds."""
    if not amount_text:
        return None, None

    text = normalize_text(amount_text)
    if not text:
        return None, None

    match = _AMOUNT_RANGE_RE.search(text)
    if match:
        low = _coerce_int(match.group(1))
        high = _coerce_int(match.group(2))
        if low is not None and high is not None and low > high:
            low, high = high, low
        return low, high

    match = _AMOUNT_UNDER_RE.search(text)
    if match:
        high = _coerce_int(match.group(1))
        if high is None:
            return None, None
        return 0, high

    match = _AMOUNT_OVER_RE.search(text)
    if match:
        low = _coerce_int(match.group(1))
        return low, None

    match = _AMOUNT_SINGLE_RE.match(text)
    if match:
        value = _coerce_int(match.group(1))
        return value, value

    return None, None


def parse_date(
    value: Optional[str],
    *,
    formats: Sequence[str] = _DEFAULT_DATE_FORMATS,
    logger: Optional[logging.Logger] = None,
) -> Optional[dt.date]:
    """Parse a disclosure date string into a ``datetime.date``."""
    if not value:
        return None

    text = normalize_text(value)
    if not text:
        return None

    for fmt in formats:
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    for fmt in _MONTH_NAME_FORMATS:
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    match = _DATE_FALLBACK_RE.search(text)
    if match:
        month, day, year = match.groups()
        year_int = int(year)
        if year_int < 100:
            year_int = 2000 + year_int if year_int < 50 else 1900 + year_int
        try:
            return dt.date(year_int, int(month), int(day))
        except ValueError:
            pass

    if logger is not None:
        logger.debug("Unable to parse date value '%s'", value)
    return None
