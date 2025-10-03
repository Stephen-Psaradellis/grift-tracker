#!/usr/bin/env python3
"""
Enhanced House Financial Disclosure Parser
Parses stock/ETF/crypto/option trades from House of Representatives disclosure documents
"""

import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import re
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Set
import time
from functools import lru_cache

INGESTION_ROOT = Path(__file__).resolve().parents[1]
if str(INGESTION_ROOT) not in sys.path:
    sys.path.insert(0, str(INGESTION_ROOT))

from parsing_utils import (  # noqa: E402
    normalize_text,
    parse_amount_range as _parse_amount_range,
    parse_date as _parse_date,
)

clean_text = normalize_text

# Optional heavy deps are imported lazily inside functions:
# pdfplumber, camelot, requests, pandas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('disclosure_parser.log'),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

# ============== Configuration ==============

class AssetType(Enum):
    """Asset type classification"""
    STOCK = "STOCK"
    ETF = "ETF"
    OPTION = "OPTION"
    CRYPTO = "CRYPTO"
    MUTUAL_FUND = "MUTUAL_FUND"
    BOND = "BOND"
    OTHER = "OTHER"

class Config:
    """Configuration constants"""
    # Amount buckets for categorizing trade sizes
    AMOUNT_BUCKETS = [
        (0, 1000, 0),
        (1000, 15000, 1),
        (15000, 50000, 2),
        (50000, 100000, 3),
        (100000, 250000, 4),
        (250000, 500000, 5),
        (500000, 1000000, 6),
        (1000000, 5000000, 7),
        (5000000, 10**12, 8),
    ]
    
    # Trade action keywords
    TRADE_ACTIONS = {
        "buy", "purchase", "acquire", "acquisition",
        "sell", "sale", "dispose", "disposition",
        "exchange", "exercise", "assignment", "expiration"
    }
    
    # Tokens to exclude (non-trade items)
    EXCLUDE_TOKENS = [
        "salary", "wages", "honoraria", "freelance", "consult", "consulting",
        "pension", "retirement", "social security", "ira", "401k", "403b",
        "mortgage", "loan", "credit", "debt", "liability",
        "student loan", "car loan", "auto loan", "personal loan",
        "revolving", "line of credit", "heloc",
        "patreon", "youtube", "tiktok", "instagram", "facebook",
        "teaching", "speaking", "book", "royalty", "royalties",
        "spouse salary", "dependent", "child",
        "brand", "marketing", "sponsorship", "endorsement",
        "bursar", "tuition", "education"
    ]
    
    # Known cryptocurrency symbols
    CRYPTO_SYMBOLS = {
        'BTC', 'ETH', 'USDT', 'BNB', 'XRP', 'USDC', 'SOL', 'ADA', 'AVAX',
        'DOGE', 'DOT', 'MATIC', 'SHIB', 'TRX', 'DAI', 'WBTC', 'LTC', 'BCH',
        'LINK', 'UNI', 'XLM', 'ALGO', 'ATOM', 'FIL', 'HBAR', 'MANA', 'SAND'
    }
    
    # Common ETF patterns
    ETF_PATTERNS = ['ETF', 'FUND', 'INDEX', 'TRUST', 'SPDR', 'ISHARES', 'VANGUARD']
    
    # Option keywords
    OPTION_KEYWORDS = ['call', 'put', 'option', 'strike', 'expiry', 'expire']
    
    # Date formats to try
    DATE_FORMATS = [
        "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%Y-%m-%d",
        "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d",
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"
    ]
    
    # PDF download settings
    MAX_RETRIES = 3
    TIMEOUT_SECONDS = 30
    MAX_WORKERS = 5  # For concurrent downloads

# ============== Data Classes ==============

@dataclass
class Filing:
    """Represents a financial disclosure filing"""
    first: str
    last: str
    filing_type: str
    state_dst: str
    year: int
    filing_date: dt.date
    doc_id: str
    
    @property
    def full_name(self) -> str:
        return f"{self.first} {self.last}".strip()
    
    @property
    def pdf_url(self) -> str:
        """Generate PDF URL based on filing type"""
        ptr_types = {"W", "C", "D"}
        folder = "ptr-pdfs" if self.filing_type.upper() in ptr_types else "financial-pdfs"
        return f"https://disclosures-clerk.house.gov/public_disc/{folder}/{self.year}/{self.doc_id}.pdf"
    
    @property
    def alternate_pdf_url(self) -> str:
        """Generate alternate PDF URL (fallback)"""
        ptr_types = {"W", "C", "D"}
        folder = "financial-pdfs" if self.filing_type.upper() in ptr_types else "ptr-pdfs"
        return f"https://disclosures-clerk.house.gov/public_disc/{folder}/{self.year}/{self.doc_id}.pdf"

@dataclass
class Trade:
    """Represents a parsed trade transaction"""
    event_uid: str
    filing_id: str
    actor: str
    date: dt.date
    action: str
    owner: str
    ticker: str
    company: str
    asset_type: AssetType
    amount_range: str
    amount_lo: int
    amount_hi: int
    amount_bucket: int
    cap_gains_over_200: bool = False
    description: str = ""
    raw_data: dict = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        d = asdict(self)
        d['date'] = self.date.isoformat()
        d['asset_type'] = self.asset_type.value
        if self.raw_data and not isinstance(self.raw_data, dict):
            d['raw_data'] = str(self.raw_data)
        return d

# ============== Parsing Functions ==============

def parse_date(s: str) -> Optional[dt.date]:
    """Parse date string with multiple format attempts"""
    return _parse_date(s, formats=Config.DATE_FORMATS, logger=logger)

def parse_financial_disclosure_xml(xml_path: str) -> List[Filing]:
    """Parse the Financial Disclosure XML file"""
    logger.info(f"Parsing XML file: {xml_path}")
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML: {e}")
        return []
    
    filings = []
    for member in root.findall(".//Member"):
        try:
            first = (member.findtext("First") or "").strip()
            last = (member.findtext("Last") or "").strip()
            filing_type = (member.findtext("FilingType") or "").strip()
            state_dst = (member.findtext("StateDst") or "").strip()
            year_txt = (member.findtext("Year") or "").strip()
            filing_date_txt = (member.findtext("FilingDate") or "").strip()
            doc_id = (member.findtext("DocID") or "").strip()
            
            if not all([doc_id, year_txt, filing_date_txt]):
                continue
            
            year = int(year_txt)
            filing_date = parse_date(filing_date_txt)
            if not filing_date:
                continue
            
            filings.append(Filing(first, last, filing_type, state_dst, year, filing_date, doc_id))
            
        except (ValueError, AttributeError) as e:
            logger.debug(f"Skipping malformed member entry: {e}")
            continue
    
    logger.info(f"Parsed {len(filings)} filings from XML")
    return filings

# ============== Enhanced Parsing Utilities ==============

# Regex patterns
ASSET_RE = re.compile(
    r"^(?P<name>.+?)\s*\((?P<ticker>[A-Z0-9.\-/]{1,10})\)\s*(?:\[(?P<type>[A-Z]{2,3})\])?$"
)
OPTION_RE = re.compile(
    r"(?P<underlying>.+?)\s+(?P<type>call|put)s?\s+(?:option)?\s*"
    r"(?:\$(?P<strike>[\d.]+))?\s*(?:exp|expire)?\s*(?P<expiry>[\d/\-]+)?",
    re.IGNORECASE
)

def is_amount_range(s: str) -> bool:
    """Check if string contains an amount range"""
    lo, hi = _parse_amount_range(s)
    return lo is not None and hi is not None and hi >= lo

def parse_amount_bucket(amount_str: str) -> Tuple[int, int, int]:
    """Parse amount range and return (low, high, bucket_score)"""
    lo, hi = _parse_amount_range(amount_str)
    if lo is None and hi is None:
        return (0, 0, 0)

    lo_val = lo or 0
    hi_val = hi if hi is not None else lo_val

    # Find appropriate bucket
    bucket_score = 0
    for lo_b, hi_b, score in Config.AMOUNT_BUCKETS:
        if lo_val >= lo_b and hi_val <= hi_b:
            bucket_score = score
            break

    return (lo_val, hi_val, bucket_score)

def parse_action(s: str) -> str:
    """Parse transaction action from string"""
    if not s:
        return ""
    
    s = clean_text(s).lower()
    
    # Handle single letter codes
    if s in ['p', 'b']:
        return "Buy"
    elif s in ['s', 's (partial)']:
        return "Sell"
    elif s == 'e':
        return "Exchange"
    
    # Check for action keywords
    for action in Config.TRADE_ACTIONS:
        if re.search(rf'\b{action}\b', s):
            return action.capitalize()
    
    return ""

@lru_cache(maxsize=1000)
def classify_asset_type(asset_str: str, ticker: str) -> AssetType:
    """Classify asset as STOCK, ETF, OPTION, CRYPTO, etc."""
    asset_lower = asset_str.lower() if asset_str else ""
    ticker_upper = ticker.upper() if ticker else ""
    
    # Check for options
    if any(kw in asset_lower for kw in Config.OPTION_KEYWORDS):
        return AssetType.OPTION
    if OPTION_RE.search(asset_str):
        return AssetType.OPTION
    
    # Check for crypto
    if ticker_upper in Config.CRYPTO_SYMBOLS:
        return AssetType.CRYPTO
    if any(kw in asset_lower for kw in ['crypto', 'bitcoin', 'ethereum', 'digital asset']):
        return AssetType.CRYPTO
    
    # Check for mutual funds
    if any(kw in asset_lower for kw in ['mutual fund', 'index fund']):
        return AssetType.MUTUAL_FUND
    
    # Check for bonds
    if any(kw in asset_lower for kw in ['bond', 'treasury', 'note', 'bill']):
        return AssetType.BOND
    
    # Check for ETFs
    if any(pattern.lower() in asset_lower for pattern in Config.ETF_PATTERNS):
        return AssetType.ETF
    if ticker_upper.endswith('F') and len(ticker_upper) == 4:  # Common ETF pattern
        return AssetType.ETF
    
    # Default to stock if has ticker, otherwise OTHER
    return AssetType.STOCK if ticker else AssetType.OTHER

def parse_asset(field: str) -> Tuple[str, str, Optional[dict]]:
    """Parse asset field to extract company, ticker, and option details"""
    if not field:
        return ("", "", None)
    
    field = clean_text(field)
    
    # Check for option pattern first
    option_match = OPTION_RE.search(field)
    if option_match:
        option_data = option_match.groupdict()
        underlying = option_data.get('underlying', '').strip()
        # Try to extract ticker from underlying
        asset_match = ASSET_RE.search(underlying)
        if asset_match:
            return (asset_match.group('name'), asset_match.group('ticker'), option_data)
        return (underlying, "", option_data)
    
    # Standard asset pattern
    asset_match = ASSET_RE.search(field)
    if asset_match:
        return (asset_match.group('name').strip(), asset_match.group('ticker').strip(), None)
    
    return (field, "", None)

def has_excluded_token(row_values: List[str]) -> bool:
    """Check if row contains excluded tokens (non-trade items)"""
    text = " ".join([v or "" for v in row_values]).lower()
    
    for token in Config.EXCLUDE_TOKENS:
        if " " in token:
            if token in text:
                return True
        else:
            if re.search(rf'\b{re.escape(token)}\b', text):
                return True
    
    return False

def row_uid(source: str, filing_id: str, line_no: int, ticker: str, 
           date_iso: str, amount_str: str, action: str) -> str:
    """Generate unique ID for a row"""
    key = f"{source}|{filing_id}|{line_no}|{ticker}|{date_iso}|{amount_str}|{action}".encode()
    return hashlib.sha256(key).hexdigest()[:16]  # Shorter hash for readability

# ============== PDF Processing ==============

class PDFProcessor:
    """Handles PDF download and parsing"""

    def __init__(self, cache_dir: Optional[Path] = None):
        """Create a processor for downloading and parsing PDFs.

        Parameters
        ----------
        cache_dir:
            Optional directory to persist downloaded PDFs. When ``None`` the
            processor keeps downloads purely in-memory so that no disclosure
            files are stored on disk – useful for ephemeral execution
            environments.
        """

        if cache_dir:
            cache_dir = Path(cache_dir)
            cache_dir.mkdir(exist_ok=True)
        self.cache_dir = cache_dir
        self._classification_cache = {}
    
    def download_pdf(self, url: str, doc_id: str, retry_count: int = 0) -> Optional[bytes]:
        """Download PDF with retry logic and caching"""
        # Check cache first
        if self.cache_dir:
            cache_file = self.cache_dir / f"{doc_id}.pdf"
            if cache_file.exists():
                logger.debug(f"Using cached PDF: {doc_id}")
                return cache_file.read_bytes()
        
        try:
            import requests
            
            logger.info(f"Downloading PDF: {url}")
            response = requests.get(url, timeout=Config.TIMEOUT_SECONDS)
            
            if response.status_code == 404 and retry_count == 0:
                # Try alternate URL
                alt_url = url.replace("ptr-pdfs", "financial-pdfs") if "ptr-pdfs" in url else url.replace("financial-pdfs", "ptr-pdfs")
                logger.info(f"Trying alternate URL: {alt_url}")
                return self.download_pdf(alt_url, doc_id, retry_count + 1)
            
            response.raise_for_status()
            pdf_bytes = response.content
            
            # Cache the PDF if persistent storage is enabled
            if self.cache_dir:
                cache_file.write_bytes(pdf_bytes)

            return pdf_bytes
            
        except Exception as e:
            if retry_count < Config.MAX_RETRIES:
                logger.warning(f"Download failed, retrying... ({e})")
                time.sleep(2 ** retry_count)  # Exponential backoff
                return self.download_pdf(url, doc_id, retry_count + 1)
            
            logger.error(f"Failed to download PDF after {Config.MAX_RETRIES} attempts: {e}")
            return None
    
    def classify_pdf(self, pdf_bytes: bytes, doc_id: str) -> str:
        """Classify PDF as PTR, FD, or UNKNOWN"""
        if doc_id in self._classification_cache:
            return self._classification_cache[doc_id]
        
        try:
            import pdfplumber
            
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                if not pdf.pages:
                    return "UNKNOWN"
                
                first_text = (pdf.pages[0].extract_text() or "").lower()
                
                if "periodic transaction report" in first_text:
                    classification = "PTR"
                elif "financial disclosure" in first_text:
                    classification = "FD"
                else:
                    classification = "UNKNOWN"
                
                self._classification_cache[doc_id] = classification
                return classification
                
        except Exception as e:
            logger.debug(f"PDF classification failed: {e}")
            return "UNKNOWN"
    
    def extract_tables_pdfplumber(self, pdf_bytes: bytes) -> List[dict]:
        """Extract tables using pdfplumber"""
        rows = []
        try:
            import pdfplumber
            
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    try:
                        tables = page.extract_tables() or []
                        for table in tables:
                            if not table or len(table) < 2:
                                continue
                            
                            # Extract headers
                            headers = [
                                (str(table[0][i] or f"col{i}")).strip().lower().replace('\n', ' ')
                                for i in range(len(table[0]))
                            ]
                            
                            # Extract rows
                            for row_data in table[1:]:
                                if len(row_data) != len(headers):
                                    continue
                                row = {
                                    headers[i]: clean_text(str(row_data[i] or ""))
                                    for i in range(len(headers))
                                }
                                rows.append(row)
                                
                    except Exception as e:
                        logger.debug(f"Error processing page {page_num}: {e}")
                        continue
                        
        except Exception as e:
            logger.debug(f"pdfplumber extraction failed: {e}")
        
        return rows
    
    def extract_tables_camelot(self, pdf_bytes: bytes) -> List[dict]:
        """Extract tables using camelot"""
        rows = []
        try:
            import camelot
            import tempfile
            
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                tmp.write(pdf_bytes)
                tmp.flush()
                
                # Try lattice mode first, then stream
                for flavor in ['lattice', 'stream']:
                    try:
                        tables = camelot.read_pdf(tmp.name, pages="all", flavor=flavor)
                        
                        for table in tables:
                            df = table.df
                            if df.empty or len(df) < 2:
                                continue
                            
                            headers = [str(h).strip().lower() for h in df.iloc[0].tolist()]
                            
                            for i in range(1, len(df)):
                                vals = df.iloc[i].tolist()
                                row = {
                                    headers[j]: clean_text(str(vals[j]))
                                    for j in range(min(len(headers), len(vals)))
                                }
                                rows.append(row)
                        
                        if rows:  # If we got results, don't try the other flavor
                            break
                            
                    except Exception as e:
                        logger.debug(f"Camelot {flavor} mode failed: {e}")
                        continue
                        
        except ImportError:
            logger.debug("Camelot not installed, skipping")
        except Exception as e:
            logger.debug(f"Camelot extraction failed: {e}")
        
        return rows
    
    def extract_transactions_region(self, pdf_bytes: bytes) -> List[dict]:
        """Extract only transaction tables from FD documents"""
        rows = []
        try:
            import pdfplumber
            
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = (page.extract_text() or "").lower()
                    
                    # Only process pages mentioning transactions
                    if "transaction" not in text:
                        continue
                    
                    tables = page.extract_tables() or []
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        
                        headers = [
                            (str(table[0][i] or f"col{i}")).strip().lower().replace('\n', ' ')
                            for i in range(len(table[0]))
                        ]
                        
                        for row_data in table[1:]:
                            if len(row_data) != len(headers):
                                continue
                            row = {
                                headers[i]: clean_text(str(row_data[i] or ""))
                                for i in range(len(headers))
                            }
                            rows.append(row)
                            
        except Exception as e:
            logger.debug(f"Transaction region extraction failed: {e}")
        
        return rows
    
    def parse_pdf_for_trades(self, pdf_bytes: bytes, filing_id: str, actor: str) -> List[Trade]:
        """Parse PDF to extract trade transactions"""
        pdf_type = self.classify_pdf(pdf_bytes, filing_id)
        
        # Get raw rows based on PDF type
        if pdf_type == "FD":
            raw_rows = self.extract_transactions_region(pdf_bytes)
            if not raw_rows:
                raw_rows = self.extract_tables_pdfplumber(pdf_bytes)
                if not raw_rows:
                    raw_rows = self.extract_tables_camelot(pdf_bytes)
        else:
            raw_rows = self.extract_tables_pdfplumber(pdf_bytes)
            if not raw_rows:
                raw_rows = self.extract_tables_camelot(pdf_bytes)
        
        trades = []
        for idx, row in enumerate(raw_rows):
            trade = self._parse_trade_row(row, filing_id, actor, idx)
            if trade:
                trades.append(trade)
        
        return trades
    
    def _parse_trade_row(self, row: dict, filing_id: str, actor: str, line_no: int) -> Optional[Trade]:
        """Parse a single row into a Trade object"""
        # Normalize keys
        normalized = {k.lower().strip(): v for k, v in row.items()}
        
        # Check for excluded tokens
        if has_excluded_token(list(normalized.values())):
            return None
        
        # Extract date
        date_str = (
            normalized.get("date") or 
            normalized.get("transaction date") or 
            normalized.get("tx date") or 
            normalized.get("trade date") or 
            normalized.get("transaction dt") or 
            ""
        )
        
        trade_date = parse_date(date_str)
        if not trade_date:
            return None
        
        # Extract action
        action_raw = normalized.get("transaction type") or normalized.get("type") or ""
        action = parse_action(action_raw)
        if not action:
            return None
        
        # Extract amount
        amount = (
            normalized.get("amount") or 
            normalized.get("amount range") or 
            normalized.get("value") or 
            normalized.get("value of asset") or 
            ""
        )
        
        if not is_amount_range(amount):
            return None
        
        lo, hi, bucket = parse_amount_bucket(amount)
        
        # Extract asset information
        asset_field = (
            normalized.get("asset") or 
            normalized.get("security") or 
            normalized.get("company") or 
            normalized.get("description") or 
            ""
        )
        
        company, ticker, option_data = parse_asset(asset_field)
        
        if not (ticker or company):
            return None
        
        # Extract owner
        owner = normalized.get("owner") or ""
        
        # Check for capital gains
        cap_gains = normalized.get("capital gains over $200") or ""
        cap_gains_over_200 = cap_gains.lower() in ['yes', 'y', 'true', '1']
        
        # Classify asset type
        asset_type = classify_asset_type(asset_field, ticker)
        
        # Generate unique ID
        uid = row_uid("house_ptr", filing_id, line_no, ticker or company, 
                     trade_date.isoformat(), amount, action)
        
        # Add description for options
        description = ""
        if option_data:
            description = f"{option_data.get('type', '').upper()} Option"
            if option_data.get('strike'):
                description += f" Strike: ${option_data['strike']}"
            if option_data.get('expiry'):
                description += f" Exp: {option_data['expiry']}"
        
        return Trade(
            event_uid=uid,
            filing_id=filing_id,
            actor=actor,
            date=trade_date,
            action=action,
            owner=owner,
            ticker=ticker,
            company=company,
            asset_type=asset_type,
            amount_range=amount,
            amount_lo=lo,
            amount_hi=hi,
            amount_bucket=bucket,
            cap_gains_over_200=cap_gains_over_200,
            description=description,
            raw_data=row
        )

# ============== Filtering Functions ==============

def filter_filings(
    filings: List[Filing],
    since: Optional[dt.date] = None,
    until: Optional[dt.date] = None,
    names: Optional[List[Tuple[str, str]]] = None,
    filing_types: Optional[List[str]] = None,
    states: Optional[List[str]] = None
) -> List[Filing]:
    """Filter filings based on criteria"""
    filtered = []
    
    for filing in filings:
        # Date filtering
        if since and filing.filing_date < since:
            continue
        if until and filing.filing_date > until:
            continue
        
        # Name filtering
        if names:
            name_match = False
            for last, first in names:
                if (filing.last.lower() == last.lower() and 
                    filing.first.lower() == first.lower()):
                    name_match = True
                    break
            if not name_match:
                continue
        
        # Filing type filtering
        if filing_types and filing.filing_type.upper() not in {t.upper() for t in filing_types}:
            continue
        
        # State filtering
        if states and filing.state_dst not in states:
            continue
        
        filtered.append(filing)
    
    return filtered

def validate_trade(trade: Trade) -> bool:
    """Validate trade data for quality"""
    # Check date is reasonable
    if trade.date > dt.date.today():
        logger.debug(f"Trade date in future: {trade.date}")
        return False
    
    if trade.date < dt.date(1990, 1, 1):
        logger.debug(f"Trade date too old: {trade.date}")
        return False
    
    # Check amounts are sensible
    if trade.amount_lo > trade.amount_hi:
        logger.debug(f"Invalid amount range: {trade.amount_lo} > {trade.amount_hi}")
        return False
    
    # Must have either ticker or company
    if not (trade.ticker or trade.company):
        logger.debug("Trade missing ticker and company")
        return False
    
    return True

# ============== Main Processing ==============

def process_filing_batch(
    filings: List[Filing], 
    processor: PDFProcessor,
    download_and_parse: bool = False,
    since_date: Optional[dt.date] = None
) -> List[Trade]:
    """Process a batch of filings"""
    all_trades = []
    
    if not download_and_parse:
        return all_trades
    
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        future_to_filing = {}
        
        for filing in filings:
            future = executor.submit(
                process_single_filing,
                filing,
                processor,
                since_date
            )
            future_to_filing[future] = filing
        
        for future in as_completed(future_to_filing):
            filing = future_to_filing[future]
            try:
                trades = future.result()
                all_trades.extend(trades)
                if trades:
                    logger.info(f"Extracted {len(trades)} trades from {filing.doc_id}")
            except Exception as e:
                logger.error(f"Failed to process filing {filing.doc_id}: {e}")
    
    return all_trades

def process_single_filing(
    filing: Filing,
    processor: PDFProcessor,
    since_date: Optional[dt.date] = None
) -> List[Trade]:
    """Process a single filing"""
    # Download PDF
    pdf_bytes = processor.download_pdf(filing.pdf_url, filing.doc_id)
    if not pdf_bytes:
        # Try alternate URL
        pdf_bytes = processor.download_pdf(filing.alternate_pdf_url, filing.doc_id)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF for {filing.doc_id}")
            return []
    
    # Parse trades from PDF
    trades = processor.parse_pdf_for_trades(pdf_bytes, filing.doc_id, filing.full_name)
    
    # Filter by date if specified
    if since_date:
        trades = [t for t in trades if t.date >= since_date]
    
    # Validate trades
    valid_trades = [t for t in trades if validate_trade(t)]
    
    if len(valid_trades) < len(trades):
        logger.debug(f"Filtered out {len(trades) - len(valid_trades)} invalid trades")
    
    return valid_trades

# ============== Export Functions ==============

def export_to_json(trades: List[Trade], output_path: str):
    """Export trades to JSON file"""
    data = [trade.to_dict() for trade in trades]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Exported {len(trades)} trades to {output_path}")

def export_to_csv(trades: List[Trade], output_path: str):
    """Export trades to CSV file"""
    try:
        import pandas as pd
        
        data = [trade.to_dict() for trade in trades]
        df = pd.DataFrame(data)
        
        # Reorder columns for better readability
        column_order = [
            'date', 'actor', 'action', 'ticker', 'company', 'asset_type',
            'amount_lo', 'amount_hi', 'amount_range', 'owner', 
            'cap_gains_over_200', 'filing_id', 'event_uid'
        ]
        
        # Only include columns that exist
        columns = [col for col in column_order if col in df.columns]
        df = df[columns]
        
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(trades)} trades to {output_path}")
        
    except ImportError:
        logger.error("pandas not installed, cannot export to CSV")
        logger.info("Install with: pip install pandas")

def export_to_parquet(trades: List[Trade], output_path: str):
    """Export trades to Parquet file for efficient storage"""
    try:
        import pandas as pd
        
        data = [trade.to_dict() for trade in trades]
        df = pd.DataFrame(data)
        
        # Convert date strings back to datetime for better Parquet compression
        df['date'] = pd.to_datetime(df['date'])
        
        df.to_parquet(output_path, index=False, compression='snappy')
        logger.info(f"Exported {len(trades)} trades to {output_path}")
        
    except ImportError:
        logger.error("pandas/pyarrow not installed, cannot export to Parquet")
        logger.info("Install with: pip install pandas pyarrow")

def generate_summary_statistics(trades: List[Trade]) -> dict:
    """Generate summary statistics from trades"""
    stats = {
        'total_trades': len(trades),
        'unique_actors': len(set(t.actor for t in trades)),
        'unique_tickers': len(set(t.ticker for t in trades if t.ticker)),
        'date_range': {
            'earliest': min(t.date for t in trades).isoformat() if trades else None,
            'latest': max(t.date for t in trades).isoformat() if trades else None
        },
        'by_action': {},
        'by_asset_type': {},
        'top_traded_tickers': [],
        'largest_trades': []
    }
    
    # Count by action
    from collections import Counter
    action_counts = Counter(t.action for t in trades)
    stats['by_action'] = dict(action_counts)
    
    # Count by asset type
    asset_type_counts = Counter(t.asset_type.value for t in trades)
    stats['by_asset_type'] = dict(asset_type_counts)
    
    # Top traded tickers
    ticker_counts = Counter(t.ticker for t in trades if t.ticker)
    stats['top_traded_tickers'] = [
        {'ticker': ticker, 'count': count}
        for ticker, count in ticker_counts.most_common(10)
    ]
    
    # Largest trades by upper amount
    largest = sorted(trades, key=lambda t: t.amount_hi, reverse=True)[:10]
    stats['largest_trades'] = [
        {
            'actor': t.actor,
            'ticker': t.ticker or t.company,
            'action': t.action,
            'amount_range': t.amount_range,
            'date': t.date.isoformat()
        }
        for t in largest
    ]
    
    return stats

# ============== Main Function ==============

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Enhanced House Financial Disclosure Parser - Extract trades from congressional disclosures",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse XML only (no PDF download)
  python %(prog)s --xml 2025FD.xml --out-json filings.json
  
  # Download and parse PDFs for specific members
  python %(prog)s --xml 2025FD.xml --names "Pelosi,Nancy;McCarthy,Kevin" --download-and-parse --out-json trades.json
  
  # Filter by date and filing type
  python %(prog)s --xml 2025FD.xml --since 2025-01-01 --filing-types W,C,D --download-and-parse --out-csv trades.csv
  
  # Generate summary statistics
  python %(prog)s --xml 2025FD.xml --download-and-parse --summary
        """
    )
    
    # Required arguments
    parser.add_argument('--xml', required=True, help='Path to FinancialDisclosure XML file (e.g., 2025FD.xml)')
    
    # Filtering options
    parser.add_argument('--since', help='YYYY-MM-DD; only filings on/after this date')
    parser.add_argument('--until', help='YYYY-MM-DD; only filings on/before this date')
    parser.add_argument('--names', help='Semicolon-separated "Last,First" list (e.g., "Smith,John;Doe,Jane")')
    parser.add_argument('--filing-types', help='Comma-separated filing type codes (e.g., W,C,D)')
    parser.add_argument('--states', help='Comma-separated state codes (e.g., CA,TX,NY)')
    
    # Processing options
    parser.add_argument('--download-and-parse', action='store_true', 
                       help='Download PDFs and extract trades (otherwise just parse XML metadata)')
    parser.add_argument('--cache-dir', help='Directory for caching PDFs (omit to keep downloads in-memory)')
    parser.add_argument('--max-workers', type=int, default=5, 
                       help='Maximum concurrent downloads (default: 5)')
    
    # Output options
    parser.add_argument('--out-json', help='Output JSON file path')
    parser.add_argument('--out-csv', help='Output CSV file path')
    parser.add_argument('--out-parquet', help='Output Parquet file path')
    parser.add_argument('--summary', action='store_true', help='Print summary statistics')
    
    # Logging options
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    parser.add_argument('--log-file', help='Log file path (in addition to stderr)')
    
    args = parser.parse_args()
    
    # Configure logging
    logger.setLevel(getattr(logging, args.log_level))
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)
    
    # Parse dates
    since_date = None
    until_date = None
    if args.since:
        since_date = dt.date.fromisoformat(args.since)
    if args.until:
        until_date = dt.date.fromisoformat(args.until)
    
    # Parse names
    names_list = []
    if args.names:
        for part in args.names.split(';'):
            if not part.strip():
                continue
            if ',' in part:
                last, first = part.split(',', 1)
            else:
                last, first = part.strip(), ""
            names_list.append((last.strip(), first.strip()))
    
    # Parse other filters
    filing_types = [t.strip() for t in args.filing_types.split(',')] if args.filing_types else None
    states = [s.strip().upper() for s in args.states.split(',')] if args.states else None
    
    # Update config
    if args.max_workers:
        Config.MAX_WORKERS = args.max_workers
    
    # Parse XML
    filings = parse_financial_disclosure_xml(args.xml)
    if not filings:
        logger.error("No filings found in XML file")
        return 1
    
    # Filter filings
    filtered = filter_filings(
        filings,
        since=since_date,
        until=until_date,
        names=names_list,
        filing_types=filing_types,
        states=states
    )
    
    logger.info(f"Filtered to {len(filtered)} filings from {len(filings)} total")
    
    if not args.download_and_parse:
        # Just output filing metadata
        result = []
        for f in filtered:
            result.append({
                'first': f.first,
                'last': f.last,
                'filing_type': f.filing_type,
                'state_dst': f.state_dst,
                'year': f.year,
                'filing_date': f.filing_date.isoformat(),
                'doc_id': f.doc_id,
                'pdf_url': f.pdf_url
            })
        
        if args.out_json:
            with open(args.out_json, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            logger.info(f"Saved filing metadata to {args.out_json}")
        else:
            print(json.dumps(result, indent=2))
        
        return 0
    
    # Process PDFs to extract trades
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    processor = PDFProcessor(cache_dir)
    
    logger.info("Starting PDF processing...")
    trades = process_filing_batch(
        filtered,
        processor,
        download_and_parse=True,
        since_date=since_date
    )
    
    logger.info(f"Extracted {len(trades)} total trades")
    
    # Export results
    if args.out_json:
        export_to_json(trades, args.out_json)
    
    if args.out_csv:
        export_to_csv(trades, args.out_csv)
    
    if args.out_parquet:
        export_to_parquet(trades, args.out_parquet)
    
    # Print summary if requested
    if args.summary:
        stats = generate_summary_statistics(trades)
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)
        print(json.dumps(stats, indent=2))
    
    # If no output file specified, print to stdout
    if not any([args.out_json, args.out_csv, args.out_parquet, args.summary]):
        data = [trade.to_dict() for trade in trades]
        print(json.dumps(data, indent=2))
    
    return 0

if __name__ == "__main__":
    sys.exit(main())