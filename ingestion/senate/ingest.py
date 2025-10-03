#!/usr/bin/env python3
"""
Senate Financial Disclosure Parser
Scrapes and parses stock/ETF/crypto/option trades from Senate eFD system
Located at: senate/ingest.py
"""

import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Set, Any
from urllib.parse import urljoin, urlparse, parse_qs
import warnings
warnings.filterwarnings('ignore')

# Optional dependencies imported lazily:
# requests, beautifulsoup4, pdfplumber, camelot-py, selenium

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('senate_disclosure_parser.log'),
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
    """Configuration constants for Senate parsing"""
    # Senate eFD base URL
    BASE_URL = "https://efdsearch.senate.gov/search/"
    SEARCH_URL = "https://efdsearch.senate.gov/search/home/"
    REPORT_BASE = "https://efdsearch.senate.gov/search/view/paper/"
    
    # Search parameters
    DEFAULT_START_DATE = "01/01/2012"  # Senate eFD goes back to 2012
    
    # Report types in Senate system
    REPORT_TYPES = {
        "annual": "Annual Report",
        "ptr": "Periodic Transaction Report",
        "termination": "Termination Report",
        "new_filer": "New Filer Report",
        "amendment": "Amendment",
        "blind_trust": "Blind Trust Report"
    }
    
    # Amount ranges used in Senate reports (different from House)
    SENATE_AMOUNT_BUCKETS = [
        (0, 1000, 0),
        (1001, 15000, 1),
        (15001, 50000, 2),
        (50001, 100000, 3),
        (100001, 250000, 4),
        (250001, 500000, 5),
        (500001, 1000000, 6),
        (1000001, 5000000, 7),
        (5000001, 25000000, 8),
        (25000001, 50000000, 9),
        (50000001, 10**12, 10),
    ]
    
    # Trade action keywords
    TRADE_ACTIONS = {
        "purchase", "buy", "bought", "acquired", "exercised",
        "sale", "sell", "sold", "disposed", "exchanged"
    }
    
    # Exclude tokens (income/liability items)
    EXCLUDE_TOKENS = [
        "salary", "honoraria", "consulting fee", "director fee",
        "pension", "social security", "retirement", "ira", "401k",
        "mortgage", "loan", "credit", "liability", "debt",
        "rent", "rental income", "royalty", "book advance",
        "speaking fee", "teaching", "spouse salary"
    ]
    
    # Cryptocurrency symbols
    CRYPTO_SYMBOLS = {
        'BTC', 'ETH', 'USDT', 'BNB', 'XRP', 'USDC', 'SOL', 'ADA',
        'DOGE', 'DOT', 'MATIC', 'SHIB', 'LTC', 'BCH', 'LINK', 'UNI'
    }
    
    # Request settings
    REQUEST_TIMEOUT = 30
    RETRY_ATTEMPTS = 3
    RATE_LIMIT_DELAY = 1  # Seconds between requests
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# ============== Data Classes ==============

@dataclass
class SenateFiling:
    """Represents a Senate financial disclosure filing"""
    senator_name: str
    state: str
    report_type: str
    filing_date: dt.date
    report_id: str  # Senate uses different ID format
    pdf_url: str
    
    @property
    def full_name(self) -> str:
        return self.senator_name
    
    @property
    def is_ptr(self) -> bool:
        return "periodic" in self.report_type.lower() or "ptr" in self.report_type.lower()

@dataclass
class SenateTrade:
    """Represents a parsed Senate trade transaction"""
    event_uid: str
    report_id: str
    senator: str
    state: str
    date: dt.date
    action: str
    owner: str
    asset: str
    ticker: str
    asset_type: AssetType
    amount_range: str
    amount_lo: int
    amount_hi: int
    notification_date: Optional[dt.date] = None
    comment: str = ""
    raw_data: dict = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        d = asdict(self)
        d['date'] = self.date.isoformat()
        d['asset_type'] = self.asset_type.value
        if self.notification_date:
            d['notification_date'] = self.notification_date.isoformat()
        if self.raw_data and not isinstance(self.raw_data, dict):
            d['raw_data'] = str(self.raw_data)
        return d

# ============== Web Scraping Functions ==============

class SenateEFDScraper:
    """Scrapes the Senate eFD system for filing metadata"""
    
    def __init__(self, use_selenium: bool = False):
        self.use_selenium = use_selenium
        self.session = None
        self.driver = None
        
    def __enter__(self):
        if self.use_selenium:
            self._init_selenium()
        else:
            self._init_requests()
        return self
        
    def __exit__(self, *args):
        if self.driver:
            self.driver.quit()
        if self.session:
            self.session.close()
    
    def _init_requests(self):
        """Initialize requests session"""
        try:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': Config.USER_AGENT,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
        except ImportError:
            logger.error("requests not installed. Install with: pip install requests")
            raise
    
    def _init_selenium(self):
        """Initialize Selenium for JavaScript-heavy pages"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument(f'user-agent={Config.USER_AGENT}')
            
            self.driver = webdriver.Chrome(options=options)
            logger.info("Initialized Selenium WebDriver")
            
        except ImportError:
            logger.error("selenium not installed. Install with: pip install selenium")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Selenium: {e}")
            logger.info("Falling back to requests-based scraping")
            self.use_selenium = False
            self._init_requests()
    
    def search_filings(
        self,
        senator_name: Optional[str] = None,
        state: Optional[str] = None,
        report_type: str = "ptr",
        start_date: Optional[dt.date] = None,
        end_date: Optional[dt.date] = None,
        max_results: int = 1000
    ) -> List[SenateFiling]:
        """Search Senate eFD system for filings"""
        
        if self.use_selenium:
            return self._search_with_selenium(
                senator_name, state, report_type, start_date, end_date, max_results
            )
        else:
            return self._search_with_requests(
                senator_name, state, report_type, start_date, end_date, max_results
            )
    
    def _search_with_requests(
        self,
        senator_name: Optional[str],
        state: Optional[str],
        report_type: str,
        start_date: Optional[dt.date],
        end_date: Optional[dt.date],
        max_results: int
    ) -> List[SenateFiling]:
        """Search using requests and BeautifulSoup"""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 not installed. Install with: pip install beautifulsoup4")
            raise
        
        filings = []
        
        # Build search parameters
        search_params = self._build_search_params(
            senator_name, state, report_type, start_date, end_date
        )
        
        try:
            # Perform search
            logger.info(f"Searching Senate eFD with params: {search_params}")
            
            # First, get the search page to obtain any necessary cookies/tokens
            home_response = self.session.get(Config.SEARCH_URL, timeout=Config.REQUEST_TIMEOUT)
            home_response.raise_for_status()
            
            # Parse for any CSRF tokens if needed
            soup = BeautifulSoup(home_response.text, 'html.parser')
            csrf_token = self._extract_csrf_token(soup)
            if csrf_token:
                search_params['csrfmiddlewaretoken'] = csrf_token
            
            # Submit search
            time.sleep(Config.RATE_LIMIT_DELAY)
            search_response = self.session.post(
                Config.SEARCH_URL,
                data=search_params,
                timeout=Config.REQUEST_TIMEOUT
            )
            search_response.raise_for_status()
            
            # Parse results
            results_soup = BeautifulSoup(search_response.text, 'html.parser')
            filings = self._parse_search_results(results_soup)
            
            # Handle pagination
            page = 2
            while len(filings) < max_results:
                next_page_url = self._get_next_page_url(results_soup)
                if not next_page_url:
                    break
                
                time.sleep(Config.RATE_LIMIT_DELAY)
                page_response = self.session.get(next_page_url, timeout=Config.REQUEST_TIMEOUT)
                page_response.raise_for_status()
                
                results_soup = BeautifulSoup(page_response.text, 'html.parser')
                page_filings = self._parse_search_results(results_soup)
                
                if not page_filings:
                    break
                
                filings.extend(page_filings)
                page += 1
                
                logger.info(f"Retrieved page {page}, total filings: {len(filings)}")
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
        
        logger.info(f"Found {len(filings)} filings")
        return filings[:max_results]
    
    def _search_with_selenium(
        self,
        senator_name: Optional[str],
        state: Optional[str],
        report_type: str,
        start_date: Optional[dt.date],
        end_date: Optional[dt.date],
        max_results: int
    ) -> List[SenateFiling]:
        """Search using Selenium for JavaScript-rendered content"""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait, Select
        from selenium.webdriver.support import expected_conditions as EC
        
        filings = []
        
        try:
            # Navigate to search page
            self.driver.get(Config.SEARCH_URL)
            wait = WebDriverWait(self.driver, 10)
            
            # Fill search form
            if senator_name:
                name_input = wait.until(EC.presence_of_element_located((By.NAME, "last_name")))
                name_input.send_keys(senator_name)
            
            if state:
                state_select = Select(wait.until(EC.presence_of_element_located((By.NAME, "state"))))
                state_select.select_by_value(state)
            
            # Select report type
            if report_type == "ptr":
                ptr_checkbox = wait.until(EC.presence_of_element_located((By.ID, "id_report_type_1")))
                ptr_checkbox.click()
            
            # Set date range
            if start_date:
                date_from = wait.until(EC.presence_of_element_located((By.NAME, "submitted_start_date")))
                date_from.send_keys(start_date.strftime("%m/%d/%Y"))
            
            if end_date:
                date_to = wait.until(EC.presence_of_element_located((By.NAME, "submitted_end_date")))
                date_to.send_keys(end_date.strftime("%m/%d/%Y"))
            
            # Submit search
            submit_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']")))
            submit_button.click()
            
            # Wait for results
            time.sleep(2)
            
            # Parse results
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            filings = self._parse_search_results(soup)
            
            # Handle pagination
            while len(filings) < max_results:
                try:
                    next_button = self.driver.find_element(By.CSS_SELECTOR, "a.next-page")
                    next_button.click()
                    time.sleep(2)
                    
                    soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    page_filings = self._parse_search_results(soup)
                    
                    if not page_filings:
                        break
                    
                    filings.extend(page_filings)
                    
                except Exception:
                    break  # No more pages
            
        except Exception as e:
            logger.error(f"Selenium search failed: {e}")
            return []
        
        return filings[:max_results]
    
    def _build_search_params(
        self,
        senator_name: Optional[str],
        state: Optional[str],
        report_type: str,
        start_date: Optional[dt.date],
        end_date: Optional[dt.date]
    ) -> dict:
        """Build search parameters for Senate eFD"""
        params = {
            'search_type': 'senator',
            'reports': [],
        }
        
        if senator_name:
            # Handle both "Last, First" and "First Last" formats
            if ',' in senator_name:
                last, first = senator_name.split(',', 1)
                params['last_name'] = last.strip()
                params['first_name'] = first.strip()
            else:
                params['last_name'] = senator_name.strip()
        
        if state:
            params['state'] = state.upper()
        
        # Report type mapping
        if report_type == "ptr":
            params['reports'].append('ptr')
        elif report_type == "annual":
            params['reports'].append('annual')
        else:
            # Search all types if not specified
            params['reports'] = ['ptr', 'annual']
        
        # Date range
        if start_date:
            params['submitted_start_date'] = start_date.strftime("%m/%d/%Y")
        else:
            params['submitted_start_date'] = Config.DEFAULT_START_DATE
        
        if end_date:
            params['submitted_end_date'] = end_date.strftime("%m/%d/%Y")
        else:
            params['submitted_end_date'] = dt.date.today().strftime("%m/%d/%Y")
        
        return params
    
    def _extract_csrf_token(self, soup) -> Optional[str]:
        """Extract CSRF token from page"""
        csrf_input = soup.find('input', {'name': 'csrfmiddlewaretoken'})
        if csrf_input:
            return csrf_input.get('value')
        return None
    
    def _parse_search_results(self, soup) -> List[SenateFiling]:
        """Parse search results HTML to extract filings"""
        filings = []
        
        # Look for result rows (adapt selectors based on actual HTML structure)
        # These selectors are estimates - may need adjustment based on actual site
        result_rows = soup.find_all('tr', class_='report-row') or \
                     soup.find_all('div', class_='search-result') or \
                     soup.find_all('tr')[1:]  # Skip header row
        
        for row in result_rows:
            try:
                filing = self._parse_result_row(row)
                if filing:
                    filings.append(filing)
            except Exception as e:
                logger.debug(f"Failed to parse row: {e}")
                continue
        
        return filings
    
    def _parse_result_row(self, row) -> Optional[SenateFiling]:
        """Parse a single result row"""
        try:
            # Extract fields (these selectors need to be adapted to actual HTML)
            cells = row.find_all('td') or row.find_all('div')
            
            if len(cells) < 4:
                return None
            
            # Typical structure (adjust based on actual site):
            # [Senator Name] [State] [Report Type] [Date] [View Link]
            
            senator_name = cells[0].get_text(strip=True)
            state = cells[1].get_text(strip=True)
            report_type = cells[2].get_text(strip=True)
            date_str = cells[3].get_text(strip=True)
            
            # Find PDF link
            pdf_link = row.find('a', href=True)
            if not pdf_link:
                return None
            
            pdf_url = pdf_link['href']
            if not pdf_url.startswith('http'):
                pdf_url = urljoin(Config.BASE_URL, pdf_url)
            
            # Extract report ID from URL
            report_id = self._extract_report_id(pdf_url)
            
            # Parse date
            filing_date = self._parse_date(date_str)
            if not filing_date:
                return None
            
            return SenateFiling(
                senator_name=senator_name,
                state=state,
                report_type=report_type,
                filing_date=filing_date,
                report_id=report_id,
                pdf_url=pdf_url
            )
            
        except Exception as e:
            logger.debug(f"Error parsing row: {e}")
            return None
    
    def _extract_report_id(self, url: str) -> str:
        """Extract report ID from URL"""
        # URLs typically like: /search/view/paper/12345/
        match = re.search(r'/(\d+)/?$', url)
        if match:
            return match.group(1)
        
        # Try query parameter
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if 'id' in params:
            return params['id'][0]
        
        # Use URL hash as fallback
        return hashlib.md5(url.encode()).hexdigest()[:8]
    
    def _parse_date(self, date_str: str) -> Optional[dt.date]:
        """Parse date from various formats"""
        date_formats = [
            "%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y",
            "%b %d, %Y", "%d-%b-%Y", "%m-%d-%Y"
        ]
        
        for fmt in date_formats:
            try:
                return dt.datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
        
        return None
    
    def _get_next_page_url(self, soup) -> Optional[str]:
        """Extract next page URL from pagination"""
        # Look for next page link
        next_link = soup.find('a', {'class': 'next'}) or \
                   soup.find('a', text=re.compile('Next', re.I))
        
        if next_link and next_link.get('href'):
            href = next_link['href']
            if not href.startswith('http'):
                href = urljoin(Config.BASE_URL, href)
            return href
        
        return None

# ============== PDF Processing ==============

class SenatePDFProcessor:
    """Process Senate disclosure PDFs"""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or Path("senate_pdf_cache")
        self.cache_dir.mkdir(exist_ok=True)
    
    def download_pdf(self, url: str, report_id: str) -> Optional[bytes]:
        """Download PDF from Senate eFD"""
        # Check cache first
        cache_file = self.cache_dir / f"{report_id}.pdf"
        if cache_file.exists():
            logger.debug(f"Using cached PDF: {report_id}")
            return cache_file.read_bytes()
        
        try:
            import requests
            
            session = requests.Session()
            session.headers.update({'User-Agent': Config.USER_AGENT})
            
            logger.info(f"Downloading PDF: {url}")
            response = session.get(url, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            
            pdf_bytes = response.content
            
            # Verify it's a PDF
            if not pdf_bytes.startswith(b'%PDF'):
                logger.error("Downloaded content is not a PDF")
                return None
            
            # Cache the PDF
            cache_file.write_bytes(pdf_bytes)
            
            return pdf_bytes
            
        except Exception as e:
            logger.error(f"Failed to download PDF: {e}")
            return None
    
    def parse_ptr_pdf(self, pdf_bytes: bytes, filing: SenateFiling) -> List[SenateTrade]:
        """Parse Senate PTR PDF for trades"""
        trades = []
        
        try:
            import pdfplumber
            
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    # Extract text and tables
                    text = page.extract_text() or ""
                    tables = page.extract_tables() or []
                    
                    # Senate PTRs typically have a specific structure
                    for table in tables:
                        if self._is_transaction_table(table):
                            table_trades = self._parse_transaction_table(
                                table, filing, page_num
                            )
                            trades.extend(table_trades)
            
        except Exception as e:
            logger.error(f"Failed to parse PDF: {e}")
        
        return trades
    
    def _is_transaction_table(self, table: List[List[str]]) -> bool:
        """Check if table contains transactions"""
        if not table or len(table) < 2:
            return False
        
        # Check headers for transaction-related keywords
        headers = " ".join([str(cell or "").lower() for cell in table[0]])
        
        transaction_keywords = [
            "transaction", "asset", "ticker", "amount", "date",
            "purchase", "sale", "type"
        ]
        
        matches = sum(1 for kw in transaction_keywords if kw in headers)
        return matches >= 2
    
    def _parse_transaction_table(
        self, 
        table: List[List[str]], 
        filing: SenateFiling,
        page_num: int
    ) -> List[SenateTrade]:
        """Parse a transaction table from Senate PTR"""
        trades = []
        
        if not table or len(table) < 2:
            return trades
        
        # Parse headers
        headers = [str(h or f"col{i}").lower().strip() for i, h in enumerate(table[0])]
        
        # Process each row
        for row_idx, row in enumerate(table[1:], 1):
            try:
                trade = self._parse_trade_row(row, headers, filing, page_num, row_idx)
                if trade:
                    trades.append(trade)
            except Exception as e:
                logger.debug(f"Failed to parse row: {e}")
                continue
        
        return trades
    
    def _parse_trade_row(
        self,
        row: List[str],
        headers: List[str],
        filing: SenateFiling,
        page_num: int,
        row_idx: int
    ) -> Optional[SenateTrade]:
        """Parse a single trade row from Senate PTR"""
        
        # Create row dict
        row_dict = {}
        for i, header in enumerate(headers):
            if i < len(row):
                row_dict[header] = str(row[i] or "").strip()
        
        # Check for excluded tokens
        row_text = " ".join(row_dict.values()).lower()
        for token in Config.EXCLUDE_TOKENS:
            if token in row_text:
                return None
        
        # Extract transaction date
        date = None
        for date_key in ["transaction date", "date", "trans date", "date of transaction"]:
            if date_key in row_dict and row_dict[date_key]:
                date = self._parse_date(row_dict[date_key])
                if date:
                    break
        
        if not date:
            return None
        
        # Extract action
        action = ""
        for action_key in ["type", "transaction type", "transaction", "action"]:
            if action_key in row_dict:
                action = self._parse_action(row_dict[action_key])
                if action:
                    break
        
        if not action:
            return None
        
        # Extract asset and ticker
        asset = ""
        ticker = ""
        for asset_key in ["asset", "asset name", "description", "security"]:
            if asset_key in row_dict and row_dict[asset_key]:
                asset = row_dict[asset_key]
                ticker = self._extract_ticker(asset)
                break
        
        # Try dedicated ticker column
        for ticker_key in ["ticker", "symbol", "ticker symbol"]:
            if ticker_key in row_dict and row_dict[ticker_key]:
                ticker = row_dict[ticker_key].upper()
                break
        
        if not asset and not ticker:
            return None
        
        # Extract amount
        amount_range = ""
        amount_lo = 0
        amount_hi = 0
        
        for amount_key in ["amount", "value", "amount range"]:
            if amount_key in row_dict and row_dict[amount_key]:
                amount_range = row_dict[amount_key]
                amount_lo, amount_hi = self._parse_amount_range(amount_range)
                break
        
        # Extract owner
        owner = ""
        for owner_key in ["owner", "ownership", "whose"]:
            if owner_key in row_dict:
                owner = row_dict[owner_key]
                break
        
        # Extract notification date if present
        notification_date = None
        for notif_key in ["notification date", "date notified", "reported"]:
            if notif_key in row_dict and row_dict[notif_key]:
                notification_date = self._parse_date(row_dict[notif_key])
                if notification_date:
                    break
        
        # Classify asset type
        asset_type = self._classify_asset_type(asset, ticker)
        
        # Generate unique ID
        uid = self._generate_trade_uid(
            filing.report_id, page_num, row_idx,
            ticker or asset, date.isoformat(), action
        )
        
        return SenateTrade(
            event_uid=uid,
            report_id=filing.report_id,
            senator=filing.senator_name,
            state=filing.state,
            date=date,
            action=action,
            owner=owner,
            asset=asset,
            ticker=ticker,
            asset_type=asset_type,
            amount_range=amount_range,
            amount_lo=amount_lo,
            amount_hi=amount_hi,
            notification_date=notification_date,
            raw_data=row_dict
        )
    
    def _parse_date(self, date_str: str) -> Optional[dt.date]:
        """Parse date from string"""
        if not date_str:
            return None
        
        date_formats = [
            "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d",
            "%B %d, %Y", "%b %d, %Y", "%m-%d-%Y"
        ]
        
        for fmt in date_formats:
            try:
                return dt.datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
        
        return None
    
    def _parse_action(self, action_str: str) -> str:
        """Parse transaction action"""
        if not action_str:
            return ""
        
        action_lower = action_str.lower().strip()
        
        # Common abbreviations in Senate reports
        if action_lower in ['p', 'purchase']:
            return "Purchase"
        elif action_lower in ['s', 'sale', 'sold']:
            return "Sale"
        elif action_lower in ['e', 'exchange']:
            return "Exchange"
        
        # Check for keywords
        for keyword in Config.TRADE_ACTIONS:
            if keyword in action_lower:
                return keyword.capitalize()
        
        return ""
    
    def _extract_ticker(self, asset_str: str) -> str:
        """Extract ticker symbol from asset description"""
        if not asset_str:
            return ""
        
        # Look for pattern: "Company Name (TICKER)"
        match = re.search(r'\(([A-Z][A-Z0-9.\-]{0,9})\)', asset_str)
        if match:
            return match.group(1)
        
        # Look for pattern: "TICKER - Company Name"
        match = re.search(r'^([A-Z][A-Z0-9.\-]{0,9})\s*[-–]\s*', asset_str)
        if match:
            return match.group(1)
        
        return ""
    
    def _parse_amount_range(self, amount_str: str) -> Tuple[int, int]:
        """Parse amount range from Senate format"""
        if not amount_str:
            return (0, 0)
        
        # Clean the string
        amount_str = amount_str.replace(',', '').replace(', '').strip()
        
        # Senate often uses format: "$1,001 - $15,000"
        match = re.search(r'(\d+)\s*[-–]\s*(\d+)', amount_str)
        if match:
            try:
                lo = int(match.group(1))
                hi = int(match.group(2))
                return (lo, hi)
            except ValueError:
                pass
        
        # Check for standard Senate buckets
        amount_lower = amount_str.lower()
        if "1,001" in amount_str and "15,000" in amount_str:
            return (1001, 15000)
        elif "15,001" in amount_str and "50,000" in amount_str:
            return (15001, 50000)
        elif "50,001" in amount_str and "100,000" in amount_str:
            return (50001, 100000)
        elif "100,001" in amount_str and "250,000" in amount_str:
            return (100001, 250000)
        elif "250,001" in amount_str and "500,000" in amount_str:
            return (250001, 500000)
        elif "500,001" in amount_str and "1,000,000" in amount_str:
            return (500001, 1000000)
        elif "over 1,000,000" in amount_lower or "over $1,000,000" in amount_lower:
            return (1000001, 5000000)
        
        return (0, 0)
    
    def _classify_asset_type(self, asset: str, ticker: str) -> AssetType:
        """Classify the asset type"""
        asset_lower = asset.lower()
        ticker_upper = ticker.upper() if ticker else ""
        
        # Check for options
        if any(kw in asset_lower for kw in ['call', 'put', 'option']):
            return AssetType.OPTION
        
        # Check for crypto
        if ticker_upper in Config.CRYPTO_SYMBOLS:
            return AssetType.CRYPTO
        if any(kw in asset_lower for kw in ['bitcoin', 'ethereum', 'crypto']):
            return AssetType.CRYPTO
        
        # Check for bonds
        if any(kw in asset_lower for kw in ['bond', 'treasury', 'note']):
            return AssetType.BOND
        
        # Check for mutual funds
        if any(kw in asset_lower for kw in ['mutual fund', 'index fund']):
            return AssetType.MUTUAL_FUND
        
        # Check for ETFs
        if 'etf' in asset_lower or ticker_upper.endswith('F'):
            return AssetType.ETF
        
        # Default to stock if has ticker
        return AssetType.STOCK if ticker else AssetType.OTHER
    
    def _generate_trade_uid(
        self,
        report_id: str,
        page: int,
        row: int,
        asset: str,
        date: str,
        action: str
    ) -> str:
        """Generate unique trade ID"""
        key = f"senate|{report_id}|{page}|{row}|{asset}|{date}|{action}".encode()
        return hashlib.sha256(key).hexdigest()[:16]

# ============== Main Processing Functions ==============

def process_senator(
    senator_name: str,
    scraper: SenateEFDScraper,
    processor: SenatePDFProcessor,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    report_types: List[str] = ["ptr"]
) -> List[SenateTrade]:
    """Process all filings for a specific senator"""
    
    all_trades = []
    
    for report_type in report_types:
        logger.info(f"Searching {report_type} filings for {senator_name}")
        
        # Search for filings
        filings = scraper.search_filings(
            senator_name=senator_name,
            report_type=report_type,
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(f"Found {len(filings)} {report_type} filings")
        
        # Process each filing
        for filing in filings:
            if not filing.is_ptr and report_type == "ptr":
                continue
            
            # Download PDF
            pdf_bytes = processor.download_pdf(filing.pdf_url, filing.report_id)
            if not pdf_bytes:
                logger.warning(f"Could not download PDF for {filing.report_id}")
                continue
            
            # Parse trades
            trades = processor.parse_ptr_pdf(pdf_bytes, filing)
            
            if trades:
                logger.info(f"Extracted {len(trades)} trades from {filing.report_id}")
                all_trades.extend(trades)
            
            # Rate limiting
            time.sleep(Config.RATE_LIMIT_DELAY)
    
    return all_trades

def process_all_senators(
    scraper: SenateEFDScraper,
    processor: SenatePDFProcessor,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    states: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> List[SenateTrade]:
    """Process all senators or filtered by state"""
    
    all_trades = []
    
    if states:
        # Process specific states
        for state in states:
            logger.info(f"Processing senators from {state}")
            
            filings = scraper.search_filings(
                state=state,
                report_type="ptr",
                start_date=start_date,
                end_date=end_date,
                max_results=limit or 1000
            )
            
            for filing in filings:
                pdf_bytes = processor.download_pdf(filing.pdf_url, filing.report_id)
                if not pdf_bytes:
                    continue
                
                trades = processor.parse_ptr_pdf(pdf_bytes, filing)
                all_trades.extend(trades)
                
                time.sleep(Config.RATE_LIMIT_DELAY)
    else:
        # Process all senators
        filings = scraper.search_filings(
            report_type="ptr",
            start_date=start_date,
            end_date=end_date,
            max_results=limit or 10000
        )
        
        logger.info(f"Processing {len(filings)} PTR filings")
        
        for i, filing in enumerate(filings, 1):
            if limit and i > limit:
                break
            
            logger.info(f"Processing filing {i}/{len(filings)}: {filing.senator_name}")
            
            pdf_bytes = processor.download_pdf(filing.pdf_url, filing.report_id)
            if not pdf_bytes:
                continue
            
            trades = processor.parse_ptr_pdf(pdf_bytes, filing)
            all_trades.extend(trades)
            
            time.sleep(Config.RATE_LIMIT_DELAY)
    
    return all_trades

# ============== Export Functions ==============

def export_to_json(trades: List[SenateTrade], output_path: str):
    """Export trades to JSON"""
    data = [trade.to_dict() for trade in trades]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Exported {len(trades)} trades to {output_path}")

def export_to_csv(trades: List[SenateTrade], output_path: str):
    """Export trades to CSV"""
    try:
        import pandas as pd
        
        data = [trade.to_dict() for trade in trades]
        df = pd.DataFrame(data)
        
        # Reorder columns
        column_order = [
            'date', 'senator', 'state', 'action', 'ticker', 'asset',
            'asset_type', 'amount_lo', 'amount_hi', 'amount_range',
            'owner', 'notification_date', 'report_id'
        ]
        
        columns = [col for col in column_order if col in df.columns]
        df = df[columns]
        
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(trades)} trades to {output_path}")
        
    except ImportError:
        logger.error("pandas not installed. Install with: pip install pandas")

def generate_summary_statistics(trades: List[SenateTrade]) -> dict:
    """Generate summary statistics"""
    from collections import Counter
    
    stats = {
        'total_trades': len(trades),
        'unique_senators': len(set(t.senator for t in trades)),
        'unique_tickers': len(set(t.ticker for t in trades if t.ticker)),
        'date_range': {
            'earliest': min(t.date for t in trades).isoformat() if trades else None,
            'latest': max(t.date for t in trades).isoformat() if trades else None
        },
        'by_action': dict(Counter(t.action for t in trades)),
        'by_asset_type': dict(Counter(t.asset_type.value for t in trades)),
        'by_state': dict(Counter(t.state for t in trades)),
        'top_traded_tickers': [],
        'most_active_senators': [],
        'largest_trades': []
    }
    
    # Top tickers
    ticker_counts = Counter(t.ticker for t in trades if t.ticker)
    stats['top_traded_tickers'] = [
        {'ticker': ticker, 'count': count}
        for ticker, count in ticker_counts.most_common(10)
    ]
    
    # Most active senators
    senator_counts = Counter(t.senator for t in trades)
    stats['most_active_senators'] = [
        {'senator': senator, 'trades': count}
        for senator, count in senator_counts.most_common(10)
    ]
    
    # Largest trades
    largest = sorted(trades, key=lambda t: t.amount_hi, reverse=True)[:10]
    stats['largest_trades'] = [
        {
            'senator': t.senator,
            'ticker': t.ticker or t.asset,
            'action': t.action,
            'amount_range': t.amount_range,
            'date': t.date.isoformat()
        }
        for t in largest
    ]
    
    return stats

# ============== Main Function ==============

def main():
    """Main entry point for Senate disclosure parser"""
    parser = argparse.ArgumentParser(
        description="Senate Financial Disclosure Parser - Extract trades from Senate eFD system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a specific senator's PTRs
  python senate/ingest.py --senator "Warren, Elizabeth" --since 2024-01-01 --out-json warren_trades.json
  
  # Process all senators from specific states
  python senate/ingest.py --states CA,NY,TX --since 2024-01-01 --out-csv state_trades.csv
  
  # Process all PTRs from 2024 with limit
  python senate/ingest.py --since 2024-01-01 --limit 100 --out-json recent_trades.json --summary
  
  # Use Selenium for JavaScript-heavy pages
  python senate/ingest.py --senator "Cruz, Ted" --use-selenium --out-json cruz_trades.json
  
  # Search and download without parsing (metadata only)
  python senate/ingest.py --since 2024-01-01 --no-parse --out-json filings.json
        """
    )
    
    # Search options
    parser.add_argument('--senator', help='Senator name (Last, First or just Last)')
    parser.add_argument('--states', help='Comma-separated state codes (e.g., CA,TX,NY)')
    parser.add_argument('--since', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--until', help='End date (YYYY-MM-DD)')
    parser.add_argument('--report-types', default='ptr',
                       help='Report types to search (ptr,annual,all)')
    
    # Processing options
    parser.add_argument('--no-parse', action='store_true',
                       help='Only search and save metadata, do not parse PDFs')
    parser.add_argument('--limit', type=int, help='Limit number of filings to process')
    parser.add_argument('--cache-dir', help='Directory for caching PDFs')
    parser.add_argument('--use-selenium', action='store_true',
                       help='Use Selenium for JavaScript-heavy pages')
    
    # Output options
    parser.add_argument('--out-json', help='Output JSON file path')
    parser.add_argument('--out-csv', help='Output CSV file path')
    parser.add_argument('--summary', action='store_true',
                       help='Print summary statistics')
    
    # Logging
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logger.setLevel(getattr(logging, args.log_level))
    
    # Parse dates
    start_date = dt.date.fromisoformat(args.since) if args.since else None
    end_date = dt.date.fromisoformat(args.until) if args.until else None
    
    # Parse states
    states = [s.strip().upper() for s in args.states.split(',')] if args.states else None
    
    # Parse report types
    if args.report_types == 'all':
        report_types = ['ptr', 'annual']
    else:
        report_types = [t.strip() for t in args.report_types.split(',')]
    
    # Initialize components
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    processor = SenatePDFProcessor(cache_dir)
    
    with SenateEFDScraper(use_selenium=args.use_selenium) as scraper:
        
        if args.no_parse:
            # Just search and save metadata
            filings = []
            
            if args.senator:
                for report_type in report_types:
                    filings.extend(scraper.search_filings(
                        senator_name=args.senator,
                        report_type=report_type,
                        start_date=start_date,
                        end_date=end_date,
                        max_results=args.limit or 1000
                    ))
            elif states:
                for state in states:
                    for report_type in report_types:
                        filings.extend(scraper.search_filings(
                            state=state,
                            report_type=report_type,
                            start_date=start_date,
                            end_date=end_date,
                            max_results=args.limit or 1000
                        ))
            else:
                for report_type in report_types:
                    filings.extend(scraper.search_filings(
                        report_type=report_type,
                        start_date=start_date,
                        end_date=end_date,
                        max_results=args.limit or 10000
                    ))
            
            # Export metadata
            metadata = [
                {
                    'senator': f.senator_name,
                    'state': f.state,
                    'report_type': f.report_type,
                    'filing_date': f.filing_date.isoformat(),
                    'report_id': f.report_id,
                    'pdf_url': f.pdf_url
                }
                for f in filings
            ]
            
            if args.out_json:
                with open(args.out_json, 'w') as f:
                    json.dump(metadata, f, indent=2)
                logger.info(f"Saved {len(metadata)} filings to {args.out_json}")
            else:
                print(json.dumps(metadata, indent=2))
            
            return 0
        
        # Process and parse PDFs
        trades = []
        
        if args.senator:
            trades = process_senator(
                args.senator,
                scraper,
                processor,
                start_date,
                end_date,
                report_types
            )
        elif states:
            trades = process_all_senators(
                scraper,
                processor,
                start_date,
                end_date,
                states,
                args.limit
            )
        else:
            trades = process_all_senators(
                scraper,
                processor,
                start_date,
                end_date,
                None,
                args.limit
            )
        
        logger.info(f"Extracted {len(trades)} total trades")
        
        # Export results
        if args.out_json:
            export_to_json(trades, args.out_json)
        
        if args.out_csv:
            export_to_csv(trades, args.out_csv)
        
        # Print summary
        if args.summary:
            stats = generate_summary_statistics(trades)
            print("\n" + "="*60)
            print("SENATE TRADING SUMMARY")
            print("="*60)
            print(json.dumps(stats, indent=2))
        
        # Default output to stdout if no file specified
        if not any([args.out_json, args.out_csv, args.summary]):
            data = [trade.to_dict() for trade in trades]
            print(json.dumps(data, indent=2))
    
    return 0

if __name__ == "__main__":
    sys.exit(main())