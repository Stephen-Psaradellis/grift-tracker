
import argparse
import datetime as dt
import hashlib
import io
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Optional, Tuple

# Optional heavy deps are imported lazily inside functions:
# pdfplumber, camelot, requests

@dataclass
class Filing:
    first: str
    last: str
    filing_type: str
    state_dst: str
    year: int
    filing_date: dt.date
    doc_id: str

    @property
    def pdf_url(self) -> str:
        # Most PTRs appear as W/C/D; others (e.g., P) are FD
        ptr_types = {"W", "C", "D"}
        folder = "ptr-pdfs" if self.filing_type.upper() in ptr_types else "financial-pdfs"
        return f"https://disclosures-clerk.house.gov/public_disc/{folder}/{self.year}/{self.doc_id}.pdf"

def parse_date(s: str) -> dt.date:
    # Clerk uses M/D/YYYY or MM/DD/YYYY
    m, d, y = [int(x) for x in s.split("/")]
    return dt.date(y, m, d)

def parse_financial_disclosure_xml(xml_path: str) -> List[Filing]:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    out: List[Filing] = []
    for m in root.findall(".//Member"):
        first = (m.findtext("First") or "").strip()
        last = (m.findtext("Last") or "").strip()
        ftype = (m.findtext("FilingType") or "").strip()
        state_dst = (m.findtext("StateDst") or "").strip()
        year_txt = (m.findtext("Year") or "").strip()
        fdate_txt = (m.findtext("FilingDate") or "").strip()
        docid = (m.findtext("DocID") or "").strip()

        if not docid or not year_txt or not fdate_txt:
            continue
        try:
            year = int(year_txt)
            fdate = parse_date(fdate_txt)
        except Exception:
            continue
        out.append(Filing(first, last, ftype, state_dst, year, fdate, docid))
    return out

def name_matches(f: Filing, names: List[Tuple[str,str]]) -> bool:
    if not names:
        return True
    last_first = (f.last.lower(), f.first.lower())
    for (ln, fn) in names:
        if (ln.lower(), fn.lower()) == last_first:
            return True
    return False

def filingtype_matches(f: Filing, types: List[str]) -> bool:
    if not types:
        return True
    return f.filing_type.upper() in {t.upper() for t in types}

def filter_filings(
    filings: List[Filing],
    since: Optional[dt.date],
    names: List[Tuple[str,str]],
    filing_types: List[str]
) -> List[Filing]:
    out = []
    for f in filings:
        if since and f.filing_date < since:
            continue
        if not name_matches(f, names):
            continue
        if not filingtype_matches(f, filing_types):
            continue
        out.append(f)
    return out

# ---------- Heuristics & utilities ----------

AMOUNT_BUCKETS = [
    (0, 1000, 0),
    (1000, 15000, 1),
    (15000, 50000, 2),
    (50000, 100000, 3),
    (100000, 250000, 4),
    (250000, 10**12, 5),
]
RANGE_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d+)?)\s*[-–]\s*\$?\s*([\d,]+(?:\.\d+)?)")
ASSET_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<ticker>[A-Z.\-]{1,10})\)\s*(?:\[[A-Z]{2,3}\])?$")

TRADE_ACTIONS = {
    "buy",
    "purchase",
    "sell",
    "sale",
    "exchange",
    "exercise",
    "assignment",
    "expiration",
    "acquire",
    "acquisition",
    "dispose",
    "disposition",
}
EXCLUDE_TOKENS = [
    "salary",
    "wages",
    "freelance",
    "consult",
    "pension",
    "retirement",
    "social security",
    "mortgage",
    "loan",
    "credit",
    "liability",
    "student loan",
    "car loan",
    "auto loan",
    "revolving",
    "patreon",
    "youtube",
    "tiktok",
    "teaching",
    "spouse salary",
    "brand",
    "marketing",
    "bursar",
]
AMOUNT_RANGE_RE = re.compile(r"\$\s*\d[\d,]*(?:\.\d+)?\s*[-–]\s*\$\s*\d[\d,]*(?:\.\d+)?")

def is_amount_range(s: str) -> bool:
    return bool(AMOUNT_RANGE_RE.search((s or "").replace("\u00a0"," ").replace("\n"," ")))

def has_excluded_token(row_values: List[str]) -> bool:
    blob = " ".join([v or "" for v in row_values]).lower()
    for token in EXCLUDE_TOKENS:
        if " " in token:
            if token in blob:
                return True
        else:
            if re.search(rf"\b{re.escape(token)}\b", blob):
                return True
    return False

def parse_action(s: str) -> str:
    t = (s or "").strip().lower()
    # normalize variants
    t = t.replace("purchase", "buy").replace("sale", "sell")
    for a in TRADE_ACTIONS:
        if a in t:
            return a.capitalize()
    return ""

def parse_amount_bucket(amount_str: str) -> Tuple[int,int,int]:
    if not amount_str:
        return (0,0,0)
    m = RANGE_RE.search(amount_str.replace("\u00a0"," ").replace("\n"," "))
    if not m:
        return (0,0,0)
    lo_txt = m.group(1).replace(",","")
    hi_txt = m.group(2).replace(",","")
    try:
        lo = int(float(lo_txt))
    except ValueError:
        lo = 0
    try:
        hi = int(float(hi_txt))
    except ValueError:
        hi = 0
    score = 0
    for lo_b, hi_b, s in AMOUNT_BUCKETS:
        if lo >= lo_b and hi <= hi_b:
            score = s
            break
    return (lo, hi, score)

def row_uid(source: str, filing_id: str, line_no: int, ticker: str, date_iso: str, amount_str: str, action: str) -> str:
    key = f"{source}|{filing_id}|{line_no}|{ticker}|{date_iso}|{amount_str}|{action}".encode()
    return hashlib.sha256(key).hexdigest()

def clean_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def looks_like_trade(row: dict) -> bool:
    k = {clean_key(k): v for k,v in row.items()}
    values_concat = " ".join((v or "") for v in k.values())
    has_amount = bool(re.search(r"\$\s*\d", values_concat))
    has_date = any(key in k for key in ["date","transaction date","tx date","trade date","transaction dt"])
    has_action = any(key in k for key in ["transaction type","type"])
    # require amount + (date or action)
    return has_amount and (has_date or has_action)

def parse_asset(field: str) -> Tuple[str,str]:
    if not field:
        return ("","")
    s = field.replace("\n"," ").strip()
    m = ASSET_RE.search(s)
    if m:
        return (m.group("name").strip(), m.group("ticker").strip())
    return (s, "")

# ---------- PDF parsing ----------

def classify_pdf(pdf_bytes: bytes) -> str:
    # returns "PTR" | "FD" | "UNKNOWN"
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            first_text = (pdf.pages[0].extract_text() or "").lower()
            if "periodic transaction report" in first_text:
                return "PTR"
            if "financial disclosure" in first_text:
                return "FD"
    except Exception:
        pass
    return "UNKNOWN"

def _rows_from_table(tbl):
    headers = [ (tbl[0][i] or f"col{i}").strip().lower().replace("\n"," ").replace("  "," ")
               for i in range(len(tbl[0])) ]
    out = []
    for r in tbl[1:]:
        row = { headers[i]: (r[i] or "").strip() for i in range(len(headers)) }
        out.append(row)
    return out

def extract_transactions_region(pdf_bytes: bytes) -> List[dict]:
    # For FD files: only keep tables on pages that mention "Transactions"
    rows: List[dict] = []
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = (page.extract_text() or "").lower()
                if "transactions" not in text:
                    continue
                tables = page.extract_tables() or []
                for tbl in tables:
                    if tbl and len(tbl) > 1:
                        rows.extend(_rows_from_table(tbl))
    except Exception:
        pass
    return rows

def parse_tables_anyway(pdf_bytes: bytes) -> List[dict]:
    # PTR or unknown: try camelot then pdfplumber
    rows: List[dict] = []
    try:
        import camelot, tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
            tmp.write(pdf_bytes); tmp.flush()
            try:
                tables = camelot.read_pdf(tmp.name, pages="all", flavor="lattice")
                if tables and len(tables) == 0:
                    tables = camelot.read_pdf(tmp.name, pages="all", flavor="stream")
            except Exception:
                tables = []
            for t in tables or []:
                df = t.df
                headers = [h.strip().lower() for h in df.iloc[0].tolist()]
                for i in range(1, len(df)):
                    vals = df.iloc[i].tolist()
                    row = {headers[j] if j < len(headers) else f"col{j}": str(vals[j]).strip() for j in range(len(vals))}
                    rows.append(row)
    except Exception:
        pass
    if not rows:
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for p in pdf.pages:
                    ts = p.extract_tables() or []
                    for tbl in ts:
                        if not tbl or len(tbl) < 2:
                            continue
                        rows.extend(_rows_from_table(tbl))
        except Exception:
            pass
    return rows

def parse_pdf_for_trades(pdf_bytes: bytes, filing_id: str, actor: str) -> List[dict]:
    ptype = classify_pdf(pdf_bytes)
    if ptype == "FD":
        raw_rows = extract_transactions_region(pdf_bytes)  # only pages mentioning "Transactions"
        if not raw_rows:
            raw_rows = parse_tables_anyway(pdf_bytes)
    else:
        raw_rows = parse_tables_anyway(pdf_bytes)

    kept = []
    for r in raw_rows:
        rr = { clean_key(k): (v or "").strip() for k,v in r.items() }
        # exclude obvious income/liability/etc. rows
        if has_excluded_token(list(rr.values())):
            continue

        # pick likely columns
        date = rr.get("date") or rr.get("transaction date") or rr.get("tx date") or rr.get("trade date") or rr.get("transaction dt") or ""
        action_raw = rr.get("transaction type") or rr.get("type") or ""
        action = parse_action(action_raw)
        owner = rr.get("owner") or ""
        amount = rr.get("amount") or rr.get("amount range") or rr.get("value") or rr.get("value of asset") or ""
        asset = rr.get("asset") or rr.get("security") or rr.get("company") or rr.get("description") or ""

        # inclusion gates
        if not action:
            continue
        if not is_amount_range(amount):
            continue

        # date parse
        date_iso = ""
        for fmt in ("%m/%d/%Y","%m/%d/%y","%m-%d-%Y","%Y-%m-%d"):
            try:
                date_iso = dt.datetime.strptime(date, fmt).date().isoformat()
                break
            except Exception:
                pass
        if not date_iso:
            continue

        company, ticker = parse_asset(asset)
        if not (ticker or company):
            continue

        lo, hi, bucket = parse_amount_bucket(amount)
        uid = row_uid("house_ptr", filing_id, len(kept)+1, ticker or company, date_iso, amount, action)
        kept.append({
            "event_uid": uid,
            "filing_id": filing_id,
            "actor": actor,
            "date": date_iso,
            "action": action,
            "owner": owner,
            "ticker": ticker,
            "company": company,
            "amount_range": amount,
            "amount_lo": lo,
            "amount_hi": hi,
            "amount_bucket": bucket,
            "raw": r
        })
    return kept

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Filter House FinancialDisclosure XML and (optionally) parse PDFs to extract trades only.")
    ap.add_argument("--xml", required=True, help="Path to FinancialDisclosure XML (e.g., 2025FD.xml)")
    ap.add_argument("--since", required=False, help="YYYY-MM-DD; only filings on/after this date")
    ap.add_argument("--names", required=False, help="Semicolon-separated 'Last,First' list. If omitted, all names included.")
    ap.add_argument("--filing-types", required=False, default="", help="Comma-separated FilingType codes (e.g., W,C,D). Empty = all.")
    ap.add_argument("--download-and-parse", action="store_true", help="If set, download each PDF and extract trade-like rows only.")
    ap.add_argument("--out-json", required=False, help="Write results (metadata or parsed rows) to JSON file")
    args = ap.parse_args()

    since = dt.date.fromisoformat(args.since) if args.since else None

    names_list: List[Tuple[str,str]] = []
    if args.names:
        for part in args.names.split(";"):
            if not part.strip():
                continue
            if "," in part:
                ln, fn = part.split(",", 1)
            else:
                ln, fn = part.strip(), ""
            names_list.append((ln.strip(), fn.strip()))

    types = [t.strip() for t in args.filing_types.split(",") if t.strip()]

    filings = parse_financial_disclosure_xml(args.xml)
    filtered = filter_filings(filings, since, names_list, types)

    # If not downloading, just print the metadata list with URLs
    result = []
    for f in filtered:
        item = {
            "first": f.first,
            "last": f.last,
            "filing_type": f.filing_type,
            "state_dst": f.state_dst,
            "year": f.year,
            "filing_date": f.filing_date.isoformat(),
            "doc_id": f.doc_id,
            "pdf_url": f.pdf_url
        }
        result.append(item)

    if not args.download_and_parse:
        if args.out_json:
            with open(args.out_json, "w", encoding="utf-8") as w:
                json.dump(result, w, indent=2)
        else:
            print(json.dumps(result, indent=2))
        return

    ALL_ROWS = []
    for f in filtered:
        # Download primary URL; if 404, try alternate folder
        try:
            import requests
            r = requests.get(f.pdf_url, timeout=30)
            if r.status_code == 404:
                alt_folder = "financial-pdfs" if "ptr-pdfs" in f.pdf_url else "ptr-pdfs"
                alt_url = f"https://disclosures-clerk.house.gov/public_disc/{alt_folder}/{f.year}/{f.doc_id}.pdf"
                r = requests.get(alt_url, timeout=30)
                if r.status_code == 404:
                    raise Exception("404 Not Found in both ptr-pdfs and financial-pdfs")
                else:
                    pdf_bytes = r.content
                    sys.stderr.write(f"[INFO] Used fallback URL {alt_url}\n")
            else:
                r.raise_for_status()
                pdf_bytes = r.content
        except Exception as e:
            sys.stderr.write(f"[WARN] Failed download {f.pdf_url}: {e}\n")
            continue

        norm = parse_pdf_for_trades(pdf_bytes, f.doc_id, f"{f.first} {f.last}".strip())
        if not norm:
            sys.stderr.write(f"[INFO] No trade-like rows found in {f.pdf_url}. Skipping.\n")
            continue
        ALL_ROWS.extend(norm)

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as w:
            json.dump(ALL_ROWS, w, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(ALL_ROWS, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()