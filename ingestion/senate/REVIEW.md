# Review of `ingestion/senate/ingest.py`

## 1. The script does not run (syntax error)
- `python -m py_compile ingestion/senate/ingest.py` fails with an "unterminated string literal" at line 866, so nothing in the module is executable in its current form. This line tries to call `str.replace` but omits the closing quote on the pattern. 【e13937†L1-L4】【F:ingestion/senate/ingest.py†L864-L870】

## 2. Search workflow is based on incorrect assumptions about the Senate eFD site
- `SenateEFDScraper._build_search_params` sends a payload with keys such as `reports` and `submitted_start_date` that match the House workflow, not the Senate API. The Senate site exposes its search results via JSON endpoints (e.g. `/search/report/data/`) and expects querystring parameters like `first_name`, `last_name`, `filer_type`, etc., not an array called `reports`. 【F:ingestion/senate/ingest.py†L406-L455】
- `_parse_search_results` and `_parse_result_row` assume the response HTML contains `<tr class="report-row">` rows with predictable column order. The Senate application renders results inside Vue components and links to reports through JSON requests, so those selectors never match and no filings will be found. 【F:ingestion/senate/ingest.py†L463-L538】
- Even if a link were discovered, `REPORT_BASE` points at `/search/view/paper/`, but the real download endpoint is `/search/view/download/` that takes `logno` and `filename` query parameters returned by the JSON search response. Because of that mismatch, `SenatePDFProcessor.download_pdf` would request the wrong URL and always fail. 【F:ingestion/senate/ingest.py†L52-L61】【F:ingestion/senate/ingest.py†L585-L617】

## 3. PDF parsing strategy cannot work on Senate disclosures
- `SenatePDFProcessor.parse_ptr_pdf` attempts to extract tables with `pdfplumber`. Most Senate PTRs are scanned images without embedded text or table structure, so `pdfplumber` returns empty pages. The Senate site instead provides machine-readable XML/CSV payloads alongside the filings that should be parsed instead of attempting OCR-free PDF scraping. 【F:ingestion/senate/ingest.py†L619-L804】
- Because the parser never looks at the XML payload and relies entirely on table extraction, the code will silently return an empty trade list for nearly all filings.

## 4. Missing handling for Senate-specific metadata
- Real Senate search responses include important identifiers (`filingId`, `docId`, `logNumber`, etc.) that are needed when fetching the data export (CSV/XML). The implementation fabricates `report_id` by hashing the URL, so even if the download worked it would not be possible to match disclosures reliably. 【F:ingestion/senate/ingest.py†L531-L545】
- There is no logic for fetching or parsing the official Senate transaction CSV (available under `/download/csv/`), so the ingest path never surfaces the trade data the project needs.

## Summary
The provided Senate ingest script cannot run due to a syntax error, targets the wrong endpoints when searching, and assumes PDFs contain structured tables even though the Senate publishes machine-readable data separately. The Senate ingestion pipeline will need to be reimplemented around the actual JSON/CSV endpoints exposed by the eFD system instead of mirroring the House PDF workflow.
