wget https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025FD.zip \
unzip 2025FD.zip

python ingestion/house/ingest.py --xml 2025FD.xml \
    --filing-types P \
    --download-and-parse \
    --cache-dir ./pdf_cache \
    --out-parquet trades.parquet