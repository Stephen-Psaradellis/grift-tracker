Designing an n8n Workflow to Mine US‑Government‑Related Data for Investment Insights
Introduction and Ethical Considerations

Publicly available data about legislative activities, official statements, news coverage and even required financial disclosures from U.S. elected officials can be used to better understand market impacts. However, correlation does not mean causation; making investment decisions based on perceived motives of public officials can lead to losses and may involve ethical and legal risks. The plan below outlines how to automate the collection of public information and build dashboards in Supabase to track patterns. It does not guarantee financial return and should not be interpreted as financial advice.

Data Sources to Combine

Congressional trading activity – Financial Modeling Prep’s Senate Trading Activity API monitors trades by U.S. senators, providing trade dates, assets, amounts and prices. It’s designed to improve transparency and helps identify potential conflicts of interest. FMP also offers related endpoints for House trades and financial disclosures.

Alternative data and market fundamentals – Finnhub offers real‑time stock, currency and crypto APIs. It provides institutional‑grade fundamental data, 30 years of historical financial statements and detailed ETF/indices constituents
finnhub.io
. Finnhub’s alternative‑data suite includes real‑time news and social‑media sentiment, ESG data and “Congressional Trading & Senate Lobbying” datasets
finnhub.io
.

Legislative and member data – The Sunlight Congress API is a live JSON API that lets you look up members of Congress, their official social‑media accounts and current legislation
sunlightlabs.github.io
. It provides endpoints to retrieve bills, votes, amendments, nominations, hearings and floor updates
sunlightlabs.github.io
. LegiScan and OpenStates APIs offer similar data for state legislatures and can be added later.

News and blog coverage – News API is a simple REST API for searching live articles across thousands of news sources. You can filter by keywords (e.g., company name or politician), date ranges, source domains and language, and sort results by date, relevancy or popularity
newsapi.org
. It requires a free API key.

Reddit and social‑media discussions – Reddit’s API (via PRAW or other clients) provides JSON access to posts, comments and subreddit information
datahen.com
. You can search for posts, retrieve comments and monitor engagement metrics such as up‑votes and comment counts
datahen.com
. It requires OAuth authentication and has rate limits
datahen.com
. Finnhub’s news‑sentiment endpoint can supplement social‑media sentiment data
finnhub.io
.

Historical price data for performance tracking – Use Finnhub or Alpha Vantage (another free API) to fetch historical price series for stocks, options, ETFs and cryptocurrencies; this will allow you to evaluate the performance of each trade suggestion over time.

Official statements and press releases – Many government agencies publish press releases and speeches as RSS feeds or static pages. You can use n8n’s HTTP Request node in combination with a scraping service (e.g., Firecrawl) to extract text from those pages; a similar technique is used in the n8n template that scrapes congressional trades and summarizes them with OpenAI
n8n.io
.

High‑Level Workflow Design
1. Prepare APIs and Credentials

Acquire API keys for FMP, Finnhub, News API, Sunlight Congress and Reddit. Store them as encrypted credentials in n8n using the Credentials manager. N8N securely stores API keys so you don’t need to re‑enter them.

Create a Supabase project and set up tables: one for raw trades (senate/house), one for legislative events (bills, votes), one for news/social posts, one for analysis metrics, and one for performance results. Supabase offers a full Postgres database with authentication, edge functions and real‑time subscriptions.

2. Schedule and Trigger Workflows

Use the Schedule Trigger node to run data‑collection workflows at different frequencies:

Daily (e.g., 03:00 UTC) – collect congressional trades, new bills, votes, and official statements.

Hourly – pull news articles and Reddit discussions to capture recent market sentiment. Finnhub’s news API and sentiment endpoints update frequently
finnhub.io
.

Weekly or monthly – compute performance metrics by fetching historical price data and evaluating previous recommendations.

Use separate schedules for heavy tasks (e.g., summarizing news) so that long processing times do not delay other flows.

3. Retrieve and Store Data

Each data‑collection workflow can follow a similar pattern:

Fetch congressional trading data: Use HTTP Request nodes to call the FMP Senate/House Trades endpoints. Parse the returned JSON and map fields (trade date, politician, ticker, transaction type, amount) into a standardized structure. Use the Supabase node to upsert the data into the “trades” table. Record the retrieval timestamp.

Fetch Finnhub alternative data: Use HTTP nodes to call Finnhub’s Congressional Trading & Senate Lobbying endpoint
finnhub.io
 and its News & Social Media Sentiment endpoint. Store trades and sentiment scores. You can also call Finnhub’s financial statements and ETF constituents endpoints to enrich company information
finnhub.io
.

Legislative data: Use HTTP nodes to call Sunlight Congress endpoints for new bills, votes and floor updates
sunlightlabs.github.io
. Filter to “latest 24 hours” using the last_updated parameter. Record sponsors, cosponsors and subjects. Optionally add LegiScan or OpenStates endpoints for state legislation.

News scraping: Use HTTP nodes to call News API’s /v2/everything endpoint with queries like q=CompanyName OR PoliticianName and date filters. The API lets you search by keyword, date, source domain and language
newsapi.org
. Parse article titles, descriptions and sentiment (you can run your own sentiment analysis or rely on Finnhub’s sentiment scores).

Social‑media scraping: For Reddit, use HTTP nodes (with OAuth credentials) to call /search endpoints or PRAW to fetch new posts containing company tickers or politician names. The Reddit API returns structured JSON of posts, comments and engagement metrics
datahen.com
. Avoid scraping content that may violate privacy or API terms.

Official statements and press releases: Use HTTP Request nodes with Firecrawl or a similar service to extract text from official press pages. Follow the approach in the n8n template: call Firecrawl’s extraction endpoint, wait, then retrieve the result and pass it to the next node for summarization
n8n.io
.

4. Process and Enrich Data

Processing tasks can be done in separate workflows triggered after data ingestion:

Normalize tickers and companies: Use a Function node (custom JavaScript) to match traded tickers to company names, sectors and industries. You can call Finnhub’s company profile endpoint for metadata.

Link trades to legislation: Cross‑reference tickers with bills or votes that mention related sectors. For example, if a senator trades a healthcare stock, check if any healthcare‑related bills were introduced or voted on near the trade date.

Compute sentiment scores: Combine Finnhub’s news/social sentiment scores
finnhub.io
 with a custom sentiment analysis of news headlines using an AI model. Use the OpenAI node (or another LLM) to classify each headline as positive, negative or neutral.

Generate features for ranking: For each traded stock/crypto/ETF, compute features such as:

Recent sentiment trend (average of last N news/social posts).

Volume of trades by officials.

Bill proximity (time between trade and relevant legislation/vote).

Official statements that mention the company or industry.

5. Suggest Investments

Scoring model: Build a simple scoring algorithm in a Function node. Assign weights to features (e.g., high official trading volume and positive sentiment may boost the score; pending negative legislation may reduce it). Use the scoring to rank assets.

Generate suggestions: For the top‑scoring assets, retrieve current price and key metrics via Finnhub or Alpha Vantage. Create a record containing the ticker, asset type, rationale (e.g., “multiple senators bought this stock last week and sentiment is positive”), suggested action (buy, watch, avoid) and a timestamp.

Save to Supabase – Upsert suggestions into a recommendations table. This will allow later analysis of performance.

Notify yourself: Use Email, Telegram or Slack nodes to send yourself a digest of recommendations. You can also add a front‑end (Next.js or Supabase’s built‑in table editor) to view suggestions.

6. Track and Evaluate Performance

Periodic evaluation: Create another scheduled workflow (e.g., weekly) to fetch historical price data for each suggested asset using Finnhub’s historical market data API (30‑year history, updated in real time
finnhub.io
). Calculate returns since the suggestion date, adjust for dividends and record results in a performance table.

Dashboard: Use Supabase’s API or a simple BI tool (Grafana, Metabase) to build dashboards that show cumulative returns, hit rate (percentage of suggestions that were profitable), distribution by sector, and correlation with legislative events. Real‑time updates can be enabled using Supabase’s realtime features.

Recommended n8n Nodes and Their Roles

Schedule Trigger – runs the workflow on a cron schedule. You can configure daily/hourly frequencies.

HTTP Request – core node for API calls. Use it to fetch data from FMP, Finnhub, Sunlight Congress, News API and Reddit. For websites without an API (press releases), call Firecrawl.

Set / Merge / Move – manipulate and format the JSON data fields returned by the APIs.

Function – write custom JavaScript to normalize data, compute scores, link trades to bills, or calculate returns.

If / Switch / Boolean – branch logic based on conditions (e.g., skip if data already exists in Supabase, or send notifications only for high‑scoring assets).

Supabase – insert, update or query records in your Supabase database. It manages credentials and allows you to build dynamic queries.

OpenAI / AI agent nodes – summarise large bodies of text (news articles, press releases) or classify sentiment. N8N supports connecting to GPT‑4 and similar models to build AI‑driven flows.

Email / Slack / Telegram – send notifications or reports.

Best Practices and Notes

Start small: Build and test each component separately (trade scraping, bill retrieval, news scraping) before connecting them. Review logs and handle errors gracefully; n8n lets you branch off and log errors if a node fails.

Respect rate limits and terms: Many APIs, especially Reddit and Sunlight, have rate limits and require API keys
datahen.com
sunlightlabs.github.io
. Implement delays or use the Wait node between calls.

Data quality: Government disclosure forms can be delayed or incomplete. Cross‑validate data from multiple sources (e.g., FMP and Finnhub) where possible.

Privacy and ethics: Avoid scraping private data or personal information. Use only publicly available data and respect API terms of service. Consider whether using this data for trading aligns with your ethical standards.

No guarantees: Even if official trades precede legislative actions, markets are complex. This system should augment, not replace, thorough research and diversification strategies.

Conclusion

By combining public data on congressional trades, legislative activity, news coverage and social‑media sentiment, you can build a robust n8n workflow that continuously collects, enriches and stores data in Supabase. Using n8n’s flexible nodes—HTTP requests, function nodes, AI integrations and Supabase connectors—you can automate the generation of investment suggestions and monitor their performance. Remember that this system provides informational insights, not guaranteed returns; always consider legal and ethical implications before acting on the output.

In summary, the report outlines an automated n8n workflow for gathering public data on congressional trades, legislative activities, news and social-media sentiment. It recommends specific APIs—such as FMP for Senate and House trading disclosures, Sunlight Congress for bills and votes
sunlightlabs.github.io
, Finnhub for market and alternative data
finnhub.io
, News API for articles
newsapi.org
, and Reddit’s API for discussion metrics
datahen.com
. Supabase is suggested as an affordable, scalable database to store and analyze the collected information.

The workflow design involves using n8n’s Schedule Trigger to fetch data at varying frequencies, HTTP Request nodes to pull from each source, Function nodes for normalization and scoring, and the Supabase node for persistence. It describes linking trades to relevant legislation, applying sentiment analysis, and ranking assets based on multiple factors. Finally, it stresses the ethical considerations of using such data and warns that the system offers informational insights rather than guaranteed investment returns.

Feel free to ask if you need anything else.

1. Ingestion Layer
Architecture

n8n Scheduled Trigger: Run daily (or multiple times per day for news/social).

HTTP Request Nodes: Call APIs directly (Quiver Quant, Capitol Trades, ProPublica Congress API, NewsAPI, Reddit API, Twitter/X API).

Function Nodes: Normalize raw responses into a common schema:

{
  "source": "quiver_quant",
  "entity": "AAPL",
  "entity_type": "stock",
  "action": "buy",
  "actor": "Rep. John Doe",
  "timestamp": "2025-09-30T14:00Z",
  "raw_text": "...original snippet..."
}


Supabase Node: Write all normalized events into tables (events, actors, entities, suggestions).

Storage Model

events: every raw event (trade, bill, tweet, news mention).

signals: derived heuristics (e.g., “+3 sentiment for AAPL”).

suggestions: final outputs with confidence + rationale.

2. Heuristic Rules Per Source

Here are rules you can encode as Function nodes or pass through an LLM scoring step:

Politician Trades (Quiver Quant, Capitol Trades)

Buy > $50k → +3 positive signal for ticker.

Sell > $50k → -3 negative signal for ticker.

Options purchases → +2 signal (extra risk tolerance implies conviction).

Multiple politicians in short window → amplify score by ×1.5.

Legislative Data (ProPublica Congress API)

Bill provides subsidy to sector (e.g., renewables) → +2 to sector ETFs.

Bill proposes regulation/harm to sector → -2 to sector ETFs.

Cosponsor count > 50 → amplify sector signal ×1.3 (higher probability of passing).

Social Media / Public Statements (X/Twitter, Press Releases)

Positive mention of company/sector → +1 sentiment.

Negative mention (slander/criticism) → -1 sentiment.

High engagement (>10k likes or retweets) → amplify score ×1.5.

News Articles (NewsAPI, Reddit discussion)

Articles mentioning company/sector with positive sentiment → +1.

Negative sentiment → -1.

Trending on Reddit finance subs (WSB, investing) → +2 if ticker appears in >5 trending posts/day.

Crypto-specific

If politicians mention crypto/blockchain positively → +1 for BTC/ETH.

Proposed restrictive legislation → -2 across crypto basket.

3. Suggestion Generation Layer

You now need a systematic way to convert signals → suggestions. Two approaches:

A. Rule-Based Scoring (Baseline)

Aggregate all signals for each entity (ticker/ETF/crypto).

Weight by recency (e.g., decay score by 50% per 7 days).

Thresholds:

Score ≥ +4 → Suggest “Long” (buy/hold).

Score ≤ -4 → Suggest “Short” (sell/avoid).

Between -3 and +3 → “Watch”.

This can be implemented in an n8n Function node with simple JS.

B. LLM-Enhanced Reasoning (Optional Upgrade)

Feed entity-specific events into an LLM (GPT or open-source model):

System: You are an investment signal generator. 
Input: [list of events for ticker X in the past 14 days]
Task: Summarize whether the overall signal is Long, Short, or Watch, with reasoning.


LLM outputs a suggestion object:

{
  "ticker": "AAPL",
  "recommendation": "Long",
  "confidence": 0.78,
  "reasoning": "Multiple congressional buys, positive legislation, no negative press."
}


Store in suggestions table in Supabase.

4. Feedback & Tracking

After generating suggestions, track market performance:

Daily price change of tickers in suggestions.

Compare against “expected direction”.

Write performance logs back into Supabase for historical accuracy tracking.

5. Alerts

Use n8n’s Email / Telegram / Discord / Slack nodes.

Send only when:

New suggestion hits “Long” or “Short”.

Confidence ≥ 0.7 (if using LLM).

✅ End result:

You have robust ingestion of multiple public sources.

Heuristics translate raw events into signals.

Either a scoring system or LLM decides actionable investment suggestions.

Everything is logged and tracked in Supabase.

Do you want me to write out the database schema for Supabase (tables + fields + relationships) so you can drop it in directly? That would give you a ready-to-go structure to store events, signals, and suggestions.

You said:
Yes
ChatGPT said:

Perfect — here’s a Supabase schema design for your pipeline. It covers raw events, derived signals, investment suggestions, and tracking performance over time.

1. Tables Overview

You’ll need five main tables:

events → all raw ingested data (politician trades, tweets, bills, news, etc.).

actors → metadata about the people/entities behind events (politicians, journalists, orgs).

entities → the financial instrument (stock, ETF, option, crypto).

signals → normalized heuristic outputs (positive/negative scores per entity).

suggestions → aggregated recommendations (“long/short/watch”) with reasoning.

performance_logs → track actual outcomes of suggestions.

2. Table Schemas
events

Stores every raw item before heuristic processing.

create table events (
  id uuid primary key default gen_random_uuid(),
  source text not null,             -- e.g., 'quiver_quant', 'newsapi'
  raw_text text,                    -- original API text
  entity_id uuid references entities(id),
  actor_id uuid references actors(id),
  action text,                      -- e.g., 'buy', 'sell', 'tweet', 'bill_proposed'
  metadata jsonb,                   -- structured API response
  timestamp timestamptz not null
);

actors

Metadata on who generated the event.

create table actors (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  role text,                        -- 'Senator', 'Representative', 'Journalist'
  party text,
  state text,
  metadata jsonb
);

entities

Financial instruments referenced in events.

create table entities (
  id uuid primary key default gen_random_uuid(),
  symbol text not null,             -- e.g., 'AAPL', 'SPY', 'BTC'
  type text not null,               -- 'stock', 'etf', 'option', 'crypto'
  sector text,
  metadata jsonb
);

signals

Derived heuristics attached to events/entities.

create table signals (
  id uuid primary key default gen_random_uuid(),
  event_id uuid references events(id),
  entity_id uuid references entities(id),
  score numeric not null,           -- +3, -2, etc.
  reasoning text,                   -- why the score was assigned
  created_at timestamptz default now()
);

suggestions

Aggregated signals → recommendation.

create table suggestions (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid references entities(id),
  recommendation text not null,     -- 'long', 'short', 'watch'
  confidence numeric,               -- 0–1
  reasoning text,
  created_at timestamptz default now()
);

performance_logs

Tracks how each suggestion performed.

create table performance_logs (
  id uuid primary key default gen_random_uuid(),
  suggestion_id uuid references suggestions(id),
  entity_id uuid references entities(id),
  date date not null,
  price_open numeric,
  price_close numeric,
  return_percent numeric,
  outcome text,                     -- 'hit', 'miss', 'neutral'
  created_at timestamptz default now()
);

3. Data Flow in Supabase

Raw event ingestion → stored in events.

Actor resolution → link politician/company to actors.

Entity resolution → link stock/ETF/crypto to entities.

Heuristic scoring → stored in signals.

Aggregation/LLM reasoning → stored in suggestions.

Daily market data sync (e.g., Alpha Vantage API) → update performance_logs.

4. Indexing & Performance Tips

Add an index on events.timestamp for time-based queries.

Add an index on signals.entity_id for quick aggregation.

Partition performance_logs by month if dataset grows large.

5. Optional Enhancements

Add a tags array field in events for easy querying (e.g., ['climate_bill', 'energy_sector']).

Create Supabase Row-Level Security (RLS) so you can safely share data later if you productize this.

👉 Question for you:
Do you want me to also generate the n8n workflows that push normalized data into these exact Supabase tables (e.g., sample HTTP → Function → Supabase node chains)? That way you can plug-and-play ingestion flows into this schema.

You said:
Yes
ChatGPT said:

Alright — let’s wire this up. I’ll give you sample n8n workflow blueprints (pseudocode-style) that match directly to the Supabase schema I designed. You’ll be able to duplicate these patterns for each source (trades, social, news, etc.).

Concrete free data sources & how to ingest them

House disclosures (Periodic Transaction Reports, PTRs)

Where: Clerk of the House disclosures site (PDF listing + PTR PDFs). They publish PTR PDFs per member and a raw feed. Example PTR PDF file is published as PDF. 
House Disclosures
+1

How to ingest:

Scrape the Clerk disclosures index with an HTTP Request node in n8n to get the latest PDF URLs.

Download PDFs and send them to a microservice (container) that parses PDFs (pdfplumber / tabula-py / camelot). Many PTR PDFs are structured tables — you can extract rows reliably.

Store extracted rows as events in Supabase.

Notes:

Values are often ranges (e.g., $50,001 - $100,000) — use range→score mapping in heuristics (see below).

Frequency: daily poll for new PTR files.

Senate disclosures / Ethics Office

Where: Senate Office of Public Records / Ethics page (financial disclosure access). 
U.S. Senate Ethics Committee

How to ingest:

Similar to House: scrape index, download PDF or data, parse tables.

Some Senate filings are also HTML or machine-friendly — sniff for CSV/JSON endpoints.

Legislation & bill text: ProPublica Congress API (free)

Where: ProPublica Congress API (bills, sponsors, cosponsors, votes). 
ProPublica

How to ingest:

n8n HTTP Request node with your ProPublica API key (they give free API keys).

Index bills by sponsor / keywords (e.g., search for "renewable", "telecom", or company names).

Normalize sponsor → actor, mapping to your entities (sector ETFs, tickers) as necessary.

News / global events: GDELT + RSS

Where: GDELT feed (mass news corpus) + targeted RSS from Reuters, WSJ feed endpoints, Google News scraping (careful).

How to ingest:

Use GDELT’s CSV/JSON dumps or BigQuery (if you want scale) to extract mentions of company names / tickers / politicians. 
GDELT Project

For smaller scale, scrape news RSS and run a local sentiment classifier.

Social media

X/Twitter: use snscrape (open-source CLI/Python) to fetch tweets for politician accounts and ticker mentions. It avoids the official paid API but is a scraper, so it can break. 
RapidSeedbox

Reddit: use Reddit’s official API for subreddits (p-limit) or selective scraping (avoid relying on Pushshift). Pushshift has reliability issues / access restrictions. 
Reddit

How to ingest:

Create a Python microservice that runs snscrape queries (e.g., snscrape twitter-search "from:SenatorX OR $AAPL since:2025-09-01 until:2025-09-30") and sends JSON back to n8n via webhook or writes to Supabase directly.

For Reddit, use OAuth app credentials and pull /r/investing, /r/wallstreetbets, /r/politics posts mentioning tickers or politicians.

Market prices (free-ish)

Alpha Vantage — free API key (limited rate). Good for daily prices. 
Alpha Vantage

yfinance — scrapes Yahoo Finance; no paid API required. Use within your Python microservice to pull OHLC and close prices.

How to ingest:

n8n schedules a daily price fetch (calls microservice or Alpha Vantage directly) and writes price snapshots to Supabase for performance_logs.

Other free trackers / aggregators

There are independent projects that mirror congress trades — useful as cross-checks, not primary sources. Use them for validation only (do not rely as sole source). Example: open trackers / GitHub projects / news sites. (Use caution; some are scraped themselves.)

Engineering pattern (how to tie all this together without paying for services)

You’ll follow a hybrid approach: n8n schedules and orchestrates, but actual scraping/parsing runs in small Docker microservices (Python/Node) you host (cheap VPS / Railway free tier / your laptop).

Architecture blueprint:

n8n (cron) → HTTP Request to your scraping microservice / or direct HTTP fetch when simple → microservice scrapes/parses → returns normalized JSON to n8n → n8n inserts into Supabase (events).

For heavy parsing (PDFs), microservice writes directly to Supabase via REST or calls a n8n webhook with results.

Use Redis / Supabase flags to mark processed files.

Why microservices? Because n8n is great as orchestrator but not for heavy PDF parsing, long scrapes, captcha resolution, or advanced error handling.

Parsing PTR PDFs: practical tips

Tools: pdfplumber, camelot, or tabula-py. If table parsing fails, fallback to pdfminer.six raw text + robust regex.

Typical approach:

Extract table rows.

Normalize ticker/company name to ticker symbol (use fuzzy matching against a local ticker mapping or yfinance lookup).

Parse amount ranges into numeric bands (e.g., <=1k, 1k-15k, 15k-50k, 50k-100k, 100k-250k, >250k).

Save as events (with amount_range and amount_bucket).

Example: when amount is "$50,001 - $100,000", map to bucket=50k-100k.

Heuristics refinement for PTR ranges (since amounts are ranges)

Use this mapping to generate scores (example):

< $1,000 → score 0 (noise)

$1k - $15k → +1 for buys, -1 for sells

$15k - $50k → +2 / -2

$50k - $100k → +3 / -3

$100k - $250k → +4 / -4

> $250k → +5 / -5

Amplify score if:

Multiple lawmakers buy same ticker within X days → multiply score ×1.5.

Politician is on committee relevant to the company (e.g., finance committee & bank stock) → +0.5 boost.

Store both raw bucket and numeric score in signals.

Practical scraping advice & anti-blocking

Rotate user agents and use a small proxy pool (residential proxies if you scale). Expect blocks for heavy scraping on X/major news sites.

Respect robots.txt when possible; scraping public PDFs and gov sites is low risk. Scraping private sites or bypassing paywalls is higher risk.

Cache aggressively: once you parsed a PTR PDF URL, don’t re-download it for 24h.

Legal / ethical caution (say it like it is)

Public data is fair game, but scraping private platforms violates their ToS and can get your IP banned or your scripts blocked. Scraping X with snscrape is common, but it’s still an unofficial approach and can break at any time. Keep legal counsel in mind if you plan to commercialize.

Do not present the system as financial advice. Log the legal disclaimers and retain provenance for every suggestion.

How to stitch it into your n8n + MCP plan

n8n: Orchestrate + schedule crawlers and calls to your microservices.

Microservices (hosted cheap): implement

PTR PDF downloader + parser

Senate disclosure HTML crawler

snscrape runner (Twitter/X)

Reddit fetcher (official API)

market price fetcher (Alpha Vantage / yfinance)

Supabase: central store (events → signals → suggestions → performance).

MCP/LLM: call db.query_signals(...), then rules.apply(...) and then LLM for final reasoning (as we planned).

Quick checklist / first-week roadmap (what to build first)

Prototype PTR ingestion:

Build a Python script to download a single PTR PDF and parse it to JSON. (Use pdfplumber/camelot). Save to Supabase. Validate extracted rows. 
House Disclosures

Wire n8n to call that script for the Clerk index page daily.

Add ProPublica bill ingestion in n8n and test mapping sponsor → sector. 
ProPublica

Add snscrape for 3–5 politician accounts + tickers. Log results. 
RapidSeedbox

Add Alpha Vantage/yfinance price fetcher; store daily closes. 
Alpha Vantage

Implement signal scoring function (JS) in n8n and write to signals.

Implement suggestion aggregator (LLM optional) — start rule-based first, then add LLM + MCP.

Sources I used (important references)

Clerk of the House disclosures / PTR PDFs. 
House Disclosures
+1

ProPublica Congress API docs. 
ProPublica

snscrape (open-source social scraper for Twitter/X etc.). 
RapidSeedbox

GDELT for news/event ingestion. 
GDELT Project

Alpha Vantage free market API / yfinance for price data. 
Alpha Vantage

Final blunt advice

You can avoid paying subscription aggregators — but you’ll pay in engineering time to parse PDFs, maintain scrapers, and handle edge cases. If you enjoy that work (you’re a pro), great. If not, you may ultimately pay for convenience.

Start with the House Clerk PTRs + ProPublica + snscrape + Alpha Vantage. That combo gives you most of the signal without recurring data fees.