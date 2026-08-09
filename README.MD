# SentiFeed

A real-time financial news sentiment analysis platform that aggregates institutional news, retail social sentiment, SEC filings, and unusual trading volume data to surface early market signals.

---

## What It Does

SentiFeed runs a multi-source data pipeline that:

1. **Fetches financial news** from approved wire services (Reuters, Dow Jones Newswires, ACCESS Newswire, GlobeNewswire, PR Newswire, MarketWatch, Stocktwits) via the TradingView news mediator API
2. **Processes SEC filings** — 8-K material events, Form 4 insider transactions, and 13-D activist disclosures
3. **Scores articles** using Claude Haiku (Anthropic) to assign per-ticker sentiment scores and prominence weights
4. **Detects social herd behavior** by polling the Stocktwits stream for retail trader sentiment and herd keyword activity
5. **Screens for unusual volume** via the Finviz Elite API, applying tiered relative volume thresholds by market cap
6. **Stores signals** — social spikes, insider buying clusters, and unusual volume events — with timestamps for lead-time analysis

The dashboard visualizes all of this in real time across four tabs: News Feed, Ticker Chart, Social Feed, and Trader Zone.

---

## Key Features

- **Multi-signal scoring (0-6)** per ticker: news sentiment, social bullish%, herd spike, unusual volume, insider cluster, squeeze setup
- **Composite ranking** using `sentiment x log(1 + density)` -- rewards both signal quality and article volume
- **Market cap tiered volume thresholds** -- large caps flagged at lower relative volume than microcaps
- **Sentiment velocity** arrows showing whether a ticker's news sentiment is improving or declining within the window
- **Custom date range** on the Ticker Chart, Trader Zone, and Company Deep Dive, for historical sentiment/price overlay analysis
- **Social Feed tab** showing Stocktwits bullish/bearish ratio and herd keyword detection per ticker
- **Watchlist with a signal-decay exit rule** -- flagging a ticker freezes its six-factor score at that moment; the tool later compares the frozen score against the live one and reports HOLD, TAKE PROFIT, or CUT. The exit is symmetric with the entry: a position closes when the signals that justified it stop agreeing, not at a manually chosen price target. See "Exit Strategy" below.

---

## Architecture

```
pipeline.py                     # Data pipeline — runs independently
│
├── Lane 1: Data Acquisition    # TradingView API, SEC EDGAR feeds, FDA RSS
├── Lane 2: Storage             # SQLite (articles.db)
├── Lane 3: Filtering           # Keyword filter, ticker matching, English filter
├── Lane 4: Article Retrieval   # Body fetch, Form 4 XML parsing
├── Lane 5: Sentiment Scoring   # Claude Haiku API — per-ticker score + prominence
└── Lane 6: Signal Detection    # Stocktwits stream, Finviz unusual volume, Form 4 clusters

app.py                          # Streamlit dashboard (main entry point)
├── News Feed                   # Filterable article list with AI breakdown
├── Ticker Chart                # Sentiment + density + social + price overlay
├── Social Feed                 # Live Stocktwits herd sentiment per ticker
├── Watchlist                   # Tickers flagged from Company Deep Dive, with
│                                # a signal-decay exit verdict (see below)
└── Trader Zone                 # Link to the pop-out screener

pages/trader_zone.py            # Standalone Trader Zone page
├── Ranked, paginated ticker list  # Composite/score/density/herd sort,
│                                  # market cap and volume filters
└── Auto-run pipeline           # Runs the pipeline on a timer while the
                                 # page is open, tracked in the database
                                 # so it survives redeploys and page reloads

pages/company_deep_dive.py      # Per-ticker page: price chart, six-factor
                                 # signal breakdown, fundamentals, recent
                                 # articles, and the "Add to watchlist" form
```

---

## Data Sources

| Source | Type | What We Get |
|--------|------|-------------|
| TradingView News Mediator API | News | Articles from approved wire services |
| SEC EDGAR | Filings | 8-K, Form 4, 13-D |
| FDA RSS | News | Drug approvals, press releases |
| Stocktwits Public API | Social | Retail trader posts, bullish/bearish tags |
| Finviz Elite API | Screener | Unusual volume, short data, market cap tiers |
| yfinance | Price | Live price, intraday change, volume |

---

## Setup

### Prerequisites

- Python 3.11 (curl_cffi and libsql do not publish installable wheels for
  older or newer Python versions at time of writing; 3.9 and 3.13 have both
  been confirmed to fail the install)
- Finviz Elite account (optional -- for the unusual volume screener)
- Anthropic API key (optional -- for LLM sentiment scoring)

### Installation

```bash
git clone https://github.com/MattH12516/Financial-News-Sentiment-Scoring.git
cd Financial-News-Sentiment-Scoring
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```dotenv
ANTHROPIC_API_KEY="your-anthropic-api-key"
FINVIZ_EMAIL="your-finviz-elite-email"
FINVIZ_PASSWORD="your-finviz-elite-password"

# Optional -- only needed to run against the hosted Turso database instead of
# the local articles.db file. See "Database" below.
TURSO_DATABASE_URL="libsql://your-database.turso.io"
TURSO_AUTH_TOKEN="your-turso-auth-token"
```

**Note:** Finviz logs in automatically with the email/password above and pulls a fresh API key each run, since the static key regenerates periodically and cannot be hardcoded.

**No TradingView credentials are needed.** The news mediator endpoint is queried as an anonymous user (`user_prostatus=non_pro`), so no session cookie is required -- confirmed by running the pipeline with none set and getting the same article count.

**None of these are required to run the app.** Every credential above degrades gracefully when absent: the pipeline skips that data source and logs why, sentiment scoring is skipped if `ANTHROPIC_API_KEY` is missing, and the app runs against a local SQLite file if the Turso variables are not set. A machine with none of these configured can still clone the repo, install dependencies, and run the dashboard against the committed `articles.db`.

### Running Locally (Development)

```bash
# Run the pipeline first to populate data
python3 pipeline.py

# Start the dashboard
python3 -m streamlit run app.py
```

The dashboard will open at `http://localhost:8501`. The Trader Zone pop-out is at `http://localhost:8501/trader_zone`.

### Deploying to Railway (Public)

The live version is hosted on Railway at the URL provided separately.

To deploy your own instance:

1. Create a free account at [railway.app](https://railway.app)
2. Create a new project → Deploy from GitHub repo → select this repository
3. Add the following environment variables in Railway's dashboard under **Variables**:
   ```
   ANTHROPIC_API_KEY=your-key
   FINVIZ_EMAIL=your-finviz-elite-email
   FINVIZ_PASSWORD=your-finviz-elite-password
   TURSO_DATABASE_URL=libsql://your-database.turso.io
   TURSO_AUTH_TOKEN=your-turso-auth-token
   ```
4. Railway auto-detects the Python app and deploys.

**Note:** Railway's filesystem is ephemeral -- any data written to a local file resets on every redeploy. Without the Turso variables above, the app falls back to the committed `articles.db`, which resets to that snapshot on each deploy. With them set, both the deployed app and any local `python pipeline.py` runs read and write the same persistent database, so data survives redeploys and local runs stay in sync with what is shown live.

---

## Database

The app talks to Turso (hosted libSQL) when `TURSO_DATABASE_URL` and
`TURSO_AUTH_TOKEN` are set, and to a local SQLite file (`articles.db`)
otherwise -- `pipeline.get_db_connection()` decides which, and every other
file calls that function rather than opening a connection directly. The two
backends use the same schema and the same SQL throughout the app.

Turso exists because Railway's free tier has no persistent disk, so a local
file on a deployed instance is wiped on every redeploy. Running the pipeline
locally with the same Turso credentials writes to the same database the
deployed app reads, rather than syncing two separate files by hand.

| Table | Contents |
|-------|----------|
| `articles` | News articles with title, source, body, sentiment score |
| `ticker_mentions` | Per-ticker sentiment scores and prominence weights per article |
| `signals` | Social spikes, Form 4 clusters, unusual volume events with timestamps |
| `relvol_history` | One current relative-volume reading per ticker, refreshed each pipeline run |
| `watchlist` | Tickers flagged from Company Deep Dive, with the six-factor snapshot frozen at the time of adding |
| `meta` | Small key/value store -- currently holds the auto-run pipeline timer and the last-saved Trader Zone filter state |

The pipeline purges articles older than 7 days on every run, and their
`ticker_mentions` rows along with them -- a mention pointing at a deleted
article would otherwise show a sentiment score with no article behind it.

---

## Signal Scoring

Each ticker in the Trader Zone is scored 0–6 based on independent data sources:

| # | Signal | Condition |
|---|--------|-----------|
| 1 | News sentiment | LLM avg score > 0.3 across recent articles |
| 2 | Social bullish% | > 60% of tagged Stocktwits posts are bullish |
| 3 | Herd spike | 3+ posts containing herd keywords (whale, squeeze, calls, etc.) |
| 4 | Unusual volume | Appears on Finviz screener above tier-appropriate relvol threshold |
| 5 | Insider cluster | 3+ Form 4 filings for same ticker within 24 hours |
| 6 | Squeeze setup | Short float > 10% AND days to cover > 5 |

**Signal stages:**
- Pre-volume — signals have fired, unusual volume not yet confirmed
- Signal led Xh early — signals preceded volume appearance by X hours
- Simultaneous — signals and volume appeared at the same time

---

## Relative Volume Thresholds by Market Cap

| Tier | Market Cap | Min RelVol to Flag |
|------|-----------|-------------------|
| Mega | $200B+ | 1.3x |
| Large | $10B–$200B | 1.5x |
| Mid | $2B–$10B | 2.0x |
| Small | $300M–$2B | 3.0x |
| Micro | $50M–$300M | 5.0x |
| Nano | <$50M | 10.0x |

Large caps require far more capital to move their volume needle, so lower thresholds are appropriate.

---

## Project Structure

```
sentifeed/
├── app.py                    # Streamlit dashboard
├── pipeline.py               # Data pipeline
├── pages/
│   └── trader_zone.py        # Trader Zone pop-out page
├── company_tickers.json      # SEC ticker/CIK mapping (~10,000 tickers)
├── financial_keywords.csv    # Keywords for article relevance filtering
├── name_stopwords.csv        # Company name false positive prevention
├── ticker_stopwords.csv      # Common-word ticker disambiguation
├── requirements.txt          # Python dependencies
├── .env                      # API keys (not committed)
└── articles.db               # SQLite database (pre-populated)
```

---

## Approved News Sources

Articles are filtered to these sources only (allowlist approach):

- Reuters
- Dow Jones Newswires
- ACCESS Newswire
- GlobeNewswire
- FinanceWire
- PR Newswire
- MarketWatch
- Stocktwits (editorial)
- TradingView (editorial)

---

## Research Context

This project was built to investigate whether a multi-factor signal aggregation system — combining institutional news sentiment, retail social behavior, SEC insider filings, and unusual market volume — can identify pre-conditions that statistically precede abnormal price moves.

The system does not predict stock prices. It identifies when multiple independent data sources simultaneously point in the same direction and surfaces those setups for human evaluation.

---

## Tech Stack

- **Python 3.11**
- **Streamlit** — dashboard framework
- **SQLite** — local database
- **Claude Haiku** (Anthropic) — LLM sentiment scoring
- **yfinance** — live price and volume data
- **Plotly** — interactive charts
- **trafilatura** — article body extraction
- **curl_cffi** — browser impersonation for TradingView fetching
- **feedparser** — SEC and FDA RSS parsing