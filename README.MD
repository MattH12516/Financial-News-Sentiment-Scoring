# SentiFeed ⚡

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

- **Multi-signal scoring (0–6)** per ticker: news sentiment, social bullish%, herd spike, unusual volume, insider cluster, squeeze setup
- **Composite ranking** using `sentiment × log(1 + density)` — rewards both signal quality and article volume
- **Pre-signal candidates** — tickers with firing signals not yet confirmed by unusual volume (early-stage watchlist)
- **Market cap tiered volume thresholds** — large caps flagged at lower relative volume than microcaps
- **Sentiment velocity** arrows showing whether a ticker's news sentiment is improving or declining within the window
- **Custom date range** on the Ticker Chart for historical sentiment/price overlay analysis
- **Social Feed tab** showing Stocktwits bullish/bearish ratio and herd keyword detection per ticker

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
├── Tab 1: News Feed            # Filterable article list with AI breakdown
├── Tab 2: Ticker Chart         # Sentiment + density + social + price overlay
├── Tab 3: Social Feed          # Live Stocktwits herd sentiment per ticker
└── Tab 4: Trader Zone          # Link to pop-out screener

pages/trader_zone.py            # Standalone Trader Zone page
├── Ranked ticker list          # Composite/score/density sort with cap filter
├── Unusual volume section      # Current Finviz unusual volume list
└── Pre-signal candidates       # Early-stage watchlist
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

- Python 3.9+
- Finviz Elite account (for unusual volume screener)
- Anthropic API key (for sentiment scoring)

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
FINVIZ_API_KEY="your-finviz-elite-api-key"
TRADINGVIEW_SESSIONID="your-tradingview-session-id"
TRADINGVIEW_SESSIONID_SIGN="your-tradingview-session-sign"
```

**Getting TradingView credentials:** Log into TradingView, open browser dev tools → Application → Cookies → copy `sessionid` and `sessionid_sign` values.

**Note:** The Finviz Elite API key regenerates periodically. Update it in `.env` when it does.

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
   FINVIZ_API_KEY=your-key
   TRADINGVIEW_SESSIONID=your-session-id
   TRADINGVIEW_SESSIONID_SIGN=your-session-sign
   ```
4. Add a `Procfile` in the project root containing:
   ```
   web: streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
   ```
5. Railway will auto-detect Python and deploy

**Note:** Railway's filesystem is ephemeral — the database resets on redeploy. The committed `articles.db` provides a pre-populated starting point. Use the "Run pipeline now" button in the dashboard to fetch fresh data after deployment.

---

## Database

The app uses SQLite (`articles.db`) with the following tables:

| Table | Contents |
|-------|----------|
| `articles` | News articles with title, source, body, sentiment score |
| `ticker_mentions` | Per-ticker sentiment scores and prominence weights per article |
| `signals` | Social spikes, Form 4 clusters, unusual volume events with timestamps |
| `relvol_history` | Relative volume snapshots over time for trend tracking |

The pipeline automatically purges articles older than 7 days and relvol history older than 30 days.

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

- **Python 3.9**
- **Streamlit** — dashboard framework
- **SQLite** — local database
- **Claude Haiku** (Anthropic) — LLM sentiment scoring
- **yfinance** — live price and volume data
- **Plotly** — interactive charts
- **trafilatura** — article body extraction
- **curl_cffi** — browser impersonation for TradingView fetching
- **feedparser** — SEC and FDA RSS parsing
