"""
pipeline.py -- SentiFeed data pipeline.

Fetches, filters, scores, and stores financial news from multiple sources.
Run independently from the dashboard to populate the database.

Lane 1  Data Acquisition      HTTP fetching with browser impersonation
Lane 2  Storage and Schema    SQLite database management
Lane 3  Filtering             Keyword filter, ticker matching, language detection
Lane 4  Article Retrieval     SEC form parsing, body extraction
Lane 5  Sentiment Scoring     LLM-based per-ticker sentiment via Claude Haiku
Lane 6  Signal Detection      Social herd, insider clusters, unusual volume

Usage:
    python3 pipeline.py

Dependencies:
    pip install -r requirements.txt
"""

from dotenv import load_dotenv
load_dotenv()



import os
import re
import csv
import io
import json
import time
import sqlite3
import calendar
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

import requests
import feedparser
import trafilatura
from collections import defaultdict


try:
    from curl_cffi import requests as creq
    _HAS_CURL = True
except ImportError:
    _HAS_CURL = False

try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 0
    _LANGDETECT = True
except ImportError:
    _LANGDETECT = False

try:
    import anthropic
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False


# ============================================================================
# CONFIG
# ============================================================================

DB_PATH               = "articles.db"
PIPELINE_LOCK         = "pipeline.lock"   # prevents overlapping auto-runs
TICKERS_PATH          = "company_tickers.json"
KEYWORDS_PATH         = "financial_keywords.csv"
MAX_TICKERS           = 50000
MAX_ARTICLE_AGE_HOURS = 72    # Articles older than this are skipped in RSS feeds
MAX_WORKERS           = 50    # Concurrent fetch threads for TradingView body fetches

# SEC requires a descriptive User-Agent identifying the requester
SEC_USER_AGENT = "SentiFeed matthunter0021@gmail.com"

# TradingView session credentials loaded from .env -- not hardcoded
TRADINGVIEW_SESSIONID      = os.getenv("TRADINGVIEW_SESSIONID", "")
TRADINGVIEW_SESSIONID_SIGN = os.getenv("TRADINGVIEW_SESSIONID_SIGN", "")

# Browser impersonation settings for sites that block automated requests
IMPERSONATE = "chrome"
BROWSER_UA  = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Per-domain rate limits in seconds between requests
DEFAULT_RATE_LIMIT = 0.5
RATE_LIMITS        = {
    "www.sec.gov": 0.2,
    "sec.gov":     0.2,
}
REQUEST_TIMEOUT = 15

# RSS/Atom feeds processed each pipeline run.
# is_fda=True routes to LLM-based ticker extraction (drug names, not company names).
FEEDS = [
    {
        "name": "SEC EDGAR 8-K",
        "url":  ("https://www.sec.gov/cgi-bin/browse-edgar"
                 "?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=40&output=atom"),
    },
    {
        "name":       "SEC Form 4",
        "url":        ("https://www.sec.gov/cgi-bin/browse-edgar"
                       "?action=getcurrent&type=4&company=&dateb=&owner=include&count=40&output=atom"),
        "fetch_body": True,
        "cik_only":   False,
    },
    {
        "name":       "SEC 13-D",
        "url":        ("https://www.sec.gov/cgi-bin/browse-edgar"
                       "?action=getcurrent&type=SC+13D&company=&dateb=&owner=include&count=40&output=atom"),
        "fetch_body": True,
        "cik_only":   False,
    },
    {
        "name":       "FDA Press Releases",
        "url":        "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml",
        "fetch_body": True,
        "cik_only":   False,
        "is_fda":     True,
    },
    {
        "name":       "FDA Drug News",
        "url":        "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/drugs/rss.xml",
        "fetch_body": True,
        "cik_only":   False,
        "is_fda":     True,
    },
]

# TradingView news mediator API -- returns structured JSON with provider, tickers, and links.
# Discovered via browser dev tools Network tab on the news-flow filtered page.
# Provider filter uses TradingView's internal provider identifiers.
TV_API_URL = (
    "https://news-mediator.tradingview.com/news-flow/v2/news"
    "?filter=lang%3Aen"
    "&filter=provider%3Aacceswire%2Cdow-jones%2Cfinancewire%2Cglobenewswire"
    "%2Cmarket-watch%2Cprnewswire%2Freuters%2Cstocktwits%2Ctradingview"
    "&client=screener&streaming=true&user_prostatus=non_pro"
)


# ============================================================================
# FINVIZ ELITE CONFIG
# ============================================================================

# Finviz Elite login credentials loaded from .env. The API key itself regenerates
# periodically, so instead of storing a static key we log in fresh each pipeline
# run and pull the current key directly from the account's API explanation page.
FINVIZ_EMAIL    = os.getenv("FINVIZ_EMAIL", "")
FINVIZ_PASSWORD = os.getenv("FINVIZ_PASSWORD", "")
# Fallback for local/manual testing if login credentials are not configured
FINVIZ_STATIC_KEY = os.getenv("FINVIZ_API_KEY", "")

_finviz_api_key_cache = None  # cached for the duration of a single pipeline run
def _finviz_validate_key(api_key):
    """Return True if this key returns a valid CSV export from the screener endpoint.
    Used to identify which UUID on the API page is the actual auth token."""
    test_url = (
        "https://elite.finviz.com/export/screener"
        "?v=111&f=sh_relvol_o1&rows=1"
        f"&auth={api_key}"
    )
    try:
        r = requests.get(
            test_url, headers={"User-Agent": BROWSER_UA}, timeout=REQUEST_TIMEOUT
        )
        if r.status_code != 200:
            return False
        first_line = r.text.split("\n")[0] if r.text else ""
        return "Ticker" in first_line
    except Exception:
        return False
    
def _finviz_login_and_fetch_key():
    """Log into Finviz using a persistent session (so all cookies set across the
    login flow -- not just .ASPXAUTH -- carry forward correctly) and pull the
    current API key from the account's api_explanation page.
    Returns the API key string, or None on failure."""
    if not FINVIZ_EMAIL or not FINVIZ_PASSWORD:
        print("   [Finviz login] FINVIZ_EMAIL/FINVIZ_PASSWORD not set")
        return None
    try:
        if _HAS_CURL:
            session = creq.Session(impersonate=IMPERSONATE)
        else:
            session = requests.Session()
            session.headers.update({"User-Agent": BROWSER_UA})

        # Load the login page first to pick up any session/CSRF cookies Finviz sets
        # before the login form is submitted.
        session.get("https://finviz.com/login-email?remember=true", timeout=REQUEST_TIMEOUT)

        resp = session.post(
            "https://finviz.com/login_submit",
            data={"email": FINVIZ_EMAIL, "password": FINVIZ_PASSWORD, "remember": "true"},
            timeout=REQUEST_TIMEOUT,
        )
        print(f"   [Finviz login] status={resp.status_code} using_curl_cffi={_HAS_CURL}")

        key_resp = session.get(
            "https://elite.finviz.com/api_explanation", timeout=REQUEST_TIMEOUT
        )
        print(f"   [Finviz api_explanation] status={key_resp.status_code} "
              f"len={len(key_resp.text)}")

        # The example URL containing "auth=" is rendered client-side by JavaScript,
        # so the raw HTML only contains the bare UUID. Collect every UUID on the
        # page and validate each against the real screener endpoint -- more robust
        # than assuming a fixed position, since the page may contain several.
        candidates = re.findall(
            r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
            key_resp.text
        )
        seen = []
        for cand in candidates:
            if cand in seen:
                continue
            seen.append(cand)
            if _finviz_validate_key(cand):
                return cand

        print(f"   [Finviz api_explanation] found {len(seen)} UUID candidate(s) but "
              "none returned valid screener data -- Elite subscription may be inactive")
        return None
    except Exception as e:
        print(f"   [Finviz login] error: {type(e).__name__}: {e}")
        return None

def get_finviz_api_key():
    """Return a current Finviz API key. Logs in fresh if credentials are configured,
    falling back to a static key from .env if login is not set up.
    Cached in-memory for the remainder of this pipeline run."""
    global _finviz_api_key_cache
    if _finviz_api_key_cache:
        return _finviz_api_key_cache

    if FINVIZ_EMAIL and FINVIZ_PASSWORD:
        key = _finviz_login_and_fetch_key()
        if key:
            print("   [Finviz] fresh API key obtained via login")
            _finviz_api_key_cache = key
            return key
        print("   [Finviz] login flow failed -- falling back to static key if set")


def _finviz_screener_url(api_key):
    """Build the screener export URL for the given API key."""
    return (
        "https://elite.finviz.com/export/screener"
        "?v=152"
        "&f=geo_usa"
        "&o=-relativevolume"
        "&rows=500"
        f"&auth={api_key}"
    )

# Relative volume thresholds by market cap tier.
# Large caps require far more capital to move the volume needle, so lower thresholds apply.
# A mega-cap at 1.3x relvol represents more actual dollar flow than a nano-cap at 10x.
FINVIZ_CAP_TIERS = [
    {"label": "mega",  "min_b": 200,  "relvol_min": 1.3},
    {"label": "large", "min_b": 10,   "relvol_min": 1.5},
    {"label": "mid",   "min_b": 2,    "relvol_min": 2.0},
    {"label": "small", "min_b": 0.3,  "relvol_min": 3.0},
    {"label": "micro", "min_b": 0.05, "relvol_min": 5.0},
    {"label": "nano",  "min_b": 0,    "relvol_min": 10.0},
]


def _cap_tier(mc_m):
    """Return (tier_label, display_label, relvol_min) for a market cap given in millions.
    Used to apply appropriate relative volume thresholds per company size."""
    if mc_m is None:
        return "unknown", "Unknown", 3.0
    mc_b   = mc_m / 1000
    labels = {
        "mega":  "Mega cap",
        "large": "Large cap",
        "mid":   "Mid cap",
        "small": "Small cap",
        "micro": "Micro cap",
        "nano":  "Nano cap",
    }
    for tier in FINVIZ_CAP_TIERS:
        if mc_b >= tier["min_b"]:
            return tier["label"], labels[tier["label"]], tier["relvol_min"]
    return "nano", "Nano cap", 10.0


# ============================================================================
# LANE 1 -- DATA ACQUISITION
# ============================================================================

# Track last request time per domain for rate limiting
_last_request  = {}
# If curl_cffi fails, fall back to standard requests for the rest of the session
_curl_disabled = False


def _rate_limit(url):
    """Enforce per-domain rate limiting to avoid overwhelming servers or getting blocked."""
    domain  = urlparse(url).netloc
    delay   = RATE_LIMITS.get(domain, DEFAULT_RATE_LIMIT)
    elapsed = time.time() - _last_request.get(domain, 0.0)
    if elapsed < delay:
        time.sleep(delay - elapsed)
    _last_request[domain] = time.time()


def fetch_raw(url, sec=False):
    """Fetch a URL with browser impersonation via curl_cffi, falling back to requests.
    SEC endpoints use a descriptive User-Agent as required by EDGAR Fair Access Policy.
    TradingView endpoints attach session cookies for authenticated content access."""
    global _curl_disabled
    _rate_limit(url)

    extra_headers = {}
    # Attach TradingView session cookies when fetching TV content
    if "tradingview.com" in urlparse(url).netloc and TRADINGVIEW_SESSIONID:
        extra_headers["Cookie"] = (
            f"sessionid={TRADINGVIEW_SESSIONID}; "
            f"sessionid_sign={TRADINGVIEW_SESSIONID_SIGN}"
        )

    # curl_cffi impersonates a real browser TLS fingerprint to bypass bot detection
    if _HAS_CURL and not _curl_disabled and not sec:
        try:
            r = creq.get(url, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT,
                         headers=extra_headers)
            if r.status_code == 200:
                return r
        except Exception as e:
            print(f"   [curl_cffi error: {type(e).__name__} -- falling back to requests]")
            _curl_disabled = True

    # Standard requests fallback -- always used for SEC endpoints
    headers = {"User-Agent": SEC_USER_AGENT if sec else BROWSER_UA}
    headers.update(extra_headers)
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"   fetch error for {url[:70]}: {e}")
        return None
    if r.status_code != 200:
        print(f"   HTTP {r.status_code} for {url[:70]}")
        return None
    return r


def fetch_feed(feed):
    """Fetch and parse an RSS/Atom feed. Returns list of feedparser entries."""
    sec  = "sec.gov" in feed["url"]
    resp = fetch_raw(feed["url"], sec=sec)
    if resp is None:
        return []
    return feedparser.parse(resp.content).entries


# ============================================================================
# LANE 2 -- STORAGE AND SCHEMA
# ============================================================================

def init_db(conn):
    """Create all required tables and indexes.
    Safe to call on an existing database -- missing columns are added via
    ALTER TABLE without destroying existing data."""

    # Core articles table -- one row per unique article URL
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            link                TEXT UNIQUE,
            title               TEXT,
            summary             TEXT,
            body                TEXT,
            source              TEXT,
            published           TEXT,
            ingested_at         TEXT,
            matched_tickers     TEXT,
            sec_form_type       TEXT,
            sentiment_score     REAL,
            sentiment_reasoning TEXT
        )
    """)

    # Add any columns missing from older database schemas
    expected = {
        "title": "TEXT", "summary": "TEXT", "body": "TEXT", "source": "TEXT",
        "published": "TEXT", "ingested_at": "TEXT", "matched_tickers": "TEXT",
        "sec_form_type": "TEXT", "sentiment_score": "REAL",
        "sentiment_reasoning": "TEXT", "form4_data": "TEXT",
        "keyword_passed": "INTEGER", "wire_source": "TEXT",
    }
    existing = {row[1] for row in conn.execute("PRAGMA table_info(articles)")}
    for col, decl in expected.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col} {decl}")
            print(f"[schema] added missing column: {col}")

    # Per-ticker mention table -- one row per ticker per article.
    # weight = LLM-assigned prominence (0-1), used for weighted density ranking.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_mentions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker       TEXT NOT NULL,
            article_id   INTEGER,
            mentioned_at TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mentions_ticker_time "
        "ON ticker_mentions(ticker, mentioned_at)"
    )

    mentions_existing = {row[1] for row in conn.execute("PRAGMA table_info(ticker_mentions)")}
    for col, decl in {
        "weight": "REAL DEFAULT 1.0",
        "score":  "REAL",
        "reasoning": "TEXT",
    }.items():
        if col not in mentions_existing:
            conn.execute(f"ALTER TABLE ticker_mentions ADD COLUMN {col} {decl}")
            print(f"[schema] added missing column to ticker_mentions: {col}")

    # Signals table -- timestamped events used for lead-time analysis.
    # signal_type values: social_spike, social_read, form4_cluster,
    #                     unusual_volume, unusual_volume_squeeze
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker       TEXT NOT NULL,
            signal_type  TEXT NOT NULL,
            signal_value REAL,
            post_count   INTEGER,
            bullish_pct  REAL,
            keyword_hits INTEGER,
            detected_at  TEXT NOT NULL,
            metadata     TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_ticker_time "
        "ON signals(ticker, detected_at)"
    )

    # Relative volume history -- snapshots relvol per pipeline run for trend tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relvol_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            relvol      REAL,
            snapshot_at TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_relvol_ticker_time "
        "ON relvol_history(ticker, snapshot_at)"
    )

    # Watchlist -- one row per recorded pick. snapshot_json freezes the signal
    # state at the moment of adding so the pick can be judged later against
    # what was actually known at the time, not against current values.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT NOT NULL,
            added_at      TEXT NOT NULL,
            added_price   REAL,
            entry_price   REAL,
            target_price  REAL,
            stop_price    REAL,
            thesis        TEXT,
            snapshot_json TEXT,
            status        TEXT NOT NULL DEFAULT 'watching',
            closed_at     TEXT,
            closed_price  REAL,
            outcome_note  TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watchlist_ticker "
        "ON watchlist(ticker, status)"
    )

    # Add metadata column to signals if missing from older schema
    sig_cols = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
    if "metadata" not in sig_cols:
        conn.execute("ALTER TABLE signals ADD COLUMN metadata TEXT")
        print("[schema] added missing column to signals: metadata")
    if "bullish_count" not in sig_cols:
        conn.execute("ALTER TABLE signals ADD COLUMN bullish_count INTEGER")
        print("[schema] added missing column to signals: bullish_count")
    if "bearish_count" not in sig_cols:
        conn.execute("ALTER TABLE signals ADD COLUMN bearish_count INTEGER")
        print("[schema] added missing column to signals: bearish_count")

    conn.commit()


def article_exists(conn, link):
    """Return True if this URL is already stored in the database."""
    return conn.execute(
        "SELECT 1 FROM articles WHERE link = ? LIMIT 1", (link,)
    ).fetchone() is not None
def _published_to_iso(entry):
    """Return an RSS/Atom entry's publish time as a UTC ISO string, or None.
    feedparser's parsed time struct is more reliable than the raw string,
    which varies in format across feeds."""
    try:
        pub = entry.get("published_parsed") or entry.get("updated_parsed")
        if pub:
            return datetime.fromtimestamp(
                calendar.timegm(pub), tz=timezone.utc
            ).isoformat()
    except Exception:
        pass
    return None


def _event_time(published, ingested_at):
    """Timestamp used for all timeline analysis -- charts, density buckets,
    and Trader Zone windows.

    Uses the article's actual publish time when available so a news spike lands
    at the moment the news broke, not the moment we happened to fetch it.
    Falls back to ingest time when no usable publish date exists.

    Rejects timestamps in the future or absurdly far in the past, which some
    feeds emit due to timezone errors -- those would otherwise plot ahead of
    real time on the chart."""
    if published:
        try:
            dt = datetime.fromisoformat(published)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if timedelta(0) <= (now - dt) <= timedelta(days=365):
                return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return ingested_at

def save_article(conn, link, title, summary, body, source, published,
                 tickers, form_type, scores, form4_data=None,
                 keyword_passed=1, wire_source=None, weights=None):
    """Insert an article and its per-ticker mention rows.
    Uses INSERT OR IGNORE so duplicate URLs are silently skipped.
    scores:  dict of {ticker: {score, reasoning, prominence}} from LLM.
    weights: dict of {ticker: prominence_float} for density weighting."""
    weights = weights or {}

    # Use the first ticker's score as the article-level sentiment for quick display
    score_val, reasoning_val = None, None
    if scores:
        first = next(iter(scores.values()))
        score_val    = first.get("score")
        reasoning_val = first.get("reasoning")

    ingested_at = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("""
        INSERT OR IGNORE INTO articles
        (link, title, summary, body, source, published, ingested_at,
         matched_tickers, sec_form_type, sentiment_score, sentiment_reasoning,
         form4_data, keyword_passed, wire_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        link, title, summary, body, source, published, ingested_at,
        ",".join(sorted(tickers)) if tickers else "",
        form_type, score_val, reasoning_val,
        json.dumps(form4_data) if form4_data else None,
        keyword_passed, wire_source,
    ))

    # Only write ticker mentions for new articles (rowcount=1 means insert succeeded)
    if cur.rowcount == 1 and tickers:
        article_id = cur.lastrowid
        # Timeline analysis keys off when the news actually broke, not when we
        # fetched it -- otherwise a catch-up run collapses hours of articles
        # into one artificial spike.
        event_at = _event_time(published, ingested_at)
        for tk in tickers:
            tk_info = scores.get(tk, {})
            conn.execute(
                "INSERT INTO ticker_mentions "
                "(ticker, article_id, mentioned_at, weight, score, reasoning) "
                "VALUES (?,?,?,?,?,?)",
                (tk, article_id, event_at, weights.get(tk, 1.0),
                 tk_info.get("score"), tk_info.get("reasoning"))
            )


def purge_old_articles(conn, days=7):
    """Remove articles older than the retention window to keep the database lean."""
    cutoff  = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    deleted = conn.execute(
        "DELETE FROM articles WHERE ingested_at < ?", (cutoff,)
    ).rowcount
    conn.commit()
    if deleted:
        print(f"[purge] removed {deleted} articles older than {days} days")


# ============================================================================
# LANE 3 -- FILTERING AND TICKER MATCHING
# ============================================================================

def load_keywords(path):
    """Load financial keywords from CSV. Articles must contain at least one keyword
    to pass the filter (unless they already have a ticker match)."""
    kws = set()
    if not os.path.exists(path):
        print(f"[keywords] {path} not found -- keyword filter DISABLED (pass-through).")
        return kws
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if row and row[0].strip():
                kws.add(row[0].strip().lower())
    print(f"[keywords] loaded {len(kws)} financial keywords")
    return kws


def passes_keyword_filter(text, keywords):
    """Return True if text contains at least one financial keyword.
    Returns True unconditionally if keyword list is empty (filter disabled)."""
    if not keywords:
        return True
    low = text.lower()
    return any(k in low for k in keywords)


def is_english(text):
    """Return True if text is detected as English, or if langdetect is unavailable.
    Only checks the first 600 characters for speed."""
    if not _LANGDETECT:
        return True
    try:
        return detect(text[:600]) == "en"
    except Exception:
        return True


def is_recent(entry, max_hours=MAX_ARTICLE_AGE_HOURS):
    """Return True if the feed entry's published date is within the age cutoff.
    Returns True if no published date is available (fail open)."""
    try:
        pub = entry.get("published_parsed")
        if not pub:
            return True
        import time as _time
        return (_time.time() - _time.mktime(pub)) / 3600 <= max_hours
    except Exception:
        return True


# Regex to extract SEC CIK numbers from filing titles (7-10 digits in parentheses)
_CIK_RE = re.compile(r"\((\d{7,10})\)")

# Regex to strip legal entity suffixes before company name matching
_SUFFIX_RE = re.compile(
    r'[,\s]+(?:incorporated|corporation|company|limited|inc|corp|ltd|llc|plc|'
    r'n\.v\.|nv|s\.a\.|sa|ag|se|ab|co)\s*\.?\s*$',
    re.IGNORECASE
)

# Manual aliases for tickers whose legal names differ significantly from common usage
_ALIASES = {
    "GOOGL": ["Google", "Alphabet"],
    "GOOG":  ["Google", "Alphabet"],
    "META":  ["Facebook", "Meta"],
    "BRK-B": ["Berkshire"],
    "BRK-A": ["Berkshire"],
}

# Stopwords loaded at module level for fast in-memory lookup
_NAME_STOPWORDS   = set()
_TICKER_STOPWORDS = set()


def _load_stopwords():
    """Load name and ticker stopword lists from CSV files.
    Stopwords prevent common English words from being mistaken for tickers
    or company names (e.g. 'news' matching NWSA)."""
    global _NAME_STOPWORDS, _TICKER_STOPWORDS

    name_path   = "name_stopwords.csv"
    ticker_path = "ticker_stopwords.csv"

    if os.path.exists(name_path):
        with open(name_path, newline="", encoding="utf-8") as f:
            _NAME_STOPWORDS = {
                r[0].strip().lower() for r in csv.reader(f) if r and r[0].strip()
            }
    if os.path.exists(ticker_path):
        with open(ticker_path, newline="", encoding="utf-8") as f:
            _TICKER_STOPWORDS = {
                r[0].strip().upper() for r in csv.reader(f) if r and r[0].strip()
            }


_load_stopwords()


def load_watchlist(path=TICKERS_PATH, max_tickers=MAX_TICKERS):
    """Build ticker matching structures from the SEC company_tickers.json file.

    Returns a dict containing:
      tickers         -- set of all valid ticker strings
      cik_to_ticker   -- maps CIK number string to ticker
      name_to_ticker  -- maps lowercase company name to ticker
      name_pattern    -- compiled regex for company name matching
      ticker_pattern  -- compiled regex for ticker symbol matching
    """
    if not os.path.exists(path):
        print(f"[watchlist] {path} not found")
        return {"tickers": set(), "cik_to_ticker": {}, "name_to_ticker": {},
                "name_pattern": None, "ticker_pattern": None}

    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    tickers        = set()
    cik_to_ticker  = {}
    name_to_ticker = {}
    name_parts     = []

    entries = list(raw.values())[:max_tickers]
    for entry in entries:
        tk   = entry.get("ticker", "").strip().upper()
        cik  = str(entry.get("cik_str", "")).lstrip("0")
        name = entry.get("title", "").strip()
        if not tk:
            continue

        tickers.add(tk)
        if cik:
            cik_to_ticker[cik] = tk

        # Strip legal suffixes for cleaner name matching
        clean_name = _SUFFIX_RE.sub("", name).strip()
        if len(clean_name) >= 3:
            name_to_ticker[clean_name.lower()] = tk
            name_parts.append(re.escape(clean_name))

        # Add manual aliases (e.g. "Google" maps to GOOGL)
        for alias in _ALIASES.get(tk, []):
            name_to_ticker[alias.lower()] = tk
            name_parts.append(re.escape(alias))

    # Single compiled alternation pattern is much faster than iterating
    # over thousands of names individually for each article
    name_pattern = (
        re.compile(r'\b(' + "|".join(name_parts) + r')\b')
        if name_parts else None
    )

    # Ticker pattern -- case-sensitive, word-boundary, 1-5 chars
    all_tickers_esc = [re.escape(tk) for tk in sorted(tickers, key=len, reverse=True)]
    ticker_pattern  = (
        re.compile(r'\b(' + "|".join(all_tickers_esc) + r')\b')
        if all_tickers_esc else None
    )

    print(f"[watchlist] {len(tickers)} tickers, {len(name_to_ticker)} company names loaded")
    return {
        "tickers":        tickers,
        "cik_to_ticker":  cik_to_ticker,
        "name_to_ticker": name_to_ticker,
        "name_pattern":   name_pattern,
        "ticker_pattern": ticker_pattern,
    }


def _explicit_ticker_format(tk, text):
    """Return True if ticker appears in an unambiguous format: $TICK, [TICK], (TICK).
    Used to confirm ambiguous common-word tickers (e.g. FORM, ANY, OPEN)."""
    t = re.escape(tk)
    patterns = [rf'\${t}\b', rf'\[{t}\]', rf'\({t}\)', rf'[-:]{t}\b']
    return any(re.search(p, text) for p in patterns)


def match_tickers(text, wl, cik_only=False):
    """Extract recognized ticker symbols from text using three strategies:
    (a) SEC CIK numbers in parentheses -- zero false positives
    (b) Company name matching against the SEC watchlist
    (c) Ticker symbol matching with stopword filtering for ambiguous tickers"""
    matched = set()

    # (a) CIK numbers -- most reliable, used for SEC filing titles
    for cik in _CIK_RE.findall(text):
        cik_norm = cik.lstrip("0")
        if cik_norm in wl["cik_to_ticker"]:
            matched.add(wl["cik_to_ticker"][cik_norm])
    if cik_only:
        return matched

    # (b) Company name matching -- stopwords block false positives like "news" -> NWSA
    if wl.get("name_pattern"):
        for m in wl["name_pattern"].finditer(text):
            name = m.group(1)
            if name.lower() in _NAME_STOPWORDS:
                continue
            if name.lower() in wl["name_to_ticker"]:
                matched.add(wl["name_to_ticker"][name.lower()])

    # (c) Ticker symbol matching -- ambiguous tickers require explicit format or prior name match
    if wl.get("ticker_pattern"):
        for m in wl["ticker_pattern"].finditer(text):
            tk = m.group(1)
            if tk in _TICKER_STOPWORDS:
                if tk in matched or _explicit_ticker_format(tk, text):
                    matched.add(tk)
            else:
                matched.add(tk)

    return matched


# ============================================================================
# LANE 4 -- ARTICLE RETRIEVAL
# ============================================================================

def detect_form_type_from_title(title):
    """Extract SEC form type from a filing title string.
    Example: '4 - SentinelOne Inc (0001583708)' -> '4'"""
    m = re.match(r"\s*([A-Z0-9/\-]+)\s+-\s+", title or "")
    return m.group(1) if m else None


def fetch_article_body(url):
    """Fetch and extract the main text body of an article using trafilatura."""
    resp = fetch_raw(url)
    if resp is None:
        return None
    return trafilatura.extract(resp.text, include_comments=False, include_tables=False)


def fetch_tradingview_api():
    """Fetch articles from TradingView's internal news mediator API.
    Returns pre-structured JSON -- provider, tickers, and timestamps are already
    available, eliminating the need for HTML parsing or source attribution logic."""
    resp = fetch_raw(TV_API_URL)
    if resp is None:
        print("   [TradingView API] fetch failed")
        return []
    try:
        data  = resp.json()
        items = data.get("items", [])
        print(f"   [TradingView API] {len(items)} articles")
        return items
    except Exception as e:
        print(f"   [TradingView API] parse error: {e}")
        return []


def _symbols_from_api(related_symbols, watchlist):
    """Extract validated tickers from TradingView's relatedSymbols field.
    Input format: [{"symbol": "NASDAQ:BBIO", ...}]
    Output: set of bare ticker strings validated against our watchlist.
    Foreign exchange symbols (e.g. LSE:RDSA) are automatically excluded."""
    tickers = set()
    for sym in (related_symbols or []):
        raw = sym.get("symbol", "")
        if ":" in raw:
            tk = raw.split(":", 1)[1]
            if tk in watchlist["tickers"]:
                tickers.add(tk)
    return tickers


def _pick_primary_sec_doc(items):
    """Select the primary document from an SEC filing index.
    Prefers .htm/.html files, falls back to .txt. Skips index pages and exhibits."""
    for it in items:
        name = it.get("name", "")
        if (name.lower().endswith((".htm", ".html")) and
                "index" not in name.lower() and
                not re.match(r'^R\d+\.htm$', name, re.IGNORECASE)):
            return name
    for it in items:
        name = it.get("name", "")
        if (name.lower().endswith(".txt") and
                "index" not in name.lower() and
                "complete" not in name.lower() and
                len(name) > 5):
            return name
    return None


def fetch_sec_body(index_url):
    """Fetch the main text body of an SEC filing from its EDGAR index URL.
    Returns (body_text, None) -- second element reserved for future metadata."""
    try:
        directory = index_url.rsplit("/", 1)[0] + "/"
        resp = fetch_raw(directory + "index.json", sec=True)
        if resp is None:
            print(f"   [SEC body] index.json failed: {directory}")
            return None, None

        items   = resp.json()["directory"]["item"]
        primary = _pick_primary_sec_doc(items)
        if not primary:
            print(f"   [SEC body] no .htm/.txt found -- files: {[it['name'] for it in items]}")
            return None, None

        doc = fetch_raw(directory + primary, sec=True)
        if doc is None:
            print(f"   [SEC body] primary doc failed: {primary}")
            return None, None

        # trafilatura preferred; manual HTML stripping used as fallback for structured filings
        body = trafilatura.extract(
            doc.text, include_comments=False, include_tables=True, favor_recall=True
        )
        if not body:
            stripped = re.sub(
                r'<(script|style)[^>]*>.*?</(script|style)>', '',
                doc.text, flags=re.DOTALL | re.IGNORECASE
            )
            stripped = re.sub(r'<[^>]+>', ' ', stripped)
            stripped = re.sub(r'&nbsp;', ' ', stripped)
            stripped = re.sub(r'&[a-zA-Z]+;', '', stripped)
            stripped = re.sub(r'\s+', ' ', stripped).strip()
            body = stripped[:5000] if len(stripped) > 100 else None

        return body, None
    except Exception as e:
        print(f"   [SEC body] error: {e}")
        return None, None


def fetch_form4_data(index_url):
    """Parse a Form 4 XML filing and return structured insider transaction data.
    Extracts issuer ticker, insider identity, role, and all reported transactions."""
    try:
        directory = index_url.rsplit("/", 1)[0] + "/"
        resp = fetch_raw(directory + "index.json", sec=True)
        if resp is None:
            print(f"   [form4] index.json fetch failed: {directory}")
            return None

        items    = resp.json()["directory"]["item"]
        xml_file = next(
            (it["name"] for it in items
             if it["name"].lower().endswith(".xml") and "index" not in it["name"].lower()),
            None
        )
        if not xml_file:
            print(f"   [form4] no .xml found in index -- files: {[it['name'] for it in items]}")
            return None

        doc = fetch_raw(directory + xml_file, sec=True)
        if doc is None:
            print(f"   [form4] XML fetch failed: {directory + xml_file}")
            return None

        root = ET.fromstring(doc.text)

        def txt(path, node=None):
            """Extract text from an XML element by path. Returns None if absent."""
            n = (node or root).find(path)
            return n.text.strip() if n is not None and n.text else None

        issuer_ticker = txt(".//issuerTradingSymbol")
        issuer_name   = txt(".//issuerName")
        insider_name  = txt(".//rptOwnerName")
        officer_title = txt(".//officerTitle")
        role = ("Officer"   if txt(".//isOfficer")         == "1" else
                "Director"  if txt(".//isDirector")        == "1" else
                "10% Owner" if txt(".//isTenPercentOwner") == "1" else "Other")

        # Parse all non-derivative (stock) transactions from the filing
        transactions = []
        for txn in root.findall(".//nonDerivativeTransaction"):
            shares = txt(".//transactionShares/value",               txn)
            price  = txt(".//transactionPricePerShare/value",        txn)
            code   = txt(".//transactionAcquiredDisposedCode/value", txn)
            date   = txt(".//transactionDate/value",                 txn)
            owned  = txt(".//sharesOwnedFollowingTransaction/value", txn)
            if code:
                transactions.append({
                    "type":        code,   # 'A' = acquired, 'D' = disposed
                    "shares":      float(shares) if shares else None,
                    "price":       float(price)  if price  else None,
                    "date":        date,
                    "owned_after": float(owned)  if owned  else None,
                })

        return {
            "issuer_ticker": issuer_ticker,
            "issuer_name":   issuer_name,
            "insider_name":  insider_name,
            "insider_role":  role,
            "insider_title": officer_title,
            "transactions":  transactions,
        }
    except Exception as e:
        print(f"   [form4] error parsing {index_url[:80]}: {e}")
        return None


def _form4_to_text(data):
    """Convert structured Form 4 data to a plain-text summary for LLM scoring.
    Example: 'John Smith (CEO) acquired 18,667 shares at $15.20/share of SentinelOne (S).'"""
    if not data:
        return None
    name      = data.get("insider_name", "Unknown")
    title_str = f" ({data['insider_title']})" if data.get("insider_title") else ""
    lines     = [f"{name}{title_str}"]
    issuer    = data.get("issuer_name", "")
    ticker    = data.get("issuer_ticker", "")
    for txn in data.get("transactions", []):
        action = "acquired" if txn.get("type") == "A" else "disposed of"
        shares = txn.get("shares")
        price  = txn.get("price")
        owned  = txn.get("owned_after")
        s = f"{action} {shares:,.0f} shares" if shares else action
        if price:
            s += f" at ${price:.2f}/share"
        if issuer and ticker:
            s += f" of {issuer} ({ticker})"
        if owned:
            s += f". Total owned: {owned:,.0f} shares"
        lines.append(s + ".")
    return " ".join(lines)


# ============================================================================
# LANE 5 -- SENTIMENT SCORING
# ============================================================================

# Claude Haiku used for speed and cost efficiency at high article volume
SCORING_MODEL     = "claude-haiku-4-5-20251001"
_anthropic_client = None


def _get_anthropic_client():
    """Lazily initialize the Anthropic API client.
    Returns None if the package is not installed or ANTHROPIC_API_KEY is not set.
    Callers check for None and skip scoring gracefully."""
    global _anthropic_client
    if _anthropic_client is None:
        if not _HAS_ANTHROPIC:
            _anthropic_client = False
        else:
            try:
                # anthropic.Anthropic() reads ANTHROPIC_API_KEY from the environment automatically
                _anthropic_client = anthropic.Anthropic()
            except Exception:
                _anthropic_client = False
    return _anthropic_client or None


def extract_fda_tickers(title, body, watchlist):
    """Use the LLM to identify publicly traded companies from an FDA press release.
    Standard name matching fails on FDA content because articles name drugs, not companies.
    Example: 'pembrolizumab' must be inferred as Merck (MRK) via drug brand knowledge.
    Returned tickers are validated against the watchlist to prevent hallucinations."""
    client = _get_anthropic_client()
    if client is None:
        return set()

    prompt = f"""You are a financial analyst reading an FDA press release.

Title: {title}
Text: {(body or '')[:3000]}

Which publicly traded US companies are directly affected by this FDA action?
Include the drug maker, licensee, or named partner. Do NOT include competitors
unless explicitly named.

Respond with ONLY a JSON array of US stock ticker symbols, e.g. ["MRK", "PFE"].
If none, return []. No preamble, no markdown."""

    try:
        response = client.messages.create(
            model=SCORING_MODEL,
            max_tokens=128,
            messages=[{"role": "user", "content": prompt}],
        )
        raw     = response.content[0].text.strip()
        raw     = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw)
        tickers = json.loads(raw)
        valid   = {t for t in tickers if isinstance(t, str) and t in watchlist["tickers"]}
        if valid:
            print(f"   [FDA] LLM identified: {sorted(valid)}")
        return valid
    except Exception as e:
        print(f"   [FDA ticker extract] error: {e}")
        return set()


def score_article(title, body, tickers):
    """Score an article's sentiment per matched ticker using Claude Haiku.

    Returns dict of {ticker: {is_about_ticker, score, reasoning, prominence}}.
    score: float -1.0 to +1.0 (negative = bearish, positive = bullish).
    prominence: float 0-1, how central this ticker is to the article.

    max_tokens=2048 prevents truncation on articles with many matched tickers.
    Partial JSON recovery extracts any complete ticker objects if the response is cut off."""
    client = _get_anthropic_client()
    if not tickers or client is None:
        return {}

    ticker_list = ", ".join(sorted(tickers))
    prompt = f"""You are a financial news analyst. Read the article and for each ticker,
assess whether the article is specifically about that company and its sentiment.

Article title: {title}
Article body: {(body or '')[:4000]}

Tickers to evaluate: {ticker_list}

For each ticker return a JSON object with:
- is_about_ticker: true/false (is this article meaningfully about this company?)
- score: float -1.0 (very bearish) to +1.0 (very bullish)
- reasoning: one sentence explaining your score
- prominence: float 0.0-1.0 (how central is this ticker to the article?)

Return a JSON object mapping each ticker to its assessment.
Return ONLY the JSON object, no preamble or markdown.

Example: {{"AAPL": {{"is_about_ticker": true, "score": 0.7, "reasoning": "...", "prominence": 0.9}}}}"""

    try:
        response = client.messages.create(
            model=SCORING_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Response truncated -- extract any complete ticker objects from partial JSON
            recovered = {}
            for m in re.finditer(r'"([A-Z0-9.\-]+)"\s*:\s*\{[^}]+\}', raw):
                try:
                    obj = json.loads("{" + m.group(0) + "}")
                    recovered.update(obj)
                except Exception:
                    pass
            if recovered:
                print(f"   [sentiment] partial recovery: {list(recovered.keys())}")
            return recovered
    except Exception as e:
        msg = str(e)
        # Rate limit / overload -- back off briefly and retry once rather than
        # silently dropping the article. More likely now that scoring runs
        # concurrently.
        if "429" in msg or "rate_limit" in msg.lower() or "overloaded" in msg.lower():
            time.sleep(2)
            try:
                response = client.messages.create(
                    model=SCORING_MODEL,
                    max_tokens=2048,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = response.content[0].text.strip()
                raw = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw)
                return json.loads(raw)
            except Exception as retry_err:
                print(f"   [sentiment] retry failed: {retry_err}")
                return {}
        print(f"   [sentiment] error: {e}")
        return {}


# ============================================================================
# SIGNAL DETECTION
# ============================================================================

# Keywords that suggest retail herd activity in Stocktwits posts
HERD_KEYWORDS = {
    "whale", "whales", "insider", "loading", "loaded", "calls", "puts",
    "yolo", "squeeze", "dd", "options", "flow", "sweep", "unusual",
    "fomo", "breakout", "accumulating", "accumulation",
}
# Emoji shorthand commonly used in retail trading posts as herd signals
HERD_EMOJIS = {"🐋", "🚀", "💎", "🙌"}

# Cashtags are stripped before keyword matching. Without this the ticker symbol
# itself matches a keyword: every DuPont post carries "$DD" and "dd" is a herd
# keyword, so DD registered 100% herd activity on every message.
CASHTAG_RE = re.compile(r"\$[A-Za-z][A-Za-z.\-]{0,9}")

# Word-boundary match so "dd" no longer fires on added/sudden/hidden and
# "puts" no longer fires on inputs/outputs.
HERD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(k) for k in sorted(HERD_KEYWORDS)) + r")\b",
    re.IGNORECASE,
)


def fetch_stocktwits_stream(ticker, limit=30):
    """Fetch live Stocktwits message stream for a ticker via the public API.
    No authentication required. Returns raw API response dict or None on failure."""
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
    try:
        r = requests.get(
            url,
            headers={"User-Agent": BROWSER_UA},
            timeout=10,
            params={"limit": limit},
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"   [Stocktwits] {ticker}: {e}")
    return None


def _parse_social(data):
    """Extract bullish/bearish counts and herd keyword count from a Stocktwits response.
    Sentiment tags are self-reported by the poster, not inferred by the system.
    Returns (post_count, bullish_pct, keyword_hits, bullish_count, bearish_count)."""
    messages = data.get("messages", []) if data else []
    bullish  = 0
    bearish  = 0
    kw_hits  = 0
    for m in messages:
        # Sentiment tag is nested under entities.sentiment.basic
        sent = (m.get("entities") or {}).get("sentiment") or {}
        tag  = sent.get("basic")
        if tag == "Bullish":
            bullish += 1
        if tag == "Bearish":
            bearish += 1
        body  = m.get("body") or ""
        clean = CASHTAG_RE.sub(" ", body)
        if HERD_RE.search(clean) or any(e in clean for e in HERD_EMOJIS):
            kw_hits += 1
    tagged      = bullish + bearish
    bullish_pct = bullish / tagged if tagged > 0 else None
    return len(messages), bullish_pct, kw_hits, bullish, bearish


def process_social_signals(conn, tickers):
    """Fetch Stocktwits stream for each matched ticker and write to the signals table.
    social_spike: 3+ herd keyword hits in the stream -- elevated retail crowd activity.
    social_read:  baseline reading with no notable herd activity detected."""
    now = datetime.now(timezone.utc).isoformat()
    for ticker in sorted(tickers):
        data = fetch_stocktwits_stream(ticker)
        post_count, bullish_pct, kw_hits, bull_ct, bear_ct = _parse_social(data)
        signal_type = "social_spike" if kw_hits >= 3 else "social_read"
        conn.execute("""
            INSERT INTO signals
            (ticker, signal_type, signal_value, post_count, bullish_pct,
             keyword_hits, detected_at, bullish_count, bearish_count)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (ticker, signal_type, bullish_pct, post_count, bullish_pct, kw_hits,
              now, bull_ct, bear_ct))
        if signal_type == "social_spike":
            print(
                f"   [social spike] {ticker}: {kw_hits} herd keywords "
                f"bullish={f'{bullish_pct:.0%}' if bullish_pct else '--'}"
            )
        time.sleep(0.3)  # Gentle rate limiting for the Stocktwits API
    conn.commit()


def detect_form4_cluster(conn, tickers):
    """Flag tickers with 3+ Form 4 filings within the last 24 hours.
    Cluster insider buying is a stronger signal than a single isolated filing.
    Case study: SentinelOne saw multiple insiders file purchases simultaneously on a
    Friday evening; stock moved 20% by the following week."""
    now    = datetime.now(timezone.utc).isoformat()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    for ticker in tickers:
        count = conn.execute("""
            SELECT COUNT(*) FROM ticker_mentions tm
            JOIN   articles a ON a.id = tm.article_id
            WHERE  tm.ticker = ?
              AND  a.sec_form_type IN ('4', '4/A')
              AND  tm.mentioned_at >= ?
        """, (ticker, cutoff)).fetchone()[0]
        if count >= 3:
            conn.execute("""
                INSERT INTO signals
                (ticker, signal_type, signal_value, post_count,
                 bullish_pct, keyword_hits, detected_at)
                VALUES (?,?,?,?,?,?,?)
            """, (ticker, "form4_cluster", float(count), count, None, 0, now))
            print(f"   [form4 cluster] {ticker}: {count} Form 4 filings in last 24h")
    conn.commit()


def _parse_finviz_val(s):
    """Parse a Finviz numeric string to float.
    Handles percentages and dashes. Examples: '10.75%' -> 10.75, '-' -> None"""
    if not s or s.strip() in ("-", ""):
        return None
    try:
        return float(s.strip().replace("%", "").replace(",", ""))
    except ValueError:
        return None


def fetch_finviz_unusual_volume():
    """Fetch the Finviz Elite unusual volume screener as a list of CSV row dicts.
    Obtains a fresh API key via login before each fetch."""
    api_key = get_finviz_api_key()
    if not api_key:
        print("   [Finviz] no API key available (login failed and no static key set) -- skipping")
        return []
    url = _finviz_screener_url(api_key)
    try:
        r = requests.get(url, headers={"User-Agent": BROWSER_UA}, timeout=15)
        if r.status_code != 200:
            print(f"   [Finviz] HTTP {r.status_code}")
            return []
        rows = list(csv.DictReader(io.StringIO(r.text)))
        print(f"   [Finviz] {len(rows)} tickers from screener")
        return rows
    except Exception as e:
        print(f"   [Finviz] error: {e}")
        return []


def process_finviz_signals(conn):
    """Fetch the Finviz unusual volume list and write signals to the database.

    Applies tiered relvol thresholds by market cap -- large caps flagged at lower relvol.
    Empty market cap in Finviz data indicates an ETF or fund, which is skipped.
    Squeeze setup: short float > 10% AND days to cover > 5 simultaneously."""

    # Purge relvol history older than 30 days to keep the table lean
    cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    conn.execute("DELETE FROM relvol_history WHERE snapshot_at < ?", (cutoff_30d,))

    rows = fetch_finviz_unusual_volume()
    if not rows:
        return

    now   = datetime.now(timezone.utc).isoformat()
    count = 0

    for row in rows:
        ticker = (row.get("Ticker") or "").strip()
        if not ticker:
            continue

        # Empty Market Cap field in Finviz indicates ETF or fund -- skip
        mc_raw = (row.get("Market Cap") or "").strip()
        if not mc_raw or mc_raw == "-":
            continue

        # Parse market cap string to millions for tier classification
        mc_m = None
        try:
            mc_clean = mc_raw.replace(",", "").replace("$", "").strip()
            if mc_clean.endswith("B"):
                mc_m = float(mc_clean[:-1]) * 1000
            elif mc_clean.endswith("M"):
                mc_m = float(mc_clean[:-1])
            elif mc_clean.endswith("K"):
                mc_m = float(mc_clean[:-1]) / 1000
            else:
                # v=152 export returns Market Cap as a plain number already in
                # millions (e.g. "9626.54" = $9.6B) -- do NOT divide again.
                mc_m = float(mc_clean)
        except (ValueError, TypeError):
            mc_m = None

        rv = _parse_finviz_val(row.get("Relative Volume"))
        sf = _parse_finviz_val(row.get("Short Float"))
        sr = _parse_finviz_val(row.get("Short Ratio"))

        # Skip extreme relvol values that are likely data errors
        if rv and rv > 100:
            continue

        tier_label, tier_display, relvol_min = _cap_tier(mc_m)

        # Record relvol for EVERY ticker Finviz returns, even those below their
        # tier threshold. This lets the dashboard show real relative volume for
        # any ticker and spot names with signals firing that have not yet moved.
        if rv:
            conn.execute(
                "INSERT INTO relvol_history (ticker, relvol, snapshot_at) VALUES (?,?,?)",
                (ticker, rv, now)
            )

        # Only write an unusual_volume signal if it clears its cap tier threshold
        if rv is None or rv < relvol_min:
            continue

        # Squeeze setup: high short interest combined with many days needed to cover
        squeeze  = (sf is not None and sf > 10 and sr is not None and sr > 5)
        sig_type = "unusual_volume_squeeze" if squeeze else "unusual_volume"

        # Store all Finviz data as JSON metadata for dashboard display
        meta = json.dumps({
            "relvol":       rv,
            "short_float":  sf,
            "short_ratio":  sr,
            "price":        row.get("Price"),
            "change":       row.get("Change"),
            "news_title":   row.get("News Title"),
            "news_url":     row.get("News URL"),
            "company":      row.get("Company"),
            "market_cap_m": mc_m,
            "cap_tier":     tier_label,
            "cap_display":  tier_display,
            "relvol_min":   relvol_min,
        })

        conn.execute("""
            INSERT INTO signals
            (ticker, signal_type, signal_value, post_count, bullish_pct,
             keyword_hits, detected_at, metadata)
            VALUES (?,?,?,?,?,?,?,?)
        """, (ticker, sig_type, rv, 0, None, int(sf) if sf else 0, now, meta))

        if squeeze:
            print(
                f"   [Finviz] squeeze setup: {ticker} "
                f"relvol={rv:.1f}x sf={sf:.1f}% sr={sr:.1f}d [{tier_display}]"
            )
        else:
            print(f"   [Finviz] {ticker} relvol={rv:.1f}x [{tier_display}, min={relvol_min}x]")
        count += 1

    conn.commit()
    print(f"   [Finviz] {count} unusual volume signals written")


# ============================================================================
# MAIN
# ============================================================================

def main():
    global _finviz_api_key_cache
    _finviz_api_key_cache = None  # force a fresh Finviz login each pipeline run

    # Lock file lets the dashboard detect an in-progress run and refuse to
    # start another on top of it.
    try:
        with open(PIPELINE_LOCK, "w") as f:
            f.write(str(time.time()))
    except Exception:
        pass

    pipeline_start = time.time()
    conn           = sqlite3.connect(DB_PATH)
    init_db(conn)
    purge_old_articles(conn)

    watchlist = load_watchlist()
    keywords  = load_keywords(KEYWORDS_PATH)

    # --- SEC and FDA feeds ---
    for feed in FEEDS:
        feed_start = time.time()
        print(f"\nFetching: {feed['name']}")
        entries = fetch_feed(feed)
        print(f"   {len(entries)} entries")

        is_sec        = "sec.gov" in feed["url"]
        is_fda        = feed.get("is_fda", False)
        do_fetch_body = feed.get("fetch_body", True)
        cik_only      = feed.get("cik_only", is_sec)
        # FDA bodies must always be fetched -- company names rarely appear in RSS titles
        always_fetch  = is_fda and do_fetch_body
        new_c = match_c = body_c = 0

        # Pass 1 -- filter by recency, deduplicate, initial ticker matching from title/summary
        to_process = []
        for entry in entries:
            if not is_recent(entry):
                continue
            link  = entry.get("link", "")
            title = entry.get("title", "")
            if not link or article_exists(conn, link):
                continue
            summary = entry.get("summary", "") or ""
            head    = f"{title} {summary}"

            if feed["name"] == "SEC Form 4":
                ft = detect_form_type_from_title(title)
                if ft not in ("4", "4/A"):
                    continue
            new_c += 1

            form_type = detect_form_type_from_title(title) if is_sec else None
            # FDA: skip name-matching in Pass 1 -- drug names do not map to company names.
            # LLM identification runs in Pass 3 once the article body is available.
            tickers = set() if is_fda else match_tickers(head, watchlist, cik_only=cik_only)
            if tickers:
                match_c += 1
            to_process.append({
                "link": link, "title": title, "summary": summary,
                "form_type": form_type, "tickers": tickers,
                "published": _published_to_iso(entry) or "", "body": None,
                "form4_data": None,
            })

        # Pass 2 -- concurrent body fetching
        def _fetch_body(item):
            """Fetch the article body; parse Form 4 XML for SEC Form 4 filings."""
            if feed["name"] == "SEC Form 4":
                data = fetch_form4_data(item["link"])
                if data:
                    item["form4_data"] = data
                    item["body"]       = _form4_to_text(data)
                    if data.get("issuer_ticker"):
                        item["tickers"].add(data["issuer_ticker"])
                return item
            if not do_fetch_body:
                return item
            if not item["tickers"] and not always_fetch:
                return item
            body, _ = (
                fetch_sec_body(item["link"]) if is_sec
                else (fetch_article_body(item["link"]), None)
            )
            item["body"] = body
            # FDA: re-run ticker matching on full body text now that it is available
            if is_fda and body:
                item["tickers"] = match_tickers(f"{item['title']} {body}", watchlist)
            return item

        with ThreadPoolExecutor(max_workers=5) as executor:
            to_process = list(executor.map(_fetch_body, to_process))

        # Pass 3 -- keyword filter (SEC only), LLM scoring, save to database
        for item in to_process:
            # SEC keyword filter runs on the full title + body to avoid premature filtering
            if is_sec and feed["name"] != "SEC Form 4" and not passes_keyword_filter(
                    f"{item['title']} {item['body'] or ''}", keywords):
                save_article(conn, link=item["link"], title=item["title"],
                             summary=item["summary"], body=None,
                             source=feed["name"], published=item["published"],
                             tickers=set(), form_type=item["form_type"],
                             scores={}, keyword_passed=0)
                continue

            if item["body"]:
                body_c += 1

            # FDA: body is now available -- ask LLM to identify the company from drug name context
            if is_fda and item["body"]:
                item["tickers"] = extract_fda_tickers(
                    item["title"], item["body"], watchlist
                )

            ai_results = score_article(
                item["title"], item["body"] or item["summary"], item["tickers"]
            )
            if ai_results:
                confirmed    = {
                    tk for tk in item["tickers"]
                    if ai_results.get(tk, {}).get("is_about_ticker")
                }
                save_tickers = confirmed
                scores       = {tk: ai_results[tk] for tk in confirmed}
                weights      = {tk: ai_results[tk].get("prominence", 1.0) for tk in confirmed}
            else:
                save_tickers, scores, weights = item["tickers"], {}, {}

            save_article(conn, link=item["link"], title=item["title"],
                         summary=item["summary"], body=item["body"],
                         source=feed["name"], published=item["published"],
                         tickers=save_tickers, form_type=item["form_type"],
                         scores=scores, form4_data=item.get("form4_data"), weights=weights)

        conn.commit()
        print(
            f"   {new_c} new, {match_c} matched, {body_c} bodies  "
            f"({time.time()-feed_start:.1f}s)"
        )

    # --- TradingView news via internal API ---
    tv_start  = time.time()
    print("\nFetching: TradingView News Flow (API)")
    api_items = fetch_tradingview_api()

    # Deduplicate using the TradingView story URL as the canonical unique key
    new_items = []
    for item in api_items:
        story_path = item.get("storyPath", "")
        if not story_path:
            continue
        dedup_key = "https://www.tradingview.com" + story_path
        if not article_exists(conn, dedup_key):
            new_items.append((item, dedup_key))

    print(f"   {len(api_items)} from API, {len(new_items)} new")
    new_c = match_c = body_c = 0
    source_stats = defaultdict(lambda: {"total": 0, "matched": 0})

    def _fetch_api_body(args):
        """Fetch article body from the original source URL when available.
        Falls back to the TradingView reader URL for paywalled sources."""
        item, dedup_key = args
        fetch_url = item.get("link") or dedup_key
        resp      = fetch_raw(fetch_url)
        if not resp:
            return item, dedup_key, None
        body = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
        return item, dedup_key, body

    if new_items:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            fetched = list(executor.map(_fetch_api_body, new_items))

        # --- Prepare scoring jobs -------------------------------------------
        scoring_jobs = []
        for item, dedup_key, body in fetched:
            title     = item.get("title", "")
            source    = item.get("provider", {}).get("name", "TradingView")
            pub_ts    = item.get("published", 0)
            published = (
                datetime.fromtimestamp(pub_ts, tz=timezone.utc).isoformat()
                if pub_ts else ""
            )
            if not title:
                continue
            if body:
                body_c += 1
            new_c += 1
            source_stats[source]["total"] += 1

            api_tickers   = _symbols_from_api(item.get("relatedSymbols", []), watchlist)
            title_tickers = match_tickers(title, watchlist)

            if not api_tickers and not title_tickers and not passes_keyword_filter(
                    title, keywords):
                save_article(conn, link=dedup_key, title=title, summary="", body=None,
                             source=source, published=published, tickers=set(),
                             form_type=None, scores={}, keyword_passed=0,
                             wire_source=source)
                continue

            if not is_english(title or (body or "")[:200]):
                continue

            tickers = api_tickers | match_tickers(f"{title} {body or ''}", watchlist)
            scoring_jobs.append({
                "dedup_key": dedup_key, "title": title, "body": body,
                "source": source, "published": published, "tickers": tickers,
            })

        # --- Score concurrently ---------------------------------------------
        # Each LLM call is network-bound, so running them in parallel cuts this
        # phase from minutes to well under a minute on large catch-up runs.
        def _score_job(job):
            job["ai_results"] = score_article(
                job["title"], job["body"] or "", job["tickers"]
            )
            return job

        if scoring_jobs:
            print(f"   scoring {len(scoring_jobs)} articles concurrently...")
            with ThreadPoolExecutor(max_workers=16) as executor:
                scoring_jobs = list(executor.map(_score_job, scoring_jobs))

        # --- Save results ----------------------------------------------------
        for job in scoring_jobs:
            ai_results = job["ai_results"]
            tickers    = job["tickers"]
            if ai_results:
                confirmed    = {
                    tk for tk in tickers
                    if ai_results.get(tk, {}).get("is_about_ticker")
                }
                save_tickers = confirmed
                scores       = {tk: ai_results[tk] for tk in confirmed}
                weights      = {tk: ai_results[tk].get("prominence", 1.0)
                                for tk in confirmed}
            else:
                save_tickers, scores, weights = tickers, {}, {}

            if save_tickers:
                match_c += 1
                source_stats[job["source"]]["matched"] += 1

            save_article(conn, link=job["dedup_key"], title=job["title"], summary="",
                         body=job["body"] if save_tickers else None,
                         source=job["source"], published=job["published"],
                         tickers=save_tickers, form_type=None,
                         scores=scores, wire_source=job["source"], weights=weights)
    conn.commit()
    print(
        f"   {new_c} new, {match_c} matched, {body_c} bodies  "
        f"({time.time()-tv_start:.1f}s)"
    )

    if source_stats:
        print("\n   Source breakdown:")
        for label, s in sorted(source_stats.items(), key=lambda x: -x[1]["total"]):
            pct = 100 * s["matched"] / s["total"] if s["total"] else 0
            print(f"      {label:<20} {s['matched']:>3}/{s['total']:<3} matched ({pct:.0f}%)")

    # --- Signal detection pass ---
    # Collect all tickers matched in this pipeline run for social and cluster analysis
    cutoff_2h = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    run_tickers = {r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM ticker_mentions WHERE mentioned_at >= ?",
        (cutoff_2h,)
    ).fetchall()}

    if run_tickers:
        print(f"\n[signals] {len(run_tickers)} tickers -- checking form4 clusters...")
        detect_form4_cluster(conn, run_tickers)
        print(f"[signals] Fetching Stocktwits social streams...")
        process_social_signals(conn, run_tickers)

    print(f"\n[signals] Fetching Finviz unusual volume...")
    process_finviz_signals(conn)

    print(f"\n[pipeline] done in {time.time() - pipeline_start:.1f}s")

    try:
        if os.path.exists(PIPELINE_LOCK):
            os.remove(PIPELINE_LOCK)
    except Exception:
        pass


if __name__ == "__main__":
    main()