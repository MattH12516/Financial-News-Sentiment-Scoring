"""
Step 8: Add full-article-body fetching and score sentiment on the body when available.
Falls back to title + summary when body fetch fails (paywalls, JS-heavy sites, etc.).
"""

import csv
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone

import feedparser
import requests
import trafilatura
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

USER_AGENT = "Your Name your.email@example.com"
DB_PATH = "articles.db"
WATCHLIST_PATH = "watchlist.csv"
LM_DICT_PATH = "Loughran-McDonald_MasterDictionary_1993-2025.csv"
LM_GLOVE_SCORES_PATH = "lm_glove_scores.json"

MAX_BODY_CHARS = 5000   # how much body text to store + score

FEEDS = [
    {"name": "SEC EDGAR 8-K", "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom"},
    {"name": "FDA Press Releases", "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml"},
    {"name": "GlobeNewswire - Public Companies", "url": "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies"},
    {"name": "PR Newswire - Financial Services", "url": "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss"},
    {"name": "ACCESSWIRE", "url": "https://www.accesswire.com/users/rss.aspx"},
    {"name": "MarketWatch Top Stories", "url": "http://feeds.marketwatch.com/marketwatch/topstories/"},
    {"name": "CNBC Top News", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html"},
    {"name": "CNBC Business News", "url": "https://www.cnbc.com/id/10001147/device/rss/rss.html"},
    {"name": "Yahoo Finance Top Stories", "url": "https://finance.yahoo.com/news/rssindex"},
]


# -------- VADER analyzer setup --------

def load_lm_word_sets():
    positive = set()
    negative = set()
    with open(LM_DICT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            word = row["Word"].lower().strip()
            if not word:
                continue
            if int(row.get("Positive", 0) or 0) > 0:
                positive.add(word)
            if int(row.get("Negative", 0) or 0) > 0:
                negative.add(word)
    return positive, negative


def init_analyzer():
    analyzer = SentimentIntensityAnalyzer()
    if os.path.exists(LM_GLOVE_SCORES_PATH):
        with open(LM_GLOVE_SCORES_PATH) as f:
            glove_scores = json.load(f)
        added = 0
        for word, score in glove_scores.items():
            if word not in analyzer.lexicon:
                analyzer.lexicon[word] = score
                added += 1
        sv = list(glove_scores.values())
        print(f"LM (GloVe): +{added} words, range {min(sv):.2f} to {max(sv):.2f}")
    else:
        try:
            lm_pos, lm_neg = load_lm_word_sets()
            for w in lm_pos:
                analyzer.lexicon.setdefault(w, 1.8)
            for w in lm_neg:
                analyzer.lexicon.setdefault(w, -1.8)
            print(f"LM (flat fallback): +{len(lm_pos)} positive, +{len(lm_neg)} negative")
        except FileNotFoundError:
            print("LM dictionary not found, using base VADER only")

    analyzer.lexicon.update({
        "soars": 2.5, "soar": 2.5, "soaring": 2.5, "surge": 2.0, "surges": 2.0,
        "rally": 2.0, "rallies": 2.0, "rallied": 2.0, "jump": 1.5, "jumps": 1.5,
        "plunge": -2.5, "plunges": -2.5, "tumble": -2.5, "tumbles": -2.5,
        "beat": 2.0, "beats": 2.0, "beating": 2.0, "missed": -2.0, "missing": -1.5,
        "growth": 1.5, "decline": -1.5, "declines": -1.5,
        "robust": 2.0, "weak": -1.5, "weakness": -1.8, "troubled": -2.0,
        "hefty": 1.5, "tiny": -1.0, "soft": -1.0, "strong": 1.5,
        "outperform": 2.0, "underperform": -2.0,
        "upgrade": 2.0, "downgrade": -2.0,
        "bullish": 2.0, "bearish": -2.0,
    })
    return analyzer


_analyzer = init_analyzer()


def score_sentiment(text):
    if not text or not text.strip():
        return ("neutral", 0.0)
    compound = _analyzer.polarity_scores(text)["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return (label, round(compound, 4))


# -------- Article body fetching --------

def fetch_article_body(url, timeout=15):
    """Fetch a URL and extract main article text. Returns text or None on failure."""
    if not url or url == "N/A":
        return None
    try:
        downloaded = trafilatura.fetch_url(url, timeout=timeout)
        if not downloaded:
            print(f"      [body] fetch_url returned nothing for {url[:80]}")
            return None
        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
        )
        if not text:
            print(f"      [body] extract returned nothing for {url[:80]}")
            return None
        return text[:MAX_BODY_CHARS]
    except Exception as e:
        print(f"      [body] exception {type(e).__name__}: {e} for {url[:80]}")
        return None


# -------- Database --------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            link TEXT PRIMARY KEY,
            source TEXT,
            title TEXT,
            published TEXT,
            summary TEXT,
            body_text TEXT,
            matched_tickers TEXT,
            sentiment_label TEXT,
            sentiment_score REAL,
            ingested_at TEXT
        )
    """)
    for col_def in ["matched_tickers TEXT", "sentiment_label TEXT",
                    "sentiment_score REAL", "body_text TEXT"]:
        try:
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col_def}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


def save_article(conn, article):
    try:
        conn.execute(
            "INSERT INTO articles (link, source, title, published, summary, body_text, "
            "matched_tickers, sentiment_label, sentiment_score, ingested_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                article["link"], article["source"], article["title"],
                article["published"], article["summary"],
                article.get("body_text"),
                article["matched_tickers"],
                article.get("sentiment_label"),
                article.get("sentiment_score"),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        return True
    except sqlite3.IntegrityError:
        return False


# -------- Watchlist + matching --------

def load_watchlist():
    entries = []
    with open(WATCHLIST_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("Ticker", "").strip().upper()
            company = row.get("Company", "").strip()
            cik_str = row.get("CIK", "").strip()
            cik = int(cik_str) if cik_str else None
            if ticker:
                entries.append((ticker, company, cik))
    return entries


def find_tickers(text, watchlist):
    if not text:
        return []
    matched = set()
    cik_to_ticker = {cik: t for t, _, cik in watchlist if cik is not None}

    for cik_match in re.finditer(r"\((\d{7,10})\)", text):
        cik = int(cik_match.group(1))
        ticker = cik_to_ticker.get(cik)
        if ticker:
            matched.add(ticker)

    for ticker, company, _ in watchlist:
        if ticker in matched:
            continue
        if re.search(rf"\b{re.escape(ticker)}\b", text):
            matched.add(ticker)
            continue
        if company and re.search(rf"\b{re.escape(company)}\b", text, re.IGNORECASE):
            matched.add(ticker)
    return sorted(matched)


# -------- Feed fetching --------

def fetch_feed(feed_config):
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(feed_config["url"], headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"   HTTP {response.status_code} — skipping")
            return []
        return feedparser.parse(response.content).entries
    except Exception as e:
        print(f"   Error: {e}")
        return []


# -------- Main pipeline --------

def main():
    init_db()
    watchlist = load_watchlist()
    print(f"\nWatchlist: {len(watchlist)} entries\n")

    conn = sqlite3.connect(DB_PATH)
    total_new = 0
    total_matched = 0
    total_body_fetched = 0

    for feed_config in FEEDS:
        print(f"Fetching: {feed_config['name']}")
        entries = fetch_feed(feed_config)
        print(f"   Got {len(entries)} entries")

        new_count = 0
        match_count = 0
        body_count = 0
        for entry in entries:
            title = entry.get("title", "")
            summary = entry.get("summary", "")[:500]
            combined_text = f"{title} {summary}"
            matched = find_tickers(combined_text, watchlist)

            body_text = None
            sentiment_label = None
            sentiment_score = None

            if matched:
                # Try to fetch full body
                body_text = fetch_article_body(entry.get("link", ""))
                if body_text:
                    body_count += 1
                # Score on body if available, else fall back
                text_to_score = body_text if body_text else combined_text
                sentiment_label, sentiment_score = score_sentiment(text_to_score)

            article = {
                "source": feed_config["name"],
                "title": title or "N/A",
                "link": entry.get("link", "N/A"),
                "published": entry.get("updated", entry.get("published", "N/A")),
                "summary": summary,
                "body_text": body_text,
                "matched_tickers": ",".join(matched),
                "sentiment_label": sentiment_label,
                "sentiment_score": sentiment_score,
            }
            if save_article(conn, article):
                new_count += 1
                if matched:
                    match_count += 1

        conn.commit()
        total_new += new_count
        total_matched += match_count
        total_body_fetched += body_count
        print(f"   {new_count} new ({match_count} matched, {body_count} body fetched)")
        time.sleep(0.5)

    # Retroactive: re-match and re-score with whatever text we have for each article
    print("\nRe-matching existing articles with current watchlist...")
    cursor = conn.execute("SELECT link, title, summary, body_text FROM articles")
    rows = cursor.fetchall()
    rematch_count = 0
    for link, title, summary, body_text in rows:
        combined = f"{title or ''} {summary or ''}"
        matched = find_tickers(combined, watchlist)
        if matched:
            text_to_score = body_text if body_text else combined
            label, score = score_sentiment(text_to_score)
            conn.execute(
                "UPDATE articles SET matched_tickers = ?, sentiment_label = ?, sentiment_score = ? "
                "WHERE link = ?",
                (",".join(matched), label, score, link),
            )
            rematch_count += 1
    conn.commit()
    print(f"  Re-matched {rematch_count} articles.")

    cursor = conn.execute("SELECT COUNT(*) FROM articles WHERE matched_tickers != ''")
    db_matched = cursor.fetchone()[0]
    cursor = conn.execute("SELECT COUNT(*) FROM articles WHERE body_text IS NOT NULL")
    db_with_bodies = cursor.fetchone()[0]
    cursor = conn.execute("SELECT COUNT(*) FROM articles")
    db_total = cursor.fetchone()[0]

    print(f"\n{'=' * 80}")
    print(f"Database: {db_total} total | {db_matched} matched | {db_with_bodies} with full bodies")
    print(f"This run: {total_new} new articles, {total_body_fetched} bodies fetched")
    print(f"{'=' * 80}\n")

    print("Matches by source:")
    cursor = conn.execute(
        "SELECT source, COUNT(*) FROM articles WHERE matched_tickers != '' "
        "GROUP BY source ORDER BY COUNT(*) DESC"
    )
    for source, count in cursor.fetchall():
        print(f"   {count:4d}  {source}")

    print("\nMost recent matched articles:\n")
    cursor = conn.execute(
        "SELECT source, title, matched_tickers, sentiment_label, sentiment_score, "
        "body_text IS NOT NULL as has_body, published "
        "FROM articles WHERE matched_tickers != '' "
        "ORDER BY ingested_at DESC LIMIT 15"
    )
    for source, title, tickers, label, score, has_body, published in cursor.fetchall():
        if score is None:
            marker = "[  ?  ]"
        elif score > 0.05:
            marker = f"[ POS {score:+.2f} ]"
        elif score < -0.05:
            marker = f"[ NEG {score:+.2f} ]"
        else:
            marker = f"[ NEU {score:+.2f} ]"
        body_marker = "[B]" if has_body else "   "
        print(f"{marker} {body_marker} [{tickers}]")
        print(f"   ({source}) {title}")
        print(f"   {published}\n")

if __name__ == "__main__":
    main()