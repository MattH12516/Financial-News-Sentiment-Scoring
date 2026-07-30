"""
app.py -- SentiFeed dashboard.

Four-tab Streamlit application that reads from the SQLite database
populated by pipeline.py.

Tabs:
  1. News Feed    -- filterable article list with LLM sentiment breakdown
  2. Ticker Chart -- sentiment, message density, social bullish%, and stock price overlay
  3. Social Feed  -- live Stocktwits herd sentiment for any ticker
  4. Trader Zone  -- launches the pop-out ranked ticker screener

Usage:
    python3 -m streamlit run app.py
"""

import os
import sqlite3
import math
import sys
import time
import subprocess
from datetime import datetime, timezone, timedelta

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf

import pipeline

st.set_page_config(page_title="SentiFeed", layout="wide", page_icon="⚡")

# Detect Railway deployment -- sets the correct base URL for the Trader Zone link.
# RAILWAY_PUBLIC_DOMAIN is set automatically by Railway in deployed environments.
_domain   = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
_base_url = f"https://{_domain}" if _domain else "http://localhost:8501"

st.markdown("""
<style>
    .stApp { background-color: #0e1117; }

    /* Navigation lives in the app's own tabs and links, not the sidebar */
    [data-testid="stSidebarNav"] { display: none; }

    /* Wider scrollbars -- easier to grab without a scroll wheel */
    ::-webkit-scrollbar { width: 18px; height: 18px; }
    ::-webkit-scrollbar-track { background: #0e1117; }
    ::-webkit-scrollbar-thumb {
        background: #3d444d; border-radius: 9px; border: 3px solid #0e1117;
    }
    ::-webkit-scrollbar-thumb:hover { background: #58a6ff; }

    /* Wide enough for an absolute timestamp without wrapping */
    .article-time { color: #6e7681; min-width: 112px; white-space: nowrap; }

    /* News Feed article row */
    .article-row {
        padding: 11px 14px; border-bottom: 1px solid #262730;
        display: flex; align-items: center; gap: 12px; font-size: 15px;
    }
    min-width: 112px; white-space: nowrap;
    .article-source {
        background: #1c1f26; color: #8b949e; padding: 2px 8px;
        border-radius: 4px; font-size: 11px; min-width: 100px;
        text-align: center; white-space: nowrap;
    }
    .ticker-badge {
        background: #1f3a5f; color: #58a6ff; padding: 2px 6px;
        border-radius: 4px; font-size: 11px; font-weight: 600;
        margin-right: 4px; white-space: nowrap;
    }
    .article-title       { color: #e6edf3; text-decoration: none; flex: 1; }
    .article-title:hover { color: #58a6ff; }

    /* Sentiment score labels */
    .sentiment-pending  { color: #6e7681; font-size: 12px; font-weight: 600; min-width: 70px; text-align: right; white-space: nowrap; }
    .sentiment-neutral  { color: #8b949e; font-size: 12px; font-weight: 600; min-width: 70px; text-align: right; white-space: nowrap; }
    .sentiment-positive { color: #3fb950; font-size: 12px; font-weight: 600; min-width: 70px; text-align: right; white-space: nowrap; }
    .sentiment-negative { color: #f85149; font-size: 12px; font-weight: 600; min-width: 70px; text-align: right; white-space: nowrap; }

    /* AI breakdown reasoning rows */
    .reasoning-ticker { font-weight: 700; color: #58a6ff; font-size: 13px; }
    .reasoning-pos    { color: #3fb950; font-weight: 700; }
    .reasoning-neg    { color: #f85149; font-weight: 700; }
    .reasoning-neu    { color: #8b949e; font-weight: 700; }
    .reasoning-text   { color: #8b949e; font-size: 13px; }
    .reasoning-meta   { color: #6e7681; font-size: 11px; }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# DATABASE HELPERS
# ============================================================================

from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")


def to_eastern(dt_series):
    """Convert a datetime series (naive-UTC or tz-aware) to Eastern for display.
    All chart traces must go through this so they align on the same x-axis --
    mixing tz-aware and tz-naive series causes visible misalignment in Plotly."""
    dt_series = pd.to_datetime(dt_series)
    if dt_series.dt.tz is None:
        dt_series = dt_series.dt.tz_localize("UTC")
    else:
        dt_series = dt_series.dt.tz_convert("UTC")
    return dt_series.dt.tz_convert(EASTERN)


def scale_marker_sizes(counts, min_size=6, max_size=22):
    """Scale a series of sample-size counts into pixel marker sizes so a
    reader can see at a glance whether a point represents 2 messages or 50."""
    counts = pd.Series(counts).fillna(0)
    if counts.max() == counts.min():
        return [min_size] * len(counts)
    scaled = min_size + (counts - counts.min()) / (counts.max() - counts.min()) * (max_size - min_size)
    return scaled.tolist()

def fmt_time(iso_str):
    """Absolute Eastern 12-hour timestamp, e.g. '5:07 PM Jul 29'."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(EASTERN)
        hour = dt.strftime("%I").lstrip("0") or "12"
        return f"{hour}:{dt.strftime('%M %p %b %d')}"
    except Exception:
        return "" 
    
def get_connection():
    """Return a SQLite connection to the pipeline database."""
    return sqlite3.connect(pipeline.DB_PATH, check_same_thread=False)


def time_ago(iso_str):
    """Convert an ISO timestamp to a human-readable relative time string.
    Examples: 'now', '5m', '3h', '2d'"""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mins = int((datetime.now(timezone.utc) - dt).total_seconds() // 60)
        if mins < 1:   return "now"
        if mins < 60:  return f"{mins}m"
        hours = mins // 60
        return f"{hours}h" if hours < 24 else f"{hours // 24}d"
    except Exception:
        return ""


@st.cache_data(ttl=10)
def load_sources(_conn):
    """Return all distinct news sources currently in the database."""
    rows = _conn.execute(
        "SELECT DISTINCT source FROM articles ORDER BY source"
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=10)
def load_tickers(_conn):
    """Return all distinct tickers currently in the ticker_mentions table."""
    rows = _conn.execute(
        "SELECT DISTINCT ticker FROM ticker_mentions ORDER BY ticker"
    ).fetchall()
    return [r[0] for r in rows]


def load_articles(conn, sources, ticker, keyword, only_matched, sort_order, limit):
    """Query articles with optional filters for source, ticker, keyword, and sort order.
    Returns a pandas DataFrame with columns for display in the News Feed tab."""
    query  = """SELECT a.id, a.title, a.source, a.link, a.ingested_at, a.published,
                       a.matched_tickers, a.sentiment_score
                FROM articles a"""
    params = []
    where  = []

    if ticker and ticker != "All":
        query += " JOIN ticker_mentions tm ON tm.article_id = a.id"
        where.append("tm.ticker = ?")
        params.append(ticker)

    if only_matched:
        where.append("a.matched_tickers != ''")
    if sources:
        where.append(f"a.source IN ({','.join('?' for _ in sources)})")
        params.extend(sources)
    if keyword:
        where.append("(a.title LIKE ? OR a.body LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])

    if where:
        query += " WHERE " + " AND ".join(where)

    if sort_order == "Highest sentiment first":
        query += (" ORDER BY CASE WHEN a.sentiment_score IS NULL THEN 1 ELSE 0 END,"
                  " a.sentiment_score DESC")
    elif sort_order == "Lowest sentiment first":
        query += (" ORDER BY CASE WHEN a.sentiment_score IS NULL THEN 1 ELSE 0 END,"
                  " a.sentiment_score ASC")
    else:
        query += " ORDER BY a.ingested_at DESC"

    query += " LIMIT ?"
    params.append(limit)
    return pd.read_sql_query(query, conn, params=params)


def load_ticker_reasoning(conn, article_id):
    """Return per-ticker LLM scores and reasoning for a single article.
    Ordered by prominence descending so the most central tickers appear first."""
    return conn.execute("""
        SELECT ticker, score, reasoning, weight
        FROM   ticker_mentions
        WHERE  article_id = ? AND reasoning IS NOT NULL
        ORDER  BY COALESCE(weight, 1.0) DESC
    """, (article_id,)).fetchall()


def load_sentiment_series(conn, ticker, since_iso):
    """Return hourly average sentiment scores for a ticker within the time window.
    Used to draw the sentiment line on the Ticker Chart."""
    rows = conn.execute("""
        SELECT strftime('%Y-%m-%dT%H:00:00', tm.mentioned_at) AS hour,
               AVG(tm.score)                                   AS avg_score,
               COUNT(*)                                        AS mentions
        FROM   ticker_mentions tm
        WHERE  tm.ticker      = ?
          AND  tm.mentioned_at >= ?
          AND  tm.score IS NOT NULL
        GROUP  BY hour
        ORDER  BY hour
    """, (ticker, since_iso)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["hour", "avg_score", "mentions"])
    df = pd.DataFrame(rows, columns=["hour", "avg_score", "mentions"])
    df["hour"] = pd.to_datetime(df["hour"])
    return df


def load_density_series(conn, ticker, since_iso):
    """Return hourly article mention counts for a ticker within the time window.
    Used to draw the message density bars on the Ticker Chart."""
    rows = conn.execute("""
        SELECT strftime('%Y-%m-%dT%H:00:00', mentioned_at) AS hour,
               COUNT(*)                                    AS mentions
        FROM   ticker_mentions
        WHERE  ticker       = ?
          AND  mentioned_at >= ?
        GROUP  BY hour
        ORDER  BY hour
    """, (ticker, since_iso)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["hour", "mentions"])
    df = pd.DataFrame(rows, columns=["hour", "mentions"])
    df["hour"] = pd.to_datetime(df["hour"])
    return df


@st.cache_data(ttl=900)
def _load_price_data_cached(ticker, period, interval):
    """Fetch historical OHLCV data from yfinance for the Ticker Chart price overlay.
    Handles both timezone-aware and timezone-naive index formats across yfinance versions."""
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period=period, interval=interval)
        if hist.empty:
            return pd.DataFrame()
        # Normalize index timezone -- yfinance behavior changed between versions
        if hist.index.tz is None:
            hist.index = hist.index.tz_localize("UTC")
        else:
            hist.index = hist.index.tz_convert("UTC")
        df       = hist[["Close", "Volume"]].reset_index()
        # Column name for the datetime index also varies by yfinance version
        time_col = next(
            (c for c in df.columns if c.lower() in ("datetime", "date", "timestamp", "index")),
            df.columns[0]
        )
        return df.rename(columns={time_col: "ts", "Close": "price", "Volume": "volume"})
    except Exception:
        return pd.DataFrame()

def load_price_data(ticker, period, interval):
    """Fetch price data with retries, and critically -- never cache an empty
    result. A single transient Yahoo failure was previously being frozen into
    the cache for the full 15 minutes, which made working tickers look broken."""
    for attempt in range(3):
        df = _load_price_data_cached(ticker, period, interval)
        if not df.empty:
            return df
        # Clear the cached empty result so the retry actually re-fetches
        _load_price_data_cached.clear()
        if attempt < 2:
            time.sleep(0.4)
    return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_social_series(_conn, ticker, since_iso):
    """Return hourly social message density (bullish/bearish counts) for the
    Ticker Chart overlay. Bucketed the same way as news sentiment so both
    series carry visible sample sizes. Falls back to post_count for older
    rows written before bullish_count/bearish_count existed."""
    try:
        rows = _conn.execute("""
            SELECT strftime('%Y-%m-%dT%H:00:00', detected_at) AS hour,
                   AVG(bullish_pct)                              AS avg_bullish_pct,
                   SUM(COALESCE(bullish_count, 0))                AS bullish_ct,
                   SUM(COALESCE(bearish_count, 0))                AS bearish_ct,
                   SUM(COALESCE(post_count, 0))                   AS total_posts,
                   SUM(COALESCE(keyword_hits, 0))                 AS herd_hits
            FROM   signals
            WHERE  ticker = ? AND detected_at >= ?
              AND  signal_type IN ('social_spike','social_read')
            GROUP  BY hour
            ORDER  BY hour
        """, (ticker, since_iso)).fetchall()
    except Exception:
        return pd.DataFrame(columns=[
            "hour", "avg_bullish_pct", "bullish_ct", "bearish_ct", "total_posts", "herd_hits"
        ])
    if not rows:
        return pd.DataFrame(columns=[
            "hour", "avg_bullish_pct", "bullish_ct", "bearish_ct", "total_posts", "herd_hits"
        ])
    df = pd.DataFrame(rows, columns=[
        "hour", "avg_bullish_pct", "bullish_ct", "bearish_ct", "total_posts", "herd_hits"
    ])
    df["hour"] = pd.to_datetime(df["hour"])
    # Sample size for marker scaling -- prefer explicit bullish+bearish counts,
    # fall back to total_posts for rows written before those columns existed.
    df["sample_size"] = df["bullish_ct"] + df["bearish_ct"]
    df.loc[df["sample_size"] == 0, "sample_size"] = df["total_posts"]
    return df


def sentiment_html(score):
    """Return an HTML span with a color-coded sentiment score for the News Feed."""
    if score is None:
        return '<span class="sentiment-pending">-- pending</span>'
    if -0.05 <= score <= 0.05:
        return f'<span class="sentiment-neutral">{score:+.2f}</span>'
    if score > 0.05:
        return f'<span class="sentiment-positive">{score:+.2f}</span>'
    return f'<span class="sentiment-negative">{score:+.2f}</span>'


def sentiment_bar_html(score):
    """Return an HTML gradient bar representing the sentiment score.
    Bar spans -1.0 (red) to 0 (grey) to +1.0 (green) with a white position marker.
    Represents sentiment only -- density is shown separately."""
    pct = (score + 1) / 2 * 100
    return f"""
        <div style="flex:1; background:linear-gradient(to right,#f85149 0%,#6e7681 50%,#3fb950 100%);
                    border-radius:4px; height:8px; position:relative; min-width:120px;"
             title="Sentiment score: {score:+.2f}">
            <div style="position:absolute; left:calc({pct:.1f}% - 2px);
                        width:4px; height:8px; background:#fff;
                        border-radius:2px;"></div>
        </div>"""


# ============================================================================
# HEADER
# ============================================================================

hcol1, hcol2, hcol3 = st.columns([3, 1, 1])
with hcol1:
    st.markdown("### SentiFeed")
with hcol2:
    if st.button("Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
with hcol3:
    run_pipeline = st.button("Run pipeline now", use_container_width=True)

if run_pipeline:
    with st.spinner("Fetching new articles -- this can take 1-10 minutes depending on how much new content there is since the last run..."):
        result = subprocess.run(
            [sys.executable, "pipeline.py"],
            capture_output=True, text=True, timeout=900
        )
    st.cache_data.clear()
    st.success("Pipeline finished -- reloading page...")
    with st.expander("Pipeline output", expanded=True):
        st.code(result.stdout + result.stderr)
    # Force a hard browser reload rather than relying on rerun() over a
    # WebSocket connection that may have gone stale during the long block.
    import streamlit.components.v1 as components
    components.html(
        "<script>setTimeout(function(){ window.parent.location.reload(); }, 1500);</script>",
        height=0
    )
conn = get_connection()

# ============================================================================
# TABS
# ============================================================================

tab_feed, tab_chart, tab_social, tab_trader = st.tabs([
    "News Feed",
    "Ticker Chart",
    "Social Feed",
    "Trader Zone",
])


# ============================================================================
# TAB 1 -- NEWS FEED
# ============================================================================
with tab_feed:
    fcol1, fcol2, fcol3, fcol4, fcol5 = st.columns([2, 1, 2, 1, 1])
    with fcol1:
        all_sources = load_sources(conn)
        sources     = st.multiselect("Source", all_sources, default=all_sources)
    with fcol2:
        feed_ticker_all = load_tickers(conn)
        ticker_search   = st.text_input(
            "Search ticker", placeholder="type any ticker..."
        ).strip().upper()
        if ticker_search:
            feed_ticker = ticker_search
        else:
            feed_ticker = st.selectbox("Or select", ["All"] + feed_ticker_all)
    with fcol3:
        keyword = st.text_input("Search title/body", placeholder="e.g. earnings, merger...")
    with fcol4:
        only_matched = st.checkbox("Matched only", value=True)
    with fcol5:
        sort_order = st.selectbox("Sort by", [
            "Newest first", "Highest sentiment first", "Lowest sentiment first"
        ])

    st.caption("Click **AI breakdown** under any article to see per-ticker scores and reasoning.")

    df = load_articles(conn, sources, feed_ticker, keyword, only_matched, sort_order, limit=200)

    if df.empty:
        st.info("No articles match these filters.")
    else:
        for _, row in df.iterrows():
            tickers_html = "".join(
                f'<span class="ticker-badge">{t}</span>'
                for t in (row["matched_tickers"] or "").split(",") if t
            )
            st.markdown(f"""
                <div class="article-row">
                    <span class="article-time">{fmt_time(row['published'] or row['ingested_at'])}</span>
                    <span class="article-source">{row['source']}</span>
                    {tickers_html}
                    <a class="article-title" href="{row['link']}" target="_blank">{row['title']}</a>
                    {sentiment_html(row['sentiment_score'])}
                </div>
            """, unsafe_allow_html=True)

            if row["matched_tickers"]:
                reasoning_rows = load_ticker_reasoning(conn, row["id"])
                if reasoning_rows:
                    with st.expander("AI breakdown"):
                        for tk, tk_score, tk_reasoning, tk_weight in reasoning_rows:
                            if tk_score is not None and tk_score > 0.05:
                                score_cls = "reasoning-pos"
                            elif tk_score is not None and tk_score < -0.05:
                                score_cls = "reasoning-neg"
                            else:
                                score_cls = "reasoning-neu"
                            score_str  = f"{tk_score:+.2f}" if tk_score is not None else "--"
                            prominence = f"{tk_weight:.0%}" if tk_weight else "--"
                            st.markdown(f"""
                                <div style="padding:8px 0; border-bottom:1px solid #1c1f26;">
                                    <span class="reasoning-ticker">{tk}</span>&nbsp;
                                    <span class="{score_cls}">{score_str}</span>&nbsp;
                                    <span class="reasoning-meta">prominence {prominence}</span><br>
                                    <span class="reasoning-text">{tk_reasoning or "No reasoning recorded."}</span>
                                </div>
                            """, unsafe_allow_html=True)

        st.caption(f"{len(df)} articles shown (limit 200)")


# ============================================================================
# TAB 2 -- TICKER CHART
# ============================================================================
with tab_chart:
    all_tickers = load_tickers(conn)
    if not all_tickers:
        st.info("No ticker data yet -- run the pipeline a few times first.")
    else:
        ccol1, ccol2 = st.columns([1, 3])
        with ccol1:
            manual_ticker = st.text_input(
                "Type any ticker", placeholder="e.g. AAPL...",
                help="Any ticker for price data. Sentiment/density shown only for tickers with DB history."
            ).strip().upper()

            if manual_ticker:
                chart_ticker = manual_ticker
                in_db        = manual_ticker in all_tickers
                if not in_db:
                    st.caption("Price only -- no sentiment history for this ticker yet.")
            else:
                search   = st.text_input("Filter DB tickers", placeholder="search...")
                filtered = (
                    [t for t in all_tickers if search.strip().upper() in t.upper()]
                    if search.strip() else all_tickers
                )
                chart_ticker = st.selectbox(
                    "Select ticker",
                    filtered if filtered else all_tickers,
                    key="chart_ticker"
                )
                in_db = True

            timeframe = st.radio(
                "Timeframe",
                ["10 min", "30 min", "1 hour", "1 day", "1 week", "1 month", "Custom"],
                index=3,
                help="Short-term windows use 1-minute bars to zoom into how price reacts around a specific event."
            )
            if timeframe == "Custom":
                date_range = st.date_input(
                    "Date range",
                    value=[
                        (datetime.now(timezone.utc) - timedelta(days=14)).date(),
                        datetime.now(timezone.utc).date(),
                    ],
                    max_value=datetime.now(timezone.utc).date(),
                )

        # Map timeframe selections to database lookback (minutes) and yfinance params.
        # Short windows fetch the finest yfinance interval (1m) then get trimmed
        # client-side to the exact requested window.
        tf_map = {
            "10 min":  {"minutes": 10,    "yf_period": "1d",  "yf_interval": "1m"},
            "30 min":  {"minutes": 30,    "yf_period": "1d",  "yf_interval": "1m"},
            "1 hour":  {"minutes": 60,    "yf_period": "1d",  "yf_interval": "1m"},
            "1 day":   {"minutes": 1440,  "yf_period": "1d",  "yf_interval": "5m"},
            "1 week":  {"minutes": 10080, "yf_period": "5d",  "yf_interval": "30m"},
            "1 month": {"minutes": 43200, "yf_period": "1mo", "yf_interval": "1h"},
        }
        if timeframe == "Custom" and len(date_range) == 2:
            start_date, end_date = date_range
            since_iso   = datetime.combine(
                start_date, datetime.min.time()
            ).replace(tzinfo=timezone.utc).isoformat()
            days_delta  = (end_date - start_date).days
            yf_period   = f"{max(days_delta, 1)}d"
            yf_interval = "1h" if days_delta > 7 else "30m"
            since_cutoff_utc = datetime.combine(
                start_date, datetime.min.time()
            ).replace(tzinfo=timezone.utc)
        else:
            tf          = tf_map[timeframe]
            since_cutoff_utc = datetime.now(timezone.utc) - timedelta(minutes=tf["minutes"])
            since_iso   = since_cutoff_utc.isoformat()
            yf_period   = tf["yf_period"]
            yf_interval = tf["yf_interval"]

        sentiment_df = load_sentiment_series(conn, chart_ticker, since_iso)
        density_df   = load_density_series(conn, chart_ticker, since_iso)
        social_df    = load_social_series(conn, chart_ticker, since_iso)
        price_df     = load_price_data(chart_ticker, yf_period, yf_interval)

        with ccol2:
            has_sentiment = not sentiment_df.empty
            has_density   = not density_df.empty
            has_social    = not social_df.empty and social_df["avg_bullish_pct"].notna().any()
            has_price     = not price_df.empty

            if not has_price and timeframe in ("10 min", "30 min", "1 hour", "1 day", "1 week"):
                st.caption(
                    f"No intraday price data available for **{chart_ticker}** at this "
                    "interval. This is a common Yahoo Finance data gap for lower-volume "
                    "tickers at fine intervals, not an app error -- try a wider timeframe."
                )

            if not has_sentiment and not has_density and not has_price and not has_social:
                st.info(f"No data found for **{chart_ticker}** in this timeframe yet.")
            else:
                fig = make_subplots(specs=[[{"secondary_y": True}]])

                # Message density bars -- raw mention counts on the left Y axis
                if has_density:
                    density_df["hour_et"] = to_eastern(density_df["hour"])
                    fig.add_trace(go.Bar(
                        x=density_df["hour_et"], y=density_df["mentions"],
                        name="News message density",
                        marker_color="rgba(251,140,0,0.8)",
                        hovertemplate="%{y} articles<extra></extra>",
                    ), secondary_y=False)

                # Average news sentiment line -- marker size shows sample size per point
                if has_sentiment:
                    sentiment_df["hour_et"] = to_eastern(sentiment_df["hour"])
                    sizes = scale_marker_sizes(sentiment_df["mentions"])
                    fig.add_trace(go.Scatter(
                        x=sentiment_df["hour_et"], y=sentiment_df["avg_score"],
                        name="Avg news sentiment",
                        mode="lines+markers",
                        line=dict(color="#58a6ff", width=2),
                        marker=dict(size=sizes, line=dict(width=1, color="#0e1117")),
                        customdata=sentiment_df["mentions"],
                        hovertemplate="%{y:+.2f} (%{customdata} articles)<extra></extra>",
                    ), secondary_y=False)
                    fig.add_hline(
                        y=0, line_dash="dot",
                        line_color="rgba(139,148,158,0.4)",
                        secondary_y=False
                    )

                # Social bullish% line -- marker size shows message sample size,
                # hover shows the bullish/bearish split explicitly
                if has_social:
                    social_df["hour_et"] = to_eastern(social_df["hour"])
                    sizes = scale_marker_sizes(social_df["sample_size"])
                    fig.add_trace(go.Scatter(
                        x=social_df["hour_et"], y=social_df["avg_bullish_pct"],
                        name="Social bullish%",
                        mode="lines+markers",
                        line=dict(color="#bc8cff", width=2, dash="dot"),
                        marker=dict(size=sizes, line=dict(width=1, color="#0e1117")),
                        customdata=list(zip(social_df["bullish_ct"], social_df["bearish_ct"])),
                        hovertemplate=(
                            "%{y:.0%} bullish<br>"
                            "%{customdata[0]:.0f} bullish / %{customdata[1]:.0f} bearish"
                            "<extra></extra>"
                        ),
                    ), secondary_y=False)

                # Stock price line on the right Y axis
                if has_price:
                    price_col = (
                        "Datetime" if "Datetime" in price_df.columns
                        else price_df.columns[0]
                    )
                    price_df["ts_et"] = to_eastern(price_df[price_col])
                    price_df_trimmed = price_df[price_df["ts_et"] >= since_cutoff_utc.astimezone(EASTERN)]
                    if price_df_trimmed.empty:
                        price_df_trimmed = price_df
                    fig.add_trace(go.Scatter(
                        x=price_df_trimmed["ts_et"], y=price_df_trimmed["price"],
                        name="Stock price",
                        mode="lines",
                        line=dict(color="#3fb950", width=2),
                        hovertemplate="$%{y:.2f}<extra></extra>",
                    ), secondary_y=True)

                fig.update_layout(
                    title=dict(
                        text=f"{chart_ticker} -- Sentiment / Density / Price (Eastern time)",
                        font=dict(color="#e6edf3")
                    ),
                    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                    font=dict(color="#8b949e"),
                    legend=dict(bgcolor="#1c1f26", bordercolor="#262730", borderwidth=1),
                    hovermode="x unified", height=480,
                    margin=dict(l=10, r=10, t=50, b=10),
                    xaxis=dict(gridcolor="#1c1f26", showgrid=True),
                    yaxis=dict(
                        gridcolor="#1c1f26", showgrid=True,
                        title="Sentiment / Density"
                    ),
                    barmode="overlay",
                    yaxis2=dict(title="Price (USD)", showgrid=False),
                )
                st.plotly_chart(fig, use_container_width=True)

                # Summary metrics below the chart
                tf_label = timeframe if timeframe != "Custom" else "selected range"
                st.caption(f"All metrics below reflect the **{tf_label}** window only.")
                scol1, scol2, scol3, scol4 = st.columns(4)
                with scol1:
                    if has_sentiment:
                        st.metric(f"Avg news sentiment ({tf_label})",
                                  f"{sentiment_df['avg_score'].mean():+.2f}")
                with scol2:
                    if has_density:
                        st.metric(f"News mentions ({tf_label})",
                                  int(density_df["mentions"].sum()))
                with scol3:
                    if has_social:
                        st.metric(f"Avg social bullish% ({tf_label})",
                                  f"{social_df['avg_bullish_pct'].mean():.0%}")
                with scol4:
                    if has_price:
                        latest   = price_df_trimmed["price"].iloc[-1]
                        earliest = price_df_trimmed["price"].iloc[0]
                        chg      = 100 * (latest - earliest) / earliest
                        st.metric("Price change", f"${latest:.2f}", f"{chg:+.2f}%")

                if not has_sentiment:
                    st.caption(
                        "News sentiment appears once scored articles exist for this ticker."
                    )
                if not has_social:
                    st.caption(
                        "Social data appears once Stocktwits signals exist for this ticker "
                        "-- populated by the pipeline, not fetched live on this chart."
                    )


# ============================================================================
# TAB 3 -- SOCIAL FEED (HERD RADAR)
# ============================================================================
with tab_social:
    st.markdown("#### Herd Radar -- Where Is Social Attention Right Now")
    st.caption(
        "Tickers ranked by current Stocktwits social activity, drawn from the "
        "pipeline's most recent runs. Bullish/bearish tags are self-reported by "
        "posters. Herd signal = 3+ posts containing keywords like whale, squeeze, "
        "or unusual flow."
    )

    rc1, rc2 = st.columns([1, 3])
    with rc1:
        radar_window = st.selectbox(
            "Activity within",
            ["1 hour", "4 hours", "12 hours", "24 hours"],
            index=3,
            key="radar_window"
        )
    radar_hours_map = {"1 hour": 1, "4 hours": 4, "12 hours": 12, "24 hours": 24}
    radar_hours = radar_hours_map[radar_window]
    radar_cutoff = (datetime.now(timezone.utc) - timedelta(hours=radar_hours)).isoformat()

    radar_rows = conn.execute("""
        SELECT ticker,
               MAX(detected_at)              AS last_seen,
               AVG(bullish_pct)               AS avg_bullish_pct,
               SUM(COALESCE(bullish_count,0)) AS bullish_ct,
               SUM(COALESCE(bearish_count,0)) AS bearish_ct,
               SUM(COALESCE(post_count,0))    AS total_posts,
               SUM(COALESCE(keyword_hits,0))  AS herd_hits,
               MAX(CASE WHEN signal_type='social_spike' THEN 1 ELSE 0 END) AS has_spike
        FROM   signals
        WHERE  signal_type IN ('social_spike','social_read')
          AND  detected_at >= ?
        GROUP  BY ticker
        ORDER  BY herd_hits DESC, total_posts DESC
    """, (radar_cutoff,)).fetchall()

    if not radar_rows:
        st.info(f"No social activity recorded in the last {radar_window}. Run the pipeline to populate.")
    else:
        st.markdown("""
            <div style="padding:6px 14px;display:flex;gap:12px;
                        border-bottom:1px solid #3d444d;color:#6e7681;font-size:12px;">
                <span style="min-width:70px;">Ticker</span>
                <span style="min-width:90px;">Messages</span>
                <span style="min-width:110px;">Bullish/Bearish</span>
                <span style="min-width:90px;">Bullish%</span>
                <span style="min-width:90px;">Herd hits</span>
                <span style="min-width:70px;text-align:right;">Last seen</span>
            </div>
        """, unsafe_allow_html=True)

        for row in radar_rows[:30]:
            (tk, last_seen, avg_bp, bull_ct, bear_ct, total_posts,
             herd_hits, has_spike) = row
            bp_str   = f"{avg_bp:.0%}" if avg_bp is not None else "--"
            bp_color = (
                "#3fb950" if avg_bp and avg_bp >= 0.6 else
                "#f85149" if avg_bp and avg_bp <= 0.4 else "#8b949e"
            )
            spike_flag = " (spike)" if has_spike else ""
            herd_color = "#e3b341" if herd_hits > 0 else "#6e7681"

            st.markdown(f"""
                <div style="padding:9px 14px;border-bottom:1px solid #262730;
                            display:flex;align-items:center;gap:12px;">
                    <span style="color:#58a6ff;font-weight:700;min-width:70px;">
                        {tk}{spike_flag}</span>
                    <span style="color:#8b949e;font-size:12px;min-width:90px;">
                        {int(total_posts)} total</span>
                    <span style="color:#8b949e;font-size:12px;min-width:110px;">
                        {int(bull_ct)} / {int(bear_ct)}</span>
                    <span style="color:{bp_color};font-weight:600;min-width:90px;">
                        {bp_str}</span>
                    <span style="color:{herd_color};font-weight:600;min-width:90px;">
                        {int(herd_hits)}</span>
                    <span style="color:#6e7681;font-size:11px;min-width:70px;text-align:right;">
                        {time_ago(last_seen)}</span>
                </div>
            """, unsafe_allow_html=True)

        st.caption(f"{len(radar_rows)} tickers with social activity in the last {radar_window}")

    st.divider()
    with st.expander("Look up a specific ticker live (not cached, fetches Stocktwits directly)"):
        social_ticker = st.text_input(
            "Ticker", placeholder="e.g. AAPL...", key="social_ticker_input"
        ).strip().upper()
        fetch_btn = st.button("Fetch stream", type="primary", key="fetch_social")

        if social_ticker and fetch_btn:
            with st.spinner(f"Fetching ${social_ticker} stream..."):
                try:
                    import requests as _req
                    r = _req.get(
                        f"https://api.stocktwits.com/api/2/streams/symbol/{social_ticker}.json",
                        headers={"User-Agent": "Mozilla/5.0"},
                        timeout=10,
                        params={"limit": 30},
                    )
                    sdata = r.json() if r.status_code == 200 else None
                    if r.status_code == 404:
                        st.warning(f"${social_ticker} not found on Stocktwits.")
                    elif r.status_code != 200:
                        st.error(f"HTTP {r.status_code}")
                except Exception as e:
                    st.error(f"Error: {e}")
                    sdata = None

            if sdata:
                msgs    = sdata.get("messages", [])
                bullish = sum(
                    1 for m in msgs
                    if ((m.get("entities") or {}).get("sentiment") or {}).get("basic") == "Bullish"
                )
                bearish = sum(
                    1 for m in msgs
                    if ((m.get("entities") or {}).get("sentiment") or {}).get("basic") == "Bearish"
                )
                tagged  = bullish + bearish
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Messages", len(msgs))
                m2.metric("Bullish",  bullish)
                m3.metric("Bearish",  bearish)
                m4.metric("Ratio",    f"{bullish/tagged:.0%}" if tagged else "--")


# ============================================================================
# TAB 4 -- TRADER ZONE LAUNCHER
# ============================================================================
with tab_trader:
    st.markdown("#### Trader Zone")
    st.caption(
        "Real-time ranked ticker screener with live price, sentiment velocity, "
        "unusual volume flags, market cap tiers, and fundamentals. "
        "Opens as a separate window you can move anywhere on your screen."
    )
    st.markdown("<br>", unsafe_allow_html=True)
    # URL is dynamic -- uses Railway public domain in production, localhost in development
    st.link_button("Open Trader Zone", url=f"{_base_url}/trader_zone")
    st.markdown("<br>", unsafe_allow_html=True)
    st.link_button("Company Deep Dive", url=f"{_base_url}/company_deep_dive")