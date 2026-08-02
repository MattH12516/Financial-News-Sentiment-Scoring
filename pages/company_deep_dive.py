"""
pages/ticker.py -- SentiFeed single-ticker detail page.

Everything known about one company in one place: live price, signal status,
the sentiment/density/price chart, recent news with reasoning, and social
herd activity.

Reachable directly at /ticker?ticker=AAPL, or by clicking a ticker in the
Trader Zone.
"""

import os
import sqlite3
import math
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import json
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf

import pipeline

EASTERN = ZoneInfo("America/New_York")

st.set_page_config(
    page_title="Ticker -- SentiFeed",
    layout="wide",
    page_icon="⚡",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .stApp { background-color: #0e1117; }

    /* Navigation lives in the app's own tabs and links, not the sidebar */
    [data-testid="stSidebarNav"] { display: none; }

    /* Navigation lives in the app's own tabs and links, not the sidebar */
    [data-testid="stSidebarNav"] { display: none; }

    /* Wider scrollbars -- easier to grab without a scroll wheel */
    ::-webkit-scrollbar { width: 18px; height: 18px; }
    ::-webkit-scrollbar-track { background: #0e1117; }
    ::-webkit-scrollbar-thumb {
        background: #3d444d; border-radius: 9px; border: 3px solid #0e1117;
    }
    ::-webkit-scrollbar-thumb:hover { background: #58a6ff; }

    .tk-headline { color: #58a6ff; font-size: 34px; font-weight: 700; }
    .tk-company  { color: #8b949e; font-size: 15px; }
    .tk-price    { color: #e6edf3; font-size: 30px; font-weight: 700; }
    .tk-chg-pos  { color: #3fb950; font-size: 18px; font-weight: 600; }
    .tk-chg-neg  { color: #f85149; font-size: 18px; font-weight: 600; }
    .tk-chg-neu  { color: #8b949e; font-size: 18px; font-weight: 600; }

    .fund-label { color: #6e7681; font-size: 11px; display: block; margin-bottom: 2px; }
    .fund-value { color: #e6edf3; font-size: 14px; font-weight: 600; }

    .news-row    { padding: 10px 0; border-bottom: 1px solid #1c1f26; }
    .news-time   { color: #6e7681; font-size: 11px; }
    .news-title  { color: #c9d1d9; font-size: 14px; text-decoration: none; }
    .news-title:hover { color: #58a6ff; }
    .news-reason { color: #8b949e; font-size: 12px; }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# HELPERS
# ============================================================================

def get_connection():
    return pipeline.get_db_connection()


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


def to_eastern(dt_series):
    """Convert a datetime series to Eastern. All chart traces go through this
    so they align on a shared axis -- mixing tz-aware and naive series causes
    visible misalignment in Plotly."""
    dt_series = pd.to_datetime(dt_series)
    if dt_series.dt.tz is None:
        dt_series = dt_series.dt.tz_localize("UTC")
    else:
        dt_series = dt_series.dt.tz_convert("UTC")
    return dt_series.dt.tz_convert(EASTERN)


def scale_marker_sizes(counts, min_size=6, max_size=22):
    """Scale sample-size counts into marker sizes so a reader can see at a
    glance whether a point rests on 2 articles or 50."""
    counts = pd.Series(counts).fillna(0)
    if counts.max() == counts.min():
        return [min_size] * len(counts)
    scaled = (min_size + (counts - counts.min()) /
              (counts.max() - counts.min()) * (max_size - min_size))
    return scaled.tolist()


def _fmt_num(val, fmt="{:.1f}"):
    """Format a possibly-string numeric value from yfinance safely."""
    if val is None or val == "":
        return "--"
    try:
        return fmt.format(float(val))
    except (ValueError, TypeError):
        return str(val)


def fmt_mc(mc):
    if not mc:
        return "--"
    try:
        mc = float(mc)
    except (ValueError, TypeError):
        return "--"
    if mc >= 1e12: return f"${mc/1e12:.1f}T"
    if mc >= 1e9:  return f"${mc/1e9:.1f}B"
    if mc >= 1e6:  return f"${mc/1e6:.0f}M"
    return f"${mc:,.0f}"


def get_cap_tier(mc):
    """Return (tier_key, display_label) from a market cap in dollars."""
    if not mc:
        return "unknown", "Unknown"
    if mc >= 200e9: return "mega",  "Mega cap ($200B+)"
    if mc >= 10e9:  return "large", "Large cap ($10B-$200B)"
    if mc >= 2e9:   return "mid",   "Mid cap ($2B-$10B)"
    if mc >= 300e6: return "small", "Small cap ($300M-$2B)"
    if mc >= 50e6:  return "micro", "Micro cap ($50M-$300M)"
    return "nano", "Nano cap (<$50M)"


# Relvol threshold each tier must clear to count as unusual volume
TIER_THRESHOLDS = {
    "mega": 1.3, "large": 1.5, "mid": 2.0,
    "small": 3.0, "micro": 5.0, "nano": 10.0, "unknown": 3.0,
}


@st.cache_data(ttl=300, show_spinner=False)
def load_snapshot(ticker):
    """Live price, change, volume, and market cap."""
    try:
        fi  = yf.Ticker(ticker).fast_info
        px  = fi.last_price
        prv = fi.previous_close
        if px and prv:
            return {
                "price":      round(px, 2),
                "chg_pct":    100 * (px - prv) / prv,
                "volume":     getattr(fi, "last_volume", None),
                "avg_vol":    getattr(fi, "three_month_average_volume", None),
                "market_cap": getattr(fi, "market_cap", None),
            }
    except Exception:
        pass
    return {}


@st.cache_data(ttl=3600, show_spinner=False)
def load_fundamentals(ticker):
    try:
        info = yf.Ticker(ticker).info
        return {
            "name":        info.get("longName") or info.get("shortName"),
            "pe":          info.get("trailingPE"),
            "fwd_pe":      info.get("forwardPE"),
            "market_cap":  info.get("marketCap"),
            "beta":        info.get("beta"),
            "sector":      info.get("sector"),
            "industry":    info.get("industry"),
            "short_float": info.get("shortPercentOfFloat"),
        }
    except Exception:
        return {}


@st.cache_data(ttl=900, show_spinner=False)
def _price_cached(ticker, period, interval):
    try:
        hist = yf.Ticker(ticker).history(period=period, interval=interval)
        if hist.empty:
            return pd.DataFrame()
        if hist.index.tz is None:
            hist.index = hist.index.tz_localize("UTC")
        else:
            hist.index = hist.index.tz_convert("UTC")
        df = hist[["Close", "Volume"]].reset_index()
        time_col = next(
            (c for c in df.columns
             if c.lower() in ("datetime", "date", "timestamp", "index")),
            df.columns[0]
        )
        return df.rename(columns={time_col: "ts", "Close": "price", "Volume": "volume"})
    except Exception:
        return pd.DataFrame()


def load_price(ticker, period, interval):
    """Fetch price with retries. Never caches an empty result -- a single
    transient Yahoo failure was previously frozen into cache for 15 minutes,
    making working tickers look broken."""
    import time as _t
    for attempt in range(3):
        df = _price_cached(ticker, period, interval)
        if not df.empty:
            return df
        _price_cached.clear()
        if attempt < 2:
            _t.sleep(0.4)
    return pd.DataFrame()


def load_sentiment_series(conn, ticker, since_iso):
    rows = conn.execute("""
        SELECT strftime('%Y-%m-%dT%H:00:00', tm.mentioned_at) AS hour,
               AVG(tm.score) AS avg_score, COUNT(*) AS mentions
        FROM   ticker_mentions tm
        WHERE  tm.ticker = ? AND tm.mentioned_at >= ? AND tm.score IS NOT NULL
        GROUP  BY hour ORDER BY hour
    """, (ticker, since_iso)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["hour", "avg_score", "mentions"])
    df = pd.DataFrame(rows, columns=["hour", "avg_score", "mentions"])
    df["hour"] = pd.to_datetime(df["hour"])
    return df


def load_density_series(conn, ticker, since_iso):
    rows = conn.execute("""
        SELECT strftime('%Y-%m-%dT%H:00:00', mentioned_at) AS hour,
               COUNT(*) AS mentions
        FROM   ticker_mentions
        WHERE  ticker = ? AND mentioned_at >= ?
        GROUP  BY hour ORDER BY hour
    """, (ticker, since_iso)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["hour", "mentions"])
    df = pd.DataFrame(rows, columns=["hour", "mentions"])
    df["hour"] = pd.to_datetime(df["hour"])
    return df


def load_social_series(conn, ticker, since_iso):
    try:
        rows = conn.execute("""
            SELECT strftime('%Y-%m-%dT%H:00:00', detected_at) AS hour,
                   AVG(bullish_pct)               AS avg_bullish_pct,
                   SUM(COALESCE(bullish_count,0)) AS bullish_ct,
                   SUM(COALESCE(bearish_count,0)) AS bearish_ct,
                   SUM(COALESCE(post_count,0))    AS total_posts
            FROM   signals
            WHERE  ticker = ? AND detected_at >= ?
              AND  signal_type IN ('social_spike','social_read')
            GROUP  BY hour ORDER BY hour
        """, (ticker, since_iso)).fetchall()
    except Exception:
        rows = []
    cols = ["hour", "avg_bullish_pct", "bullish_ct", "bearish_ct", "total_posts"]
    if not rows:
        return pd.DataFrame(columns=cols + ["sample_size"])
    df = pd.DataFrame(rows, columns=cols)
    df["hour"] = pd.to_datetime(df["hour"])
    df["sample_size"] = df["bullish_ct"] + df["bearish_ct"]
    df.loc[df["sample_size"] == 0, "sample_size"] = df["total_posts"]
    return df


def load_articles(conn, ticker, since_iso, limit=25):
    return conn.execute("""
        SELECT a.title, a.link, a.source,
               COALESCE(NULLIF(a.published, ''), a.ingested_at) AS ts,
               tm.score, tm.reasoning
        FROM   ticker_mentions tm
        JOIN   articles a ON a.id = tm.article_id
        WHERE  tm.ticker = ? AND tm.mentioned_at >= ?
        ORDER  BY ts DESC LIMIT ?
    """, (ticker, since_iso, limit)).fetchall()


def load_herd(conn, ticker, hours=24):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    try:
        row = conn.execute("""
            SELECT AVG(bullish_pct), SUM(COALESCE(bullish_count,0)),
                   SUM(COALESCE(bearish_count,0)), SUM(COALESCE(post_count,0)),
                   SUM(COALESCE(keyword_hits,0))
            FROM   signals
            WHERE  ticker = ? AND detected_at >= ?
              AND  signal_type IN ('social_spike','social_read')
        """, (ticker, cutoff)).fetchone()
        if row and row[3]:
            return {
                "bullish_pct": row[0], "bullish_ct": int(row[1] or 0),
                "bearish_ct": int(row[2] or 0), "total_posts": int(row[3] or 0),
                "herd_hits": int(row[4] or 0),
            }
    except Exception:
        pass
    return {}


def load_finviz_relvol(conn, ticker):
    """Most recent Finviz relative volume reading for this ticker."""
    try:
        row = conn.execute("""
            SELECT relvol, snapshot_at FROM relvol_history
            WHERE ticker = ? ORDER BY id DESC LIMIT 1
        """, (ticker,)).fetchone()
        if row and row[0] is not None:
            return {"relvol": row[0], "at": row[1]}
    except Exception:
        pass
    return {}


def load_recent_signals(conn, ticker, hours=48):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    try:
        rows = conn.execute("""
            SELECT signal_type, signal_value, detected_at, metadata
            FROM   signals
            WHERE  ticker = ? AND detected_at >= ?
            ORDER  BY detected_at DESC
        """, (ticker, cutoff)).fetchall()
        return [{
            "type": r[0], "value": r[1], "at": r[2],
            "meta": json.loads(r[3]) if r[3] else {},
        } for r in rows]
    except Exception:
        return []


# ============================================================================
# TICKER SELECTION
# ============================================================================

nav1, nav2, _ = st.columns([1, 1, 4])
with nav1:
    st.page_link("app.py", label="Back to SentiFeed", icon=":material/arrow_back:")
with nav2:
    st.page_link("pages/trader_zone.py", label="Trader Zone",
                 icon=":material/table_chart:")

st.markdown("## Company Deep Dive")

# Support /ticker?ticker=AAPL so Trader Zone rows can link straight here
_qp = st.query_params
_default = (_qp.get("ticker") or "").upper()

col_in, col_btn = st.columns([2, 6])
with col_in:
    ticker = st.text_input(
        "Ticker", value=_default, placeholder="e.g. AAPL"
    ).strip().upper()

if not ticker:
    st.info("Enter a ticker above to see everything known about it.")
    st.stop()

st.query_params["ticker"] = ticker

conn = get_connection()

# ============================================================================
# HEADER -- price and identity
# ============================================================================

snap = load_snapshot(ticker)
fund = load_fundamentals(ticker)

mc               = snap.get("market_cap") or fund.get("market_cap")
tier_key, tier_lbl = get_cap_tier(mc)

h1, h2, h3 = st.columns([2, 2, 4])
with h1:
    st.markdown(
        f'<div class="tk-headline">{ticker}</div>'
        f'<div class="tk-company">{fund.get("name") or ""}</div>',
        unsafe_allow_html=True
    )
with h2:
    if snap:
        chg     = snap["chg_pct"]
        chg_cls = ("tk-chg-pos" if chg > 0 else
                   "tk-chg-neg" if chg < 0 else "tk-chg-neu")
        st.markdown(
            f'<div class="tk-price">${snap["price"]:.2f}</div>'
            f'<div class="{chg_cls}">{chg:+.2f}% today</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div class="tk-price">--</div>'
            '<div class="tk-chg-neu">No live price available</div>',
            unsafe_allow_html=True
        )
with h3:
    st.markdown(
        f'<div class="tk-company" style="padding-top:8px;">'
        f'{tier_lbl} &nbsp;·&nbsp; {fmt_mc(mc)}<br>'
        f'{fund.get("sector") or ""}'
        f'{" -- " + fund.get("industry") if fund.get("industry") else ""}'
        f'</div>',
        unsafe_allow_html=True
    )

st.divider()

# ============================================================================
# SIGNAL STATUS
# ============================================================================

herd      = load_herd(conn, ticker)
fv        = load_finviz_relvol(conn, ticker)
sigs      = load_recent_signals(conn, ticker)
threshold = TIER_THRESHOLDS.get(tier_key, 3.0)

relvol    = fv.get("relvol")
on_vol_list = any(
    s["type"] in ("unusual_volume", "unusual_volume_squeeze") for s in sigs
)
has_whale = any(s["type"] == "form4_cluster" for s in sigs)

s1, s2, s3 = st.columns(3)
with s1:
    st.metric(
        "Relative volume",
        f"{relvol:.1f}x" if relvol else "--",
        help="Today's trading volume compared to this stock's normal volume."
    )
with s2:
    st.metric(
        "Social (24h)",
        f"{herd['bullish_pct']:.0%} bull" if herd.get("bullish_pct") is not None else "--",
        help=(f"{herd.get('bullish_ct',0)} bullish / {herd.get('bearish_ct',0)} bearish "
              f"messages, {herd.get('herd_hits',0)} herd keyword hits")
        if herd else "No social data recorded in the last 24h"
    )
with s3:
    st.metric("Whale buying", "Yes" if has_whale else "No",
              help="A cluster of 3+ SEC Form 4 insider purchases filed within a "
                   "24-hour period, detected at any point in the last 48 hours")

# Plain-language status read
if on_vol_list:
    st.success(
        f"**Confirmed by volume.** {ticker} appeared on the Finviz unusual volume "
        f"screener" + (f" at {relvol:.1f}x relative volume." if relvol else ".")
    )
elif relvol is not None:
    if herd.get("total_posts") or sigs:
        st.warning(
            f"**Signals present, volume not yet elevated.** {ticker} is trading at "
            f"{relvol:.1f}x its normal volume. News or social activity has been "
            "recorded, but the market has not moved unusually yet."
        )
    else:
        st.info(f"{ticker} is trading at {relvol:.1f}x normal volume, with no "
                "news or social signals recorded.")
else:
    st.info(f"No relative volume reading recorded for {ticker} yet.")

# ============================================================================
# WATCHLIST -- add this ticker with its signal state frozen
# ============================================================================

def ensure_watchlist(conn):
    """Create the table on demand so the page works even if the pipeline
    has not been run since the schema changed."""
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_ticker "
                 "ON watchlist(ticker, status)")
    conn.commit()


ensure_watchlist(conn)

_open_pick = conn.execute(
    "SELECT added_at, entry_price FROM watchlist "
    "WHERE ticker = ? AND status != 'closed' "
    "ORDER BY added_at DESC LIMIT 1", (ticker,)
).fetchone()

if _open_pick:
    st.caption(f"{ticker} is already on the watchlist -- added "
               f"{_open_pick[0][:10]} at ${_open_pick[1]:.2f} entry.")

with st.expander(f"Add {ticker} to watchlist", expanded=False):
    _live = snap.get("price") if snap else None
    if _live is None:
        try:
            _fi   = yf.Ticker(ticker).fast_info
            _live = _fi.get("last_price") if hasattr(_fi, "get") else None
            if _live is None:
                _live = getattr(_fi, "last_price", None)
            _live = float(_live) if _live else None
        except Exception:
            _live = None

    if _live:
        st.caption(f"Records {ticker} at ${_live:.2f}, with its signal state "
                   "frozen at this moment.")
    else:
        st.caption(f"No live price available for {ticker} right now -- it will "
                   "be recorded without a reference price.")

    _note = st.text_input("Note (optional)",
                          placeholder="Anything worth remembering.")

    if st.button("Add to watchlist", type="primary", key="wl_add"):
        _since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        _sent  = conn.execute("""
            SELECT AVG(score), COUNT(*) FROM ticker_mentions
            WHERE  ticker = ? AND score IS NOT NULL AND mentioned_at >= ?
        """, (ticker, _since)).fetchone()

        _snapshot = {
            "price":            _live,
            "chg_pct":          snap.get("chg_pct") if snap else None,
            "relvol":           relvol,
            "relvol_threshold": threshold,
            "cap_tier":         tier_lbl,
            "market_cap":       mc,
            "avg_sentiment":    _sent[0],
            "mentions_24h":     _sent[1],
            "bullish_pct":      herd.get("bullish_pct"),
            "bullish_ct":       herd.get("bullish_ct"),
            "bearish_ct":       herd.get("bearish_ct"),
            "herd_hits":        herd.get("herd_hits"),
            "total_posts":      herd.get("total_posts"),
            "on_volume_list":   on_vol_list,
            "whale_buying":     has_whale,
            "signal_types":     sorted({s["type"] for s in sigs}),
        }

        conn.execute("""
            INSERT INTO watchlist
            (ticker, added_at, added_price, entry_price, thesis,
             snapshot_json, status)
            VALUES (?,?,?,?,?,?,'watching')
        """, (ticker, datetime.now(timezone.utc).isoformat(),
              _live, _live, _note.strip() or None, json.dumps(_snapshot)))
        conn.commit()
        st.success(f"{ticker} added at ${_live:.2f}." if _live
                   else f"{ticker} added.")
        st.rerun()
# ============================================================================
# CHART
# ============================================================================

st.markdown("#### Sentiment / Density / Price")

tf_map = {
    "10 min":  {"minutes": 10,    "period": "1d",  "interval": "1m"},
    "30 min":  {"minutes": 30,    "period": "1d",  "interval": "1m"},
    "1 hour":  {"minutes": 60,    "period": "1d",  "interval": "1m"},
    "1 day":   {"minutes": 1440,  "period": "1d",  "interval": "5m"},
    "1 week":  {"minutes": 10080, "period": "5d",  "interval": "30m"},
    "1 month": {"minutes": 43200, "period": "1mo", "interval": "1h"},
}
timeframe = st.radio("Timeframe", list(tf_map.keys()), index=3, horizontal=True)
tf        = tf_map[timeframe]
since_dt  = datetime.now(timezone.utc) - timedelta(minutes=tf["minutes"])
since_iso = since_dt.isoformat()

# Short windows are meaningless outside market hours -- say so rather than
# letting an empty chart look like a bug.
_now_et      = datetime.now(timezone.utc).astimezone(EASTERN)
_mins_now    = _now_et.hour * 60 + _now_et.minute
_market_open = _now_et.weekday() < 5 and (9 * 60 + 30) <= _mins_now <= (16 * 60)
if timeframe in ("10 min", "30 min", "1 hour") and not _market_open:
    st.caption(
        "Market is currently closed -- short intraday windows will show no new "
        "price data until 9:30 AM ET on the next trading day."
    )

sentiment_df = load_sentiment_series(conn, ticker, since_iso)
density_df   = load_density_series(conn, ticker, since_iso)
social_df    = load_social_series(conn, ticker, since_iso)
price_df     = load_price(ticker, tf["period"], tf["interval"])

has_sent   = not sentiment_df.empty
has_dens   = not density_df.empty
has_social = not social_df.empty and social_df["avg_bullish_pct"].notna().any()
has_price  = not price_df.empty

if not has_price:
    st.caption(
        f"No price data available for **{ticker}** at this interval. Yahoo Finance "
        "coverage is inconsistent for lower-volume tickers at fine intervals -- "
        "this is a data gap, not an app error. Try a wider timeframe."
    )

if not (has_sent or has_dens or has_social or has_price):
    st.info(f"No data found for {ticker} in this timeframe yet.")
else:
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    if has_dens:
        density_df["hour_et"] = to_eastern(density_df["hour"])
        fig.add_trace(go.Bar(
            x=density_df["hour_et"], y=density_df["mentions"],
            name="News density",
            marker_color="rgba(251,140,0,0.8)",
            hovertemplate="%{y} articles<extra></extra>",
        ), secondary_y=False)

    if has_sent:
        sentiment_df["hour_et"] = to_eastern(sentiment_df["hour"])
        fig.add_trace(go.Scatter(
            x=sentiment_df["hour_et"], y=sentiment_df["avg_score"],
            name="Avg news sentiment", mode="lines+markers",
            line=dict(color="#58a6ff", width=2),
            marker=dict(size=scale_marker_sizes(sentiment_df["mentions"]),
                        line=dict(width=1, color="#0e1117")),
            customdata=sentiment_df["mentions"],
            hovertemplate="%{y:+.2f} (%{customdata} articles)<extra></extra>",
        ), secondary_y=False)
        fig.add_hline(y=0, line_dash="dot",
                      line_color="rgba(139,148,158,0.4)", secondary_y=False)

    if has_social:
        social_df["hour_et"] = to_eastern(social_df["hour"])
        fig.add_trace(go.Scatter(
            x=social_df["hour_et"], y=social_df["avg_bullish_pct"],
            name="Social bullish%", mode="lines+markers",
            line=dict(color="#bc8cff", width=2, dash="dot"),
            marker=dict(size=scale_marker_sizes(social_df["sample_size"]),
                        line=dict(width=1, color="#0e1117")),
            customdata=list(zip(social_df["bullish_ct"], social_df["bearish_ct"])),
            hovertemplate=("%{y:.0%} bullish<br>%{customdata[0]:.0f} bullish / "
                           "%{customdata[1]:.0f} bearish<extra></extra>"),
        ), secondary_y=False)

    if has_price:
        price_df["ts_et"] = to_eastern(price_df["ts"])
        trimmed = price_df[price_df["ts_et"] >= since_dt.astimezone(EASTERN)]
        if trimmed.empty:
            trimmed = price_df
        fig.add_trace(go.Scatter(
            x=trimmed["ts_et"], y=trimmed["price"],
            name="Stock price", mode="lines",
            line=dict(color="#3fb950", width=2),
            hovertemplate="$%{y:.2f}<extra></extra>",
        ), secondary_y=True)

    fig.update_layout(
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
        font=dict(color="#8b949e"),
        legend=dict(bgcolor="#1c1f26", bordercolor="#262730", borderwidth=1),
        hovermode="x unified", height=460,
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(gridcolor="#1c1f26", showgrid=True, title="Eastern time"),
        yaxis=dict(gridcolor="#1c1f26", showgrid=True, title="Sentiment / Density"),
        barmode="overlay",
        yaxis2=dict(title="Price (USD)", showgrid=False),
    )
    st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# FUNDAMENTALS
# ============================================================================

if any(fund.values()):
    f1, f2, f3, f4, f5 = st.columns(5)
    with f1:
        st.markdown(f'<span class="fund-label">Market cap</span>'
                    f'<span class="fund-value">{fmt_mc(mc)}</span>',
                    unsafe_allow_html=True)
    with f2:
        st.markdown(f'<span class="fund-label">P/E (TTM)</span>'
                    f'<span class="fund-value">{_fmt_num(fund.get("pe"))}</span>',
                    unsafe_allow_html=True)
    with f3:
        st.markdown(f'<span class="fund-label">Forward P/E</span>'
                    f'<span class="fund-value">{_fmt_num(fund.get("fwd_pe"))}</span>',
                    unsafe_allow_html=True)
    with f4:
        st.markdown(f'<span class="fund-label">Beta</span>'
                    f'<span class="fund-value">'
                    f'{_fmt_num(fund.get("beta"), "{:.2f}")}</span>',
                    unsafe_allow_html=True)
    with f5:
        st.markdown(f'<span class="fund-label">Short float</span>'
                    f'<span class="fund-value">'
                    f'{_fmt_num(fund.get("short_float"), "{:.1%}")}</span>',
                    unsafe_allow_html=True)

st.divider()

# ============================================================================
# NEWS
# ============================================================================

st.markdown(f"#### Recent news -- last {timeframe}")

articles = load_articles(conn, ticker, since_iso)
if not articles:
    st.caption(
        f"No articles for {ticker} in this window. Try a wider timeframe, or "
        "run the pipeline to fetch fresh news."
    )
else:
    for title, link, source, ts, score, reasoning in articles:
        sc       = score or 0
        sc_color = ("#3fb950" if sc > 0.05 else
                    "#f85149" if sc < -0.05 else "#8b949e")
        sc_str   = f"{score:+.2f}" if score is not None else "--"
        st.markdown(f"""
            <div class="news-row">
                <span class="news-time">{fmt_time(ts)} &nbsp;·&nbsp; {source}
                &nbsp;·&nbsp;
                <span style="color:{sc_color};font-weight:700;">{sc_str}</span>
                </span><br>
                <a class="news-title" href="{link}" target="_blank">{title}</a>
                {"<br><span class='news-reason'>" + reasoning + "</span>"
                 if reasoning else ""}
            </div>
        """, unsafe_allow_html=True)
    st.caption(f"{len(articles)} articles shown")

# ============================================================================
# SIGNAL HISTORY
# ============================================================================

st.divider()
st.markdown("#### Signal history (last 48h)")

if not sigs:
    st.caption("No signals recorded for this ticker in the last 48 hours.")
else:
    label_map = {
        "social_spike":           "Herd keyword spike",
        "social_read":            "Social reading",
        "form4_cluster":          "Whale buying (3+ Form 4 purchases)",
        "unusual_volume":         "Unusual volume",
        "unusual_volume_squeeze": "Unusual volume + squeeze setup",
    }
    for s in sigs[:30]:
        detail = ""
        if s["type"].startswith("unusual_volume") and s["meta"].get("relvol"):
            detail = f" -- {s['meta']['relvol']:.1f}x"
        elif s["type"] == "form4_cluster" and s["value"]:
            detail = f" -- {int(s['value'])} filings"
        st.markdown(
            f'<div style="padding:5px 0;border-bottom:1px solid #1c1f26;">'
            f'<span style="color:#6e7681;font-size:11px;">{fmt_time(s["at"])}</span>'
            f'&nbsp;&nbsp;<span style="color:#c9d1d9;font-size:13px;">'
            f'{label_map.get(s["type"], s["type"])}{detail}</span></div>',
            unsafe_allow_html=True
        )