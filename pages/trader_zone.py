"""
pages/trader_zone.py -- SentiFeed Trader Zone (pop-out page)

Open via the "Open Trader Zone" button in the main dashboard.
Runs as a separate Streamlit page with its own auto-refresh loop.

Features:
  - Composite / score / density ranking with market cap filter
  - Live price + intraday change via yfinance
  - Sentiment velocity arrow (improving / declining / flat)
  - Unusual volume flag with live relvol computed from yfinance
  - Alert threshold row highlighting
  - Six-factor signal scoring (0-6)
  - Drill-down: signal breakdown, relvol trend, fundamentals, articles
  - Currently on Unusual Volume section (Finviz data)
  - Pre-Signal Candidates watchlist (signals fired, volume not yet confirmed)
"""

import os
import sqlite3
import math
import sys
import subprocess
from datetime import datetime, timezone, timedelta

import json
import streamlit as st
import yfinance as yf

try:
    from streamlit_autorefresh import st_autorefresh as _st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

import pipeline

st.set_page_config(
    page_title="Trader Zone -- SentiFeed",
    layout="wide",
    page_icon="⚡",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .stApp { background-color: #0e1117; }

    /* Standard ticker row */
    .tz-row {
        padding: 10px 14px; border-bottom: 1px solid #262730;
        display: flex; align-items: center; gap: 12px;
    }
    /* Highlighted row for tickers above the alert threshold */
    .tz-alert-row {
        padding: 10px 14px; border-bottom: 1px solid #262730;
        display: flex; align-items: center; gap: 12px;
        background: rgba(63,185,80,0.06); border-left: 3px solid #3fb950;
    }

    /* Ticker row element styles */
    .tz-rank   { color: #6e7681; font-size: 13px; min-width: 24px; text-align: right; }
    .tz-ticker { color: #58a6ff; font-size: 15px; font-weight: 700; min-width: 58px; }
    .tz-score-pos { color: #3fb950; font-size: 14px; font-weight: 700; min-width: 50px; }
    .tz-score-neg { color: #f85149; font-size: 14px; font-weight: 700; min-width: 50px; }
    .tz-score-neu { color: #8b949e; font-size: 14px; font-weight: 700; min-width: 50px; }
    .tz-density {
        background: #1f3a5f; color: #58a6ff; padding: 2px 8px;
        border-radius: 4px; font-size: 12px; font-weight: 600; white-space: nowrap;
    }

    /* Velocity arrow colors */
    .tz-vel-up   { color: #3fb950; font-size: 14px; min-width: 18px; text-align: center; font-weight: 700; }
    .tz-vel-down { color: #f85149; font-size: 14px; min-width: 18px; text-align: center; font-weight: 700; }
    .tz-vel-flat { color: #6e7681; font-size: 14px; min-width: 18px; text-align: center; }

    /* Price and change columns */
    .tz-vol-flag { font-size: 13px; min-width: 18px; text-align: center; }
    .tz-price    { color: #e6edf3; font-size: 13px; min-width: 72px; text-align: right; }
    .tz-chg-pos  { color: #3fb950; font-size: 13px; min-width: 62px; text-align: right; font-weight: 600; }
    .tz-chg-neg  { color: #f85149; font-size: 13px; min-width: 62px; text-align: right; font-weight: 600; }
    .tz-chg-neu  { color: #8b949e; font-size: 13px; min-width: 62px; text-align: right; }
    .tz-rs       { color: #6e7681; font-size: 12px; min-width: 54px; text-align: right; }

    /* Drill-down article and fundamentals styles */
    .tz-article-title { color: #c9d1d9; font-size: 13px; }
    .tz-article-meta  { color: #6e7681; font-size: 11px; }
    .reasoning-text   { color: #8b949e; font-size: 13px; }
    .fund-label { color: #6e7681; font-size: 11px; display: block; margin-bottom: 2px; }
    .fund-value { color: #e6edf3; font-size: 14px; font-weight: 600; }

    /* Signal score badge colors */
    .sig-high { background:#1a3a2a; color:#3fb950; padding:2px 8px; border-radius:4px; font-size:12px; font-weight:700; white-space:nowrap; }
    .sig-med  { background:#3a2a1a; color:#e3b341; padding:2px 8px; border-radius:4px; font-size:12px; font-weight:700; white-space:nowrap; }
    .sig-low  { background:#1c1f26; color:#6e7681; padding:2px 8px; border-radius:4px; font-size:12px; font-weight:700; white-space:nowrap; }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# DATABASE AND DATA HELPERS
# ============================================================================

def get_connection():
    """Return a SQLite connection to the pipeline database."""
    return sqlite3.connect(pipeline.DB_PATH, check_same_thread=False)


def time_ago(iso_str):
    """Convert an ISO timestamp to a human-readable relative time string."""
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


def load_trader_zone(conn, since_iso, use_weighted, sort_mode="composite"):
    """Query ranked ticker data from the database.

    sort_mode options:
      'composite' -- sentiment x log(1 + density), rewards both quality and volume
      'score'     -- pure average sentiment score
      'density'   -- pure mention volume

    SQLite has no LOG() function so sorting is performed in Python.
    """
    density_expr = (
        "SUM(COALESCE(tm.weight, 1.0))" if use_weighted
        else "CAST(COUNT(*) AS REAL)"
    )
    rows = conn.execute(f"""
        SELECT tm.ticker, AVG(tm.score) AS score,
               {density_expr} AS density, COUNT(*) AS raw_count
        FROM   ticker_mentions tm
        WHERE  tm.mentioned_at >= ? AND tm.score IS NOT NULL
        GROUP  BY tm.ticker
    """, (since_iso,)).fetchall()

    if sort_mode == "composite":
        rows = sorted(
            rows, key=lambda r: (r[1] or 0) * math.log(1 + (r[2] or 0)), reverse=True
        )
    elif sort_mode == "density":
        rows = sorted(rows, key=lambda r: r[2] or 0, reverse=True)
    else:
        rows = sorted(rows, key=lambda r: r[1] or 0, reverse=True)
    return rows


def load_velocity(conn, ticker, since_iso, window_minutes):
    """Compute sentiment velocity as the change from the first half to the second half
    of the selected time window. Returns positive float if improving, negative if declining."""
    half   = (datetime.now(timezone.utc) - timedelta(minutes=window_minutes // 2)).isoformat()
    recent = conn.execute("""
        SELECT AVG(score) FROM ticker_mentions
        WHERE ticker=? AND mentioned_at>=? AND score IS NOT NULL
    """, (ticker, half)).fetchone()[0]
    early  = conn.execute("""
        SELECT AVG(score) FROM ticker_mentions
        WHERE ticker=? AND mentioned_at>=? AND mentioned_at<? AND score IS NOT NULL
    """, (ticker, since_iso, half)).fetchone()[0]
    if recent is None or early is None:
        return None
    return recent - early


def load_ticker_articles(conn, ticker, since_iso):
    """Return the 8 most recent articles mentioning a ticker within the time window."""
    return conn.execute("""
        SELECT a.title, a.link, a.source, a.ingested_at, tm.score, tm.reasoning
        FROM   ticker_mentions tm
        JOIN   articles a ON a.id = tm.article_id
        WHERE  tm.ticker=? AND tm.mentioned_at>=?
        ORDER  BY tm.mentioned_at DESC LIMIT 8
    """, (ticker, since_iso)).fetchall()


from concurrent.futures import ThreadPoolExecutor, as_completed


def _fetch_one_snapshot(tk):
    """Fetch a single ticker's price snapshot. Returns (ticker, data_or_None)."""
    try:
        fi  = yf.Ticker(tk).fast_info
        px  = fi.last_price
        prv = fi.previous_close
        if px and prv:
            return tk, {
                "price":      round(px, 2),
                "chg_pct":    100 * (px - prv) / prv,
                "volume":     getattr(fi, "last_volume", None),
                "avg_vol":    getattr(fi, "three_month_average_volume", None),
                "market_cap": getattr(fi, "market_cap", None),
            }
    except Exception:
        pass
    return tk, None


@st.cache_data(ttl=300, show_spinner=False)
def load_price_snapshot(ticker_tuple):
    """Fetch live price, change, volume, and market cap for a batch of tickers
    concurrently. Capped at 100 tickers to avoid yfinance rate limiting.
    Cached 5 minutes. Only successful fetches are cached -- a transient yfinance
    failure on one run won't get frozen into the cache for the full TTL."""
    result = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(_fetch_one_snapshot, tk) for tk in ticker_tuple]
        for future in as_completed(futures):
            tk, data = future.result()
            if data:
                result[tk] = data
    return result

@st.cache_data(ttl=3600, show_spinner=False)
def load_fundamentals(ticker):
    """Fetch PE, market cap, beta, sector, and short float from yfinance.
    Cached 1 hour since these values change infrequently."""
    try:
        info = yf.Ticker(ticker).info
        return {
            "pe":          info.get("trailingPE"),
            "fwd_pe":      info.get("forwardPE"),
            "market_cap":  info.get("marketCap"),
            "beta":        info.get("beta"),
            "sector":      info.get("sector"),
            "short_float": info.get("shortPercentOfFloat"),
        }
    except Exception:
        return {}


def fmt_market_cap(mc):
    """Format a market cap in dollars to a short human-readable string."""
    if not mc: return "--"
    if mc >= 1e12: return f"${mc/1e12:.1f}T"
    if mc >= 1e9:  return f"${mc/1e9:.1f}B"
    if mc >= 1e6:  return f"${mc/1e6:.1f}M"
    return f"${mc:,.0f}"


@st.cache_data(ttl=300, show_spinner=False)
def load_social_badges(ticker_tuple):
    """Fetch the most recent social signal reading per ticker for the row badges."""
    result = {}
    try:
        conn = get_connection()
        for tk in ticker_tuple:
            row = conn.execute("""
                SELECT bullish_pct, keyword_hits, signal_type
                FROM   signals
                WHERE  ticker = ?
                ORDER  BY detected_at DESC LIMIT 1
            """, (tk,)).fetchone()
            if row:
                result[tk] = {
                    "bullish_pct":  row[0],
                    "keyword_hits": row[1],
                    "signal_type":  row[2],
                }
    except Exception:
        pass
    return result


@st.cache_data(ttl=120, show_spinner=False)
def load_all_signals_batch(ticker_tuple, since_iso):
    """Fetch all signals for a batch of tickers in a single query."""
    result = {}
    try:
        conn = get_connection()
        ph   = ",".join("?" for _ in ticker_tuple)
        rows = conn.execute(f"""
            SELECT ticker, signal_type, signal_value,
                   bullish_pct, keyword_hits, detected_at, metadata
            FROM   signals
            WHERE  ticker IN ({ph})
              AND  detected_at >= ?
            ORDER  BY detected_at DESC
        """, (*ticker_tuple, since_iso)).fetchall()
        for ticker, stype, sval, bull, kw, at, meta in rows:
            result.setdefault(ticker, []).append({
                "type":        stype,
                "value":       sval,
                "bullish_pct": bull,
                "kw_hits":     kw,
                "at":          at,
                "meta":        json.loads(meta) if meta else {},
            })
    except Exception:
        pass
    return result


def compute_signal_score(tk, news_score, signals_list):
    """Evaluate six independent signal factors for a ticker.
    Returns (points, flags) where points is 0-6 and flags is a list of
    (icon, description) tuples for display in the drill-down.

    The six factors are:
    1. News sentiment > 0.3 (professional news coverage)
    2. Social bullish% >= 60% (retail crowd sentiment)
    3. Herd keyword spike (crowd behavior language detected)
    4. On Finviz unusual volume list (market confirmation)
    5. Form 4 insider buying cluster (insider behavior)
    6. Short squeeze setup (short float > 10% and days to cover > 5)
    """
    points = 0
    flags  = []

    # Factor 1: News sentiment from LLM-scored articles
    if news_score is not None and news_score > 0.3:
        points += 1
        flags.append(("✅", f"News sentiment {news_score:+.2f}"))
    elif news_score is not None and news_score > 0.05:
        flags.append(("🔶", f"News sentiment {news_score:+.2f} -- weak positive"))
    else:
        ns = f"{news_score:+.2f}" if news_score is not None else "--"
        flags.append(("❌", f"News sentiment {ns}"))

    # Factor 2: Social bullish% from Stocktwits self-reported tags
    bull = next((s["bullish_pct"] for s in signals_list
                 if s.get("bullish_pct") is not None), None)
    if bull is not None and bull >= 0.6:
        points += 1
        flags.append(("✅", f"Social {bull:.0%} bullish"))
    elif bull is not None:
        flags.append(("🔶", f"Social {bull:.0%} bullish -- below 60% threshold"))
    else:
        flags.append(("⬜", "Social -- no data yet"))

    # Factor 3: Herd keyword spike in Stocktwits messages
    if any(s["type"] == "social_spike" for s in signals_list):
        points += 1
        flags.append(("✅", "Herd keyword spike detected"))
    else:
        flags.append(("❌", "No herd spike"))

    # Factor 4: Unusual volume confirmed by Finviz screener
    fv_sig = next(
        (s for s in signals_list
         if s["type"] in ("unusual_volume", "unusual_volume_squeeze")),
        None
    )
    if fv_sig:
        rv          = fv_sig["meta"].get("relvol")
        rv_str      = f"{rv:.1f}x" if rv else ""
        squeeze_tag = " -- squeeze setup" if fv_sig["type"] == "unusual_volume_squeeze" else ""
        points += 1
        flags.append(("✅", f"Unusual volume {rv_str}{squeeze_tag}"))
    else:
        flags.append(("❌", "Not on Finviz unusual volume list"))

    # Factor 5: Cluster of Form 4 insider filings in the last 24 hours
    if any(s["type"] == "form4_cluster" for s in signals_list):
        points += 1
        flags.append(("✅", "Insider buying cluster (Form 4s)"))
    else:
        flags.append(("⬜", "No insider cluster"))

    # Factor 6: Short squeeze potential -- trapped shorts amplify upward moves
    sf = fv_sig["meta"].get("short_float") if fv_sig else None
    sr = fv_sig["meta"].get("short_ratio") if fv_sig else None
    if sf is not None and sr is not None and sf > 10 and sr > 5:
        points += 1
        flags.append(("✅", f"Squeeze setup: {sf:.1f}% short float, {sr:.1f} days to cover"))
    elif sf is not None:
        flags.append(("⬜", f"Short float {sf:.1f}% -- no squeeze setup"))
    else:
        flags.append(("⬜", "Short data not available"))

    return points, flags


def infer_signal_reason(signals_list, mentions=0, avg_score=None):
    """Return a short plain-English description of why this ticker is on the radar."""
    parts = []
    if any(s["type"] == "form4_cluster" for s in signals_list):
        parts.append("insider buying cluster")
    if any(s["type"] == "social_spike" for s in signals_list):
        parts.append("retail herd spike")
    if mentions >= 3:
        parts.append(f"{mentions} news articles")
    elif mentions > 0:
        parts.append(f"{mentions} news mention{'s' if mentions > 1 else ''}")
    fv = next(
        (s for s in signals_list
         if s["type"] in ("unusual_volume", "unusual_volume_squeeze")),
        None
    )
    if fv and fv.get("meta", {}).get("news_title"):
        parts.append(f'"{fv["meta"]["news_title"][:40]}"')
    return " · ".join(parts) if parts else "social/news activity"


def get_signal_stage(signals_list):
    """Return (label, color) indicating where in the signal chain this ticker sits.

    Pre-volume:   social/news/insider signals fired, volume not yet confirmed
    Volume only:  unusual volume present but no prior social/news signals
    Signal led:   signals preceded volume by X hours (the ideal early-detection case)
    Simultaneous: signals and volume appeared at the same time
    """
    vol_sigs   = [s for s in signals_list
                  if s["type"] in ("unusual_volume", "unusual_volume_squeeze")]
    other_sigs = [s for s in signals_list
                  if s["type"] not in ("unusual_volume", "unusual_volume_squeeze")]

    if not other_sigs and not vol_sigs:
        return "--", "#6e7681"
    if other_sigs and not vol_sigs:
        return "Pre-volume", "#e3b341"
    if vol_sigs and not other_sigs:
        return "Volume only", "#8b949e"
    try:
        first_sig = min(datetime.fromisoformat(s["at"]) for s in other_sigs)
        first_vol = min(datetime.fromisoformat(s["at"]) for s in vol_sigs)
        delta_h   = (first_vol - first_sig).total_seconds() / 3600
        if delta_h >= 1:
            return f"Signal led {delta_h:.0f}h early", "#3fb950"
        return "Simultaneous", "#bc8cff"
    except Exception:
        return "Confirmed", "#3fb950"


@st.cache_data(ttl=300, show_spinner=False)
def load_relvol_trend(_conn, ticker):
    """Return the last 5 relvol snapshots for a ticker, most recent first."""
    try:
        rows = _conn.execute("""
            SELECT relvol FROM relvol_history
            WHERE ticker = ?
            ORDER BY snapshot_at DESC LIMIT 5
        """, (ticker,)).fetchall()
        return [r[0] for r in rows if r[0] is not None]
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def load_pre_signal_candidates(_conn, lookback_days=7, vol_lookback_hours=48):
    """Return tickers with social/news/insider signals not yet on the Finviz unusual
    volume list. These are early-stage watchlist candidates scored 0-4."""
    since_sig = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    since_vol = (datetime.now(timezone.utc) - timedelta(hours=vol_lookback_hours)).isoformat()
    try:
        vol_tickers = {r[0] for r in _conn.execute("""
            SELECT DISTINCT ticker FROM signals
            WHERE signal_type IN ('unusual_volume','unusual_volume_squeeze')
              AND detected_at >= ?
        """, (since_vol,)).fetchall()}

        rows = _conn.execute("""
            SELECT ticker, signal_type, signal_value, bullish_pct,
                   keyword_hits, detected_at, metadata
            FROM signals
            WHERE detected_at >= ?
              AND signal_type NOT IN ('unusual_volume','unusual_volume_squeeze')
            ORDER BY detected_at DESC
        """, (since_sig,)).fetchall()

        by_ticker = {}
        for ticker, stype, sval, bull, kw, at, meta in rows:
            if ticker in vol_tickers:
                continue
            by_ticker.setdefault(ticker, []).append({
                "type": stype, "value": sval, "bullish_pct": bull,
                "kw_hits": kw, "at": at,
                "meta": json.loads(meta) if meta else {},
            })

        candidates = []
        for ticker, sigs in by_ticker.items():
            news_row  = _conn.execute("""
                SELECT COUNT(*), AVG(score) FROM ticker_mentions
                WHERE ticker = ? AND mentioned_at >= ? AND score IS NOT NULL
            """, (ticker, since_sig)).fetchone()
            mentions  = news_row[0] or 0
            avg_score = news_row[1]
            has_spike = any(s["type"] == "social_spike"  for s in sigs)
            has_form4 = any(s["type"] == "form4_cluster" for s in sigs)
            bull_pct  = next(
                (s["bullish_pct"] for s in sigs if s.get("bullish_pct") is not None),
                None
            )
            sig_score = sum([
                1 if avg_score and avg_score > 0.1 else 0,
                1 if bull_pct  and bull_pct  >= 0.6 else 0,
                1 if has_spike else 0,
                1 if has_form4 else 0,
            ])
            candidates.append({
                "ticker":    ticker,
                "signals":   sigs,
                "mentions":  mentions,
                "avg_score": avg_score,
                "bull_pct":  bull_pct,
                "has_spike": has_spike,
                "has_form4": has_form4,
                "score":     sig_score,
                "first_at":  min(s["at"] for s in sigs),
                "reason":    infer_signal_reason(sigs, mentions, avg_score),
            })
        return sorted(candidates, key=lambda x: x["score"], reverse=True)
    except Exception:
        return []


def get_cap_tier(mc):
    """Return (tier_key, display_label, short_label) from a market cap in dollars."""
    if not mc:
        return "unknown", "Unknown", "--"
    if mc >= 200e9: return "mega",  "Mega cap",  "Mega $200B+"
    if mc >= 10e9:  return "large", "Large cap", "Large $10B-$200B"
    if mc >= 2e9:   return "mid",   "Mid cap",   "Mid $2B-$10B"
    if mc >= 300e6: return "small", "Small cap", "Small $300M-$2B"
    if mc >= 50e6:  return "micro", "Micro cap", "Micro $50M-$300M"
    return "nano", "Nano cap", "Nano <$50M"


def fmt_mc(mc):
    """Format a market cap in dollars to a short human-readable string."""
    if not mc: return "--"
    if mc >= 1e12: return f"${mc/1e12:.1f}T"
    if mc >= 1e9:  return f"${mc/1e9:.1f}B"
    if mc >= 1e6:  return f"${mc/1e6:.0f}M"
    return f"${mc:,.0f}"


@st.cache_data(ttl=120, show_spinner=False)
def load_unusual_volume_now(_conn, lookback_hours=12):
    """Return all tickers on the Finviz unusual volume list from the most recent
    pipeline run. Deduplicates by ticker, keeping the highest relvol entry."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    try:
        rows = _conn.execute("""
            SELECT ticker, signal_type, signal_value, detected_at, metadata
            FROM   signals
            WHERE  signal_type IN ('unusual_volume','unusual_volume_squeeze')
              AND  detected_at >= ?
            ORDER  BY signal_value DESC
        """, (cutoff,)).fetchall()
        result = []
        seen   = set()
        for ticker, stype, relvol, det_at, meta in rows:
            if ticker in seen:
                continue
            seen.add(ticker)
            m = json.loads(meta) if meta else {}
            result.append({
                "ticker":      ticker,
                "signal_type": stype,
                "relvol":      relvol,
                "detected_at": det_at,
                "company":     m.get("company", ""),
                "price":       m.get("price"),
                "change":      m.get("change"),
                "short_float": m.get("short_float"),
                "short_ratio": m.get("short_ratio"),
                "cap_tier":    m.get("cap_display", "--"),
                "market_cap_m":m.get("market_cap_m"),
                "relvol_min":  m.get("relvol_min"),
                "news_title":  m.get("news_title", ""),
                "news_url":    m.get("news_url", ""),
                "squeeze":     stype == "unusual_volume_squeeze",
            })
        return result
    except Exception:
        return []


def sentiment_bar_html(score):
    """Return an HTML gradient bar representing the sentiment score.
    Bar spans -1.0 (red) to 0 (grey) to +1.0 (green) with a white position marker."""
    pct = (score + 1) / 2 * 100
    return (
        f'<div style="flex:1;background:linear-gradient(to right,'
        f'#f85149 0%,#6e7681 50%,#3fb950 100%);border-radius:4px;'
        f'height:7px;position:relative;min-width:90px;"'
        f' title="Sentiment: {score:+.2f}">'
        f'<div style="position:absolute;left:calc({pct:.1f}% - 2px);'
        f'width:4px;height:7px;background:#fff;border-radius:2px;"></div>'
        f'</div>'
    )


# ============================================================================
# PAGE HEADER
# ============================================================================

st.markdown("## Trader Zone")

conn = get_connection()

# ============================================================================
# CONTROLS
# ============================================================================

c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 2])

with c1:
    window_label = st.radio(
        "Time window", ["30 min", "1 hour", "4 hours", "24 hours"], index=1
    )
with c2:
    use_weighted = st.radio("Density mode", ["Raw mentions", "Weighted"]) == "Weighted"
with c3:
    sort_label = st.radio(
        "Rank by", ["Composite", "Score", "Density"],
        help="Composite = sentiment x log(1 + density)"
    )
    sort_mode  = sort_label.lower().split()[0]
    vol_filter = st.radio(
        "Volume filter",
        ["All", "On volume list", "Pre-signal only"],
        index=0,
        help=(
            "On volume list = confirmed by Finviz unusual volume. "
            "Pre-signal = signals fired but not yet on volume list."
        ),
    )
with c4:
    alert_threshold = st.slider(
        "Alert threshold", 0.0, 1.0, 0.6, 0.05,
        help="Rows with score >= this are highlighted green"
    )
    fast_mode = st.toggle("Fast mode", help="Locks to 30 min window")
    cap_filter = st.multiselect(
        "Market cap filter",
        ["Mega $200B+", "Large $10B-$200B", "Mid $2B-$10B",
         "Small $300M-$2B", "Micro $50M-$300M", "Nano <$50M"],
        default=[],
        help="Leave empty to show all. Select tiers to filter.",
        placeholder="All sizes",
    )
with c5:
    refresh_secs = st.select_slider(
        "Auto-refresh interval", options=[30, 60, 120, 300], value=60,
        format_func=lambda x: f"{x}s" if x < 60 else f"{x//60}m"
    )
    auto_pipeline = st.toggle(
        "Auto-run pipeline",
        help="Fetches and scores new articles on each refresh tick"
    )

if fast_mode:
    window_label = "30 min"

# ============================================================================
# AUTO-REFRESH
# ============================================================================

if _HAS_AUTOREFRESH:
    count    = _st_autorefresh(interval=refresh_secs * 1000, key="tz_refresh")
    prev_key = "tz_pipeline_count"
    # Only fire pipeline when the autorefresh counter actually increments
    if (auto_pipeline and count > 0
            and count != st.session_state.get(prev_key, -1)):
        st.session_state[prev_key] = count
        # Resolve project root so pipeline.py can be found regardless of cwd
        _proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        subprocess.Popen([sys.executable, "pipeline.py"], cwd=_proj)
        st.cache_data.clear()
else:
    st.caption("Install streamlit-autorefresh for auto-refresh support.")

# ============================================================================
# MANUAL BUTTONS
# ============================================================================

b1, b2, _ = st.columns([1, 1, 5])
with b1:
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
with b2:
    if st.button("Run pipeline now"):
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with st.spinner("Fetching new articles -- this can take 1-10 minutes depending on how much new content there is since the last run..."):
            result = subprocess.run(
                [sys.executable, "pipeline.py"],
                capture_output=True, text=True, timeout=900,
                cwd=project_dir,
            )
        st.cache_data.clear()
        st.success("Pipeline finished -- reloading page...")
        with st.expander("Pipeline output", expanded=result.returncode != 0):
            st.code(result.stdout + result.stderr)
        import streamlit.components.v1 as components
        components.html(
            "<script>setTimeout(function(){ window.parent.location.reload(); }, 1500);</script>",
            height=0
        )
st.divider()

# ============================================================================
# DATA LOADING
# ============================================================================

window_map = {"30 min": 30, "1 hour": 60, "4 hours": 240, "24 hours": 1440}
minutes    = window_map[window_label]
since_iso  = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()

tz_rows = load_trader_zone(conn, since_iso, use_weighted, sort_mode)

if not tz_rows:
    st.info(
        f"No scored articles in the last {window_label}. "
        "Run the pipeline to fetch fresh data."
    )
    st.stop()

# Batch price fetch -- capped at 100 tickers to avoid rate limiting
all_tks = tuple(r[0] for r in tz_rows if r[1] is not None)[:100]

# Unusual volume ticker set -- 24h lookback regardless of selected time window
uv_cutoff  = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
uv_tickers = {r[0] for r in conn.execute("""
    SELECT DISTINCT ticker FROM signals
    WHERE signal_type IN ('unusual_volume','unusual_volume_squeeze')
      AND detected_at >= ?
""", (uv_cutoff,)).fetchall()}

with st.spinner(f"Loading live prices for {len(all_tks)} tickers..."):
    price_snap = load_price_snapshot(all_tks)
social_badges = load_social_badges(all_tks)
signals_batch = load_all_signals_batch(all_tks, since_iso)
show_rs       = sort_mode == "composite"

# ============================================================================
# COLUMN HEADERS
# ============================================================================

rs_hdr = (
    '<span style="min-width:54px;text-align:right;color:#6e7681;font-size:12px;">'
    'Rank score</span>'
    if show_rs else ""
)
st.markdown(f"""
    <div style="padding:6px 14px;display:flex;gap:12px;
                border-bottom:1px solid #3d444d;color:#6e7681;font-size:12px;">
        <span style="min-width:24px;">#</span>
        <span style="min-width:58px;">Ticker</span>
        <span style="min-width:50px;">Score</span>
        <span style="min-width:18px;">vel</span>
        <span style="flex:1;min-width:90px;">Sentiment</span>
        <span style="min-width:72px;text-align:right;">Price</span>
        <span style="min-width:62px;text-align:right;">Day chg</span>
        <span style="min-width:18px;text-align:center;">Vol</span>
        <span style="min-width:54px;text-align:right;">Density</span>
        <span style="min-width:58px;text-align:right;">Social</span>
        <span style="min-width:60px;text-align:right;">Mkt Cap</span>
        <span style="min-width:42px;text-align:right;">Signal</span>
        {rs_hdr}
    </div>
""", unsafe_allow_html=True)

# ============================================================================
# TICKER ROWS
# ============================================================================

display_rank = 0
for tk, score, density, raw_count in tz_rows:
    if score is None:
        continue

    # Market cap and tier from yfinance snapshot
    pd_snap                      = price_snap.get(tk, {})
    mc                           = pd_snap.get("market_cap")
    tier_key, tier_label, tier_short = get_cap_tier(mc)

    # Apply market cap filter if selections were made
    if cap_filter and tier_short not in cap_filter:
        continue

    # Apply volume filter
    on_vol_list = tk in uv_tickers
    if vol_filter == "On volume list" and not on_vol_list:
        continue
    if vol_filter == "Pre-signal only" and on_vol_list:
        continue

    display_rank += 1
    rank = display_rank

    score_cls   = ("tz-score-pos" if score > 0.05 else
                   "tz-score-neg" if score < -0.05 else "tz-score-neu")
    density_val = f"{density:.1f}" if use_weighted else str(int(density))

    # Sentiment velocity arrow
    vel = load_velocity(conn, tk, since_iso, minutes)
    if vel is None:
        vel_html = '<span class="tz-vel-flat">--</span>'
    elif vel > 0.08:
        vel_html = '<span class="tz-vel-up">↑</span>'
    elif vel < -0.08:
        vel_html = '<span class="tz-vel-down">↓</span>'
    else:
        vel_html = '<span class="tz-vel-flat">→</span>'

    # Live price and intraday change
    if pd_snap:
        price_str = f"${pd_snap['price']:.2f}"
        chg       = pd_snap["chg_pct"]
        chg_cls   = ("tz-chg-pos" if chg > 0 else
                     "tz-chg-neg" if chg < 0 else "tz-chg-neu")
        chg_str   = f"{chg:+.2f}%"
    else:
        price_str = "--"
        chg_cls   = "tz-chg-neu"
        chg_str   = "--"

    # Unusual volume flag -- fires if current volume is on pace to exceed 3-month average
    vol_html = '<span style="min-width:18px;"></span>'
    if pd_snap.get("volume") and pd_snap.get("avg_vol") and pd_snap["avg_vol"] > 0:
        if pd_snap["volume"] > pd_snap["avg_vol"] * 0.5:
            vol_html = '<span class="tz-vol-flag" title="Unusual volume">🔥</span>'

    # Alert threshold highlighting
    row_cls = "tz-alert-row" if score >= alert_threshold else "tz-row"

    # Composite rank score
    rs_html = ""
    if show_rs:
        rs      = score * math.log(1 + (density or 0))
        rs_html = f'<span class="tz-rs">{rs:.3f}</span>'

    # Social bullish% badge
    sb = social_badges.get(tk, {})
    if sb.get("bullish_pct") is not None:
        bp          = sb["bullish_pct"]
        sp_flag     = " (spike)" if sb.get("signal_type") == "social_spike" else ""
        bp_color    = "#3fb950" if bp >= 0.6 else "#f85149" if bp <= 0.4 else "#8b949e"
        social_html = (
            f'<span style="min-width:58px;text-align:right;'
            f'color:{bp_color};font-size:12px;font-weight:600;">'
            f'{bp:.0%}{sp_flag}</span>'
        )
    else:
        social_html = '<span style="min-width:58px;"></span>'

    # Six-factor signal score
    sig_list       = signals_batch.get(tk, [])
    sig_pts, flags = compute_signal_score(tk, score, sig_list)
    sig_cls        = "sig-high" if sig_pts >= 4 else "sig-med" if sig_pts >= 2 else "sig-low"
    sig_badge      = f'<span class="{sig_cls}">{sig_pts}/6</span>'

    # Relvol: prefer Finviz stored value, fall back to live yfinance calculation
    live_rv     = None
    fv_sig_meta = next(
        (s["meta"] for s in sig_list
         if s["type"] in ("unusual_volume", "unusual_volume_squeeze")),
        {}
    )
    if pd_snap.get("volume") and pd_snap.get("avg_vol") and pd_snap["avg_vol"] > 0:
        live_rv = pd_snap["volume"] / pd_snap["avg_vol"]
    finviz_rv  = fv_sig_meta.get("relvol")
    relvol_val = finviz_rv or live_rv
    if relvol_val:
        rv_color = ("#3fb950" if relvol_val >= 3 else
                    "#e3b341" if relvol_val >= 1.5 else "#6e7681")
        rv_html  = (
            f'<span style="color:{rv_color};font-size:11px;'
            f'min-width:48px;text-align:right;">{relvol_val:.1f}x</span>'
        )
    else:
        rv_html = '<span style="min-width:48px;"></span>'

    # Market cap display string
    mc_html = (
        f'<span style="color:#6e7681;font-size:11px;'
        f'min-width:60px;text-align:right;">'
        f'{tier_short} {fmt_mc(mc)}</span>'
    )

    st.markdown(f"""
        <div class="{row_cls}">
            <span class="tz-rank">{rank}</span>
            <span class="tz-ticker">{tk}</span>
            <span class="{score_cls}">{score:+.2f}</span>
            {vel_html}
            {sentiment_bar_html(score)}
            <span class="tz-price">{price_str}</span>
            <span class="{chg_cls}">{chg_str}</span>
            {vol_html}
            {rv_html}
            <span class="tz-density">{density_val}</span>
            {social_html}
            {mc_html}
            {sig_badge}
            {rs_html}
        </div>
    """, unsafe_allow_html=True)

    # ── Drill-down expander ────────────────────────────────────────────────────
    articles = load_ticker_articles(conn, tk, since_iso)
    with st.expander(f"  {tk} -- fundamentals + articles"):

        # Signal stage, relvol trend, and plain-English reason
        reason          = infer_signal_reason(sig_list, 0, score)
        stage_lbl, stage_color = get_signal_stage(sig_list)
        rv_hist         = load_relvol_trend(conn, tk)
        if len(rv_hist) >= 2:
            direction = "up" if rv_hist[0] > rv_hist[1] else "down" if rv_hist[0] < rv_hist[1] else "flat"
            trend_str = f"&nbsp;·&nbsp; RelVol trend: {direction} ({rv_hist[1]:.1f}x to {rv_hist[0]:.1f}x)"
        else:
            trend_str = ""

        fund   = load_fundamentals(tk)
        mc_f   = fund.get("market_cap")
        if mc_f:
            _, _, mc_tier_short = get_cap_tier(mc_f)
            mc_context = f"&nbsp;·&nbsp; {mc_tier_short} ({fmt_market_cap(mc_f)})"
        else:
            mc_context = ""

        st.markdown(
            f'<span style="color:{stage_color};font-weight:600;">{stage_lbl}</span>'
            f'<span style="color:#6e7681;font-size:12px;">{trend_str}{mc_context}</span><br>'
            f'<span style="color:#8b949e;font-size:12px;">Why on radar: {reason}</span>',
            unsafe_allow_html=True
        )

        # Six-factor breakdown
        st.markdown(f"**Signal score: {sig_pts}/6**")
        for icon, desc in flags:
            st.markdown(f"{icon} {desc}")

        # Finviz news headline if available
        fv_sig = next(
            (s for s in sig_list
             if s["type"] in ("unusual_volume", "unusual_volume_squeeze")),
            None
        )
        if fv_sig and fv_sig["meta"].get("news_title"):
            news_url = fv_sig["meta"].get("news_url", "#")
            st.markdown(
                f'**Finviz news:** [{fv_sig["meta"]["news_title"]}]({news_url})'
            )

        st.divider()

        # Fundamentals from yfinance
        if any(fund.values()):
            f1, f2, f3, f4, f5 = st.columns(5)
            with f1:
                mc_disp = fmt_market_cap(fund.get("market_cap"))
                st.markdown(
                    f'<span class="fund-label">Market cap</span>'
                    f'<span class="fund-value">{mc_disp}</span>',
                    unsafe_allow_html=True
                )
            with f2:
                pe = fund.get("pe")
                st.markdown(
                    f'<span class="fund-label">P/E (TTM)</span>'
                    f'<span class="fund-value">{f"{pe:.1f}" if pe else "--"}</span>',
                    unsafe_allow_html=True
                )
            with f3:
                beta = fund.get("beta")
                st.markdown(
                    f'<span class="fund-label">Beta</span>'
                    f'<span class="fund-value">{f"{beta:.2f}" if beta else "--"}</span>',
                    unsafe_allow_html=True
                )
            with f4:
                sf = fund.get("short_float")
                st.markdown(
                    f'<span class="fund-label">Short float</span>'
                    f'<span class="fund-value">{f"{sf:.1%}" if sf else "--"}</span>',
                    unsafe_allow_html=True
                )
            with f5:
                st.markdown(
                    f'<span class="fund-label">Sector</span>'
                    f'<span class="fund-value">{fund.get("sector") or "--"}</span>',
                    unsafe_allow_html=True
                )
            st.divider()

        # Recent articles driving the signal
        if articles:
            for title, link, source, ingested_at, art_score, reasoning in articles:
                sc       = art_score or 0
                sc_color = ("#3fb950" if sc > 0.05 else
                            "#f85149" if sc < -0.05 else "#8b949e")
                sc_str   = f"{art_score:+.2f}" if art_score is not None else "--"
                st.markdown(f"""
                    <div style="padding:7px 0;border-bottom:1px solid #1c1f26;">
                        <span class="tz-article-meta">
                            {time_ago(ingested_at)} &nbsp;·&nbsp; {source}
                            &nbsp;·&nbsp;
                            <span style="color:{sc_color};font-weight:700;">{sc_str}</span>
                        </span><br>
                        <a class="tz-article-title" href="{link}"
                           target="_blank">{title}</a>
                        {"<br><span class='reasoning-text'>" + reasoning + "</span>"
                         if reasoning else ""}
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.caption("No articles in this window for this ticker.")


# ============================================================================
# FOOTER
# ============================================================================

density_lbl = "weighted" if use_weighted else "raw"
refresh_lbl = f" · auto-refresh {refresh_secs}s" if _HAS_AUTOREFRESH else ""
st.caption(
    f"{len(tz_rows)} tickers · last {window_label} · "
    f"{density_lbl} density · ranked by {sort_mode}{refresh_lbl}"
)


# ============================================================================
# CURRENTLY ON UNUSUAL VOLUME
# ============================================================================

st.divider()
st.markdown("#### Currently on Unusual Volume")
st.caption(
    "Tickers Finviz flagged for unusual relative volume in the last pipeline run. "
    "Thresholds vary by market cap tier -- large caps flagged at lower relvol. "
    "Squeeze = short float > 10% and days to cover > 5."
)

vol_tickers = load_unusual_volume_now(conn, lookback_hours=12)
if not vol_tickers:
    st.info("No unusual volume data yet -- run the pipeline to populate.")
else:
    if cap_filter:
        vol_tickers = [
            v for v in vol_tickers
            if any(v["cap_tier"].startswith(c.split()[0]) for c in cap_filter)
        ]

    st.markdown(f"""
        <div style="padding:6px 14px;display:flex;gap:12px;
                    border-bottom:1px solid #3d444d;color:#6e7681;font-size:12px;">
            <span style="min-width:60px;">Ticker</span>
            <span style="min-width:80px;">Company</span>
            <span style="min-width:54px;">RelVol</span>
            <span style="min-width:54px;">Price</span>
            <span style="min-width:54px;">Chg%</span>
            <span style="min-width:80px;">Cap Tier</span>
            <span style="min-width:60px;">Short Flt</span>
            <span style="min-width:54px;">Days Cov</span>
            <span style="flex:1;">News</span>
        </div>
    """, unsafe_allow_html=True)

    for v in vol_tickers:
        tk        = v["ticker"]
        rv        = v["relvol"]
        squeeze   = v["squeeze"]
        rv_color  = ("#3fb950" if rv and rv >= 3 else
                     "#e3b341" if rv and rv >= 1.5 else "#8b949e")
        chg       = v.get("change", "--") or "--"
        chg_color = ("#3fb950" if chg.startswith("+") else
                     "#f85149" if chg.startswith("-") else "#8b949e")
        sf        = f"{v['short_float']:.1f}%" if v.get("short_float") else "--"
        sr        = f"{v['short_ratio']:.1f}"  if v.get("short_ratio") else "--"
        squeeze_label = " (sq)" if squeeze else ""
        news_html = (
            f'<a href="{v["news_url"]}" target="_blank" '
            f'style="color:#58a6ff;font-size:11px;">'
            f'{v["news_title"][:45]}{"..." if len(v["news_title"]) > 45 else ""}</a>'
            if v.get("news_url") else
            f'<span style="color:#6e7681;font-size:11px;">{v["news_title"][:50]}</span>'
        )
        rv_str = f"{rv:.1f}x" if rv else "--"

        st.markdown(f"""
            <div style="padding:9px 14px;border-bottom:1px solid #262730;
                        display:flex;align-items:center;gap:12px;">
                <span style="color:#58a6ff;font-weight:700;min-width:60px;">
                    {tk}{squeeze_label}</span>
                <span style="color:#8b949e;font-size:12px;min-width:80px;">
                    {(v.get("company") or "")[:12]}</span>
                <span style="color:{rv_color};font-weight:600;min-width:54px;">
                    {rv_str}</span>
                <span style="color:#e6edf3;font-size:12px;min-width:54px;">
                    ${v.get("price","--")}</span>
                <span style="color:{chg_color};font-size:12px;min-width:54px;">
                    {chg}</span>
                <span style="color:#6e7681;font-size:11px;min-width:80px;">
                    {v.get("cap_tier","--")}</span>
                <span style="color:#8b949e;font-size:12px;min-width:60px;">
                    {sf}</span>
                <span style="color:#8b949e;font-size:12px;min-width:54px;">
                    {sr}</span>
                <span style="flex:1;">{news_html}</span>
            </div>
        """, unsafe_allow_html=True)

    st.caption(
        f"{len(vol_tickers)} tickers on unusual volume list · "
        "last pipeline run · sorted by relvol desc"
    )


# ============================================================================
# PRE-SIGNAL CANDIDATES
# ============================================================================

st.divider()
st.markdown("#### Pre-Signal Candidates")
st.caption(
    "Tickers with social, news, or insider signals but not yet confirmed by unusual volume. "
    "The herd has moved -- market volume has not followed yet. "
    "These are early-stage watchlist candidates."
)

pc1, pc2 = st.columns([1, 3])
with pc1:
    candidate_window = st.selectbox(
        "First seen within",
        ["1 hour", "4 hours", "12 hours", "24 hours", "7 days"],
        index=3,
        key="candidate_window",
    )
window_hours_map = {
    "1 hour": 1, "4 hours": 4, "12 hours": 12,
    "24 hours": 24, "7 days": 168,
}
candidate_hours = window_hours_map[candidate_window]

candidates = load_pre_signal_candidates(
    conn, lookback_days=candidate_hours / 24, vol_lookback_hours=48
)

# Filter by first-seen recency
cutoff_dt  = datetime.now(timezone.utc) - timedelta(hours=candidate_hours)
candidates = [
    c for c in candidates
    if datetime.fromisoformat(c["first_at"]).replace(tzinfo=timezone.utc) >= cutoff_dt
]

# Sort by most recent first
candidates = sorted(candidates, key=lambda x: x["first_at"], reverse=True)

# Apply market cap filter using live yfinance data for candidate tickers
if cap_filter and candidates:
    cand_tks   = tuple(c["ticker"] for c in candidates)
    cand_snaps = load_price_snapshot(cand_tks)
    candidates = [
        c for c in candidates
        if get_cap_tier(
            cand_snaps.get(c["ticker"], {}).get("market_cap")
        )[2] in cap_filter
    ]

if not candidates:
    st.info(
        f"No pre-signal candidates in the last {candidate_window}. "
        "Run the pipeline to update."
    )
else:
    st.markdown("""
        <div style="padding:6px 14px;display:flex;gap:12px;
                    border-bottom:1px solid #3d444d;color:#6e7681;font-size:12px;">
            <span style="min-width:58px;">Ticker</span>
            <span style="min-width:40px;">Score</span>
            <span style="min-width:72px;">Social</span>
            <span style="min-width:72px;">News</span>
            <span style="flex:1;">Why on radar</span>
            <span style="min-width:72px;text-align:right;">First seen</span>
        </div>
    """, unsafe_allow_html=True)

    for c in candidates[:20]:
        tk     = c["ticker"]
        sc     = c["score"]
        bp     = c["bull_pct"]
        ns     = c["avg_score"]
        reason = c["reason"]

        sc_color = "#3fb950" if sc >= 3 else "#e3b341" if sc >= 2 else "#6e7681"
        bp_str   = f"{bp:.0%}" if bp is not None else "--"
        bp_color = ("#3fb950" if bp and bp >= 0.6 else
                    "#f85149" if bp and bp <= 0.4 else "#8b949e")
        ns_str   = f"{ns:+.2f} ({c['mentions']})" if ns is not None else "--"
        ns_color = ("#3fb950" if ns and ns > 0.1 else
                    "#f85149" if ns and ns < -0.1 else "#8b949e")

        try:
            dt   = datetime.fromisoformat(c["first_at"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            mins = int((datetime.now(timezone.utc) - dt).total_seconds() // 60)
            age  = (f"{mins}m ago"       if mins < 60 else
                    f"{mins//60}h ago"   if mins < 1440 else
                    f"{mins//1440}d ago")
        except Exception:
            age = "--"

        st.markdown(f"""
            <div style="padding:9px 14px;border-bottom:1px solid #262730;
                        display:flex;align-items:center;gap:12px;">
                <span style="color:#58a6ff;font-weight:700;min-width:58px;">{tk}</span>
                <span style="color:{sc_color};font-weight:700;min-width:40px;">{sc}/4</span>
                <span style="color:{bp_color};font-size:12px;min-width:72px;">{bp_str} bull</span>
                <span style="color:{ns_color};font-size:12px;min-width:72px;">{ns_str}</span>
                <span style="color:#8b949e;font-size:12px;flex:1;">{reason}</span>
                <span style="color:#6e7681;font-size:11px;min-width:72px;text-align:right;">{age}</span>
            </div>
        """, unsafe_allow_html=True)

    st.caption(
        f"{len(candidates)} pre-signal candidates · "
        f"last {candidate_window} · sorted by signal strength"
    )