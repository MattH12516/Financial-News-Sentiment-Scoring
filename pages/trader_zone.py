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

Single ranked view. The unusual-volume and pre-signal tabs were removed --
both were filtered views of data already on the ranked list (RelVol column,
signal score, and the pre-volume / why-on-radar line in each drill-down).
The loaders load_unusual_volume_now() and load_pre_signal_candidates() are
retained but currently uncalled.
"""
import os
import sqlite3
import math
import sys
import time
import subprocess
from datetime import datetime, timezone, timedelta, time as dt_time
from zoneinfo import ZoneInfo

import json
import streamlit as st
import yfinance as yf

try:
    from streamlit_autorefresh import st_autorefresh as _st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

from app import EASTERN
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

    /* Navigation lives in the app's own tabs and links, not the sidebar */
    [data-testid="stSidebarNav"] { display: none; }

    /* Wider scrollbars -- easier to grab and drag without a scroll wheel */
    ::-webkit-scrollbar { width: 18px; height: 18px; }
    ::-webkit-scrollbar-track { background: #0e1117; }
    ::-webkit-scrollbar-thumb {
        background: #3d444d; border-radius: 9px; border: 3px solid #0e1117;
    }
    ::-webkit-scrollbar-thumb:hover { background: #58a6ff; }

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

def compute_composite(score, density, herd_hits=0, bullish_pct=None):
    """Composite rank = sentiment x log(1 + news density) x herd multiplier.

    The herd multiplier reflects that a loud, bullish retail crowd is itself a
    driving force -- it amplifies a ticker's rank when social chatter is heavy
    and skewed bullish, and dampens it when the crowd is heavily bearish.
    Neutral (1.0) when there is no herd data, so tickers are never penalised
    simply for lacking social coverage. Clamped to avoid runaway values."""
    base = (score or 0) * math.log(1 + (density or 0))
    if herd_hits and bullish_pct is not None:
        # bullish_tilt: 0.0 at 50% bullish, +1.0 at 100%, -1.0 at 0%
        bullish_tilt = (bullish_pct - 0.5) * 2
        herd_mult    = 1 + (math.log(1 + herd_hits) / 5) * bullish_tilt
        herd_mult    = max(0.5, min(2.0, herd_mult))
    else:
        herd_mult = 1.0
    return base * herd_mult


@st.cache_data(ttl=300, show_spinner=False)
def load_herd_data(ticker_tuple, lookback_hours=24):
    """Social herd metrics per ticker: message counts, bullish/bearish split,
    and herd keyword hits. Feeds both the ranking formula and the row display."""
    result = {}
    if not ticker_tuple:
        return result
    try:
        conn   = get_connection()
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(hours=lookback_hours)).isoformat()
        ph     = ",".join("?" for _ in ticker_tuple)
        rows   = conn.execute(f"""
            SELECT s.ticker,
                   s.bullish_pct                AS avg_bullish_pct,
                   COALESCE(s.bullish_count,0)  AS bullish_ct,
                   COALESCE(s.bearish_count,0)  AS bearish_ct,
                   COALESCE(s.post_count,0)     AS total_posts,
                   COALESCE(s.keyword_hits,0)   AS herd_hits
            FROM   signals s
            JOIN  (SELECT ticker, MAX(detected_at) AS latest
                   FROM   signals
                   WHERE  ticker IN ({ph})
                     AND  signal_type IN ('social_spike','social_read')
                     AND  detected_at >= ?
                   GROUP  BY ticker) m
              ON   m.ticker = s.ticker AND m.latest = s.detected_at
            WHERE  s.signal_type IN ('social_spike','social_read')
        """, (*ticker_tuple, cutoff)).fetchall()
        for tk, avg_bp, bull_ct, bear_ct, posts, herd in rows:
            result[tk] = {
                "bullish_pct": avg_bp,
                "bullish_ct":  int(bull_ct or 0),
                "bearish_ct":  int(bear_ct or 0),
                "total_posts": int(posts or 0),
                "herd_hits":   int(herd or 0),
            }
    except Exception:
        pass
    return result


@st.cache_data(ttl=300, show_spinner=False)
def load_latest_relvol(ticker_tuple):
    """Most recent relative volume reading per ticker, for ALL tickers Finviz
    returned -- not just those that cleared their cap tier threshold."""
    result = {}
    if not ticker_tuple:
        return result
    try:
        conn = get_connection()
        ph   = ",".join("?" for _ in ticker_tuple)
        rows = conn.execute(f"""
            SELECT ticker, relvol FROM relvol_history
            WHERE  ticker IN ({ph})
              AND  id IN (SELECT MAX(id) FROM relvol_history GROUP BY ticker)
        """, (*ticker_tuple,)).fetchall()
        for tk, rv in rows:
            if rv is not None:
                result[tk] = rv
    except Exception:
        pass
    return result

def load_trader_zone(conn, since_iso, use_weighted, sort_mode="composite",
                     until_iso=None):
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
    # until_iso is only set when a custom date range ends before today.
    upper  = " AND tm.mentioned_at <= ?" if until_iso else ""
    params = (since_iso, until_iso) if until_iso else (since_iso,)
    rows = conn.execute(f"""
        SELECT tm.ticker, AVG(tm.score) AS score,
               {density_expr} AS density, COUNT(*) AS raw_count
        FROM   ticker_mentions tm
        WHERE  tm.mentioned_at >= ?{upper} AND tm.score IS NOT NULL
        GROUP  BY tm.ticker
    """, params).fetchall()

    # Sorting happens in the caller for composite mode (needs herd data);
    # simple modes sort here.
    if sort_mode == "density":
        rows = sorted(rows, key=lambda r: r[2] or 0, reverse=True)
    elif sort_mode == "score":
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


def load_ticker_articles(conn, ticker, since_iso, until_iso=None):
    """Return the 8 most recent articles mentioning a ticker within the time window."""
    upper  = " AND tm.mentioned_at <= ?" if until_iso else ""
    params = ((ticker, since_iso, until_iso) if until_iso
              else (ticker, since_iso))
    return conn.execute(f"""
        SELECT a.title, a.link, a.source,
               COALESCE(NULLIF(a.published, ''), a.ingested_at), tm.score, tm.reasoning
        FROM   ticker_mentions tm
        JOIN   articles a ON a.id = tm.article_id
        WHERE  tm.ticker=? AND tm.mentioned_at>=?{upper}
        ORDER  BY tm.mentioned_at DESC LIMIT 8
    """, params).fetchall()


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

@st.cache_data(ttl=21600, show_spinner=False)
def load_market_caps(ticker_tuple):
    """Market cap only, for every ticker -- needed to filter by cap tier before
    paginating. Cached 6 hours because market cap barely moves intraday, so the
    cost is paid once rather than on every rerun. Only called when a cap filter
    is actually selected."""
    result = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_fetch_one_cap, tk): tk for tk in ticker_tuple}
        for future in as_completed(futures):
            tk, mc = future.result()
            if mc:
                result[tk] = mc
    return result


def _fetch_one_cap(tk):
    try:
        return tk, yf.Ticker(tk).fast_info.market_cap
    except Exception:
        return tk, None


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
    
def _fmt_num(val, fmt="{:.1f}"):
    """Format a possibly-string numeric value from yfinance safely.
    yfinance occasionally returns strings where floats are expected, which
    crashes f-string numeric formatting and kills the whole page render."""
    if val is None or val == "":
        return "--"
    try:
        return fmt.format(float(val))
    except (ValueError, TypeError):
        return str(val)

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
def load_all_signals_batch(ticker_tuple, since_iso, until_iso=None):
    """Fetch all signals for a batch of tickers in a single query."""
    result = {}
    try:
        conn = get_connection()
        ph   = ",".join("?" for _ in ticker_tuple)
        upper  = " AND detected_at <= ?" if until_iso else ""
        params = ((*ticker_tuple, since_iso, until_iso) if until_iso
                  else (*ticker_tuple, since_iso))
        rows = conn.execute(f"""
            SELECT ticker, signal_type, signal_value,
                   bullish_pct, keyword_hits, detected_at, metadata
            FROM   signals
            WHERE  ticker IN ({ph})
              AND  detected_at >= ?{upper}
            ORDER  BY detected_at DESC
        """, params).fetchall()
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


# Same thresholds the pipeline uses in _cap_tier, mirrored here so the UI can
# explain *why* a ticker missed rather than just saying it is not on the list.
CAP_RELVOL_MIN = {
    "mega": 1.3, "large": 1.5, "mid": 2.0,
    "small": 3.0, "micro": 5.0, "nano": 10.0, "unknown": 3.0,
}


def compute_signal_score(tk, news_score, signals_list, herd=None,
                         relvol=None, tier_key=None):
    """Evaluate six independent signal factors for a ticker.
    Returns (points, flags) where points is 0-6 and flags is a list of
    (icon, description) tuples for display in the drill-down.

    `herd` is the aggregated social data for the selected time window, passed
    in so the score and the Herd column always describe the same period.

    The six factors are:
    1. News sentiment > 0.3 (professional news coverage)
    2. Social bullish% >= 60% (retail crowd sentiment)
    3. Herd keyword activity (crowd behaviour language detected)
    4. On Finviz unusual volume list (market confirmation)
    5. Whale buying -- 3+ SEC Form 4 purchases in 24h (insider behaviour)
    6. Short squeeze setup (short float > 10% and days to cover > 5)
    """
    points = 0
    flags  = []
    herd   = herd or {}

    # Factor 1: News sentiment from LLM-scored articles
    if news_score is not None and news_score > 0.3:
        points += 1
        flags.append(("✅", f"News sentiment {news_score:+.2f}"))
    elif news_score is not None and news_score > 0.05:
        flags.append(("🔶", f"News sentiment {news_score:+.2f} -- weak positive"))
    else:
        ns = f"{news_score:+.2f}" if news_score is not None else "--"
        flags.append(("❌", f"News sentiment {ns}"))

    # Factor 2: Social bullish% -- prefer the windowed herd aggregate shown in
    # the Herd column so the score and the column never contradict each other.
    bull = herd.get("bullish_pct")
    if bull is None:
        bull = next((s["bullish_pct"] for s in signals_list
                     if s.get("bullish_pct") is not None), None)
    if bull is not None and bull >= 0.6:
        points += 1
        flags.append(("✅", f"Social {bull:.0%} bullish"))
    elif bull is not None:
        flags.append(("🔶", f"Social {bull:.0%} bullish -- below 60% threshold"))
    else:
        flags.append(("⬜", "Social -- no data in this window"))

    # Factor 3: Herd keyword activity. Counts cumulative hits across the window
    # rather than requiring 3+ within a single pipeline run -- sustained chatter
    # is as meaningful as one concentrated burst, and the old version could show
    # a high herd count in the column while reporting "no spike" here.
    herd_hits = herd.get("herd_hits", 0)
    if herd_hits >= 3:
        points += 1
        flags.append(("✅", f"Herd keyword activity -- {herd_hits} hits in window"))
    elif any(s["type"] == "social_spike" for s in signals_list):
        points += 1
        flags.append(("✅", "Herd keyword spike detected"))
    elif herd_hits > 0:
        flags.append(("🔶", f"Light herd activity -- {herd_hits} hits in window"))
    else:
        flags.append(("❌", "No herd activity in this window"))

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
        # The old label read "Not on Finviz unusual volume list", which was
        # wrong: the pipeline records relvol for every ticker Finviz returns and
        # only withholds the signal when relvol misses that ticker's market-cap
        # threshold. A nano cap at 6.0x is on the list and below the 10x bar --
        # saying so is more useful than a bare no.
        _thr = CAP_RELVOL_MIN.get(tier_key or "unknown")
        if relvol and _thr:
            flags.append(("❌", f"Volume {relvol:.1f}x -- below the {_thr:g}x "
                                f"threshold for this cap tier"))
        elif relvol:
            flags.append(("❌", f"Volume {relvol:.1f}x -- below threshold"))
        else:
            flags.append(("❌", "No relative volume reading for this ticker"))

    # Factor 5: Multiple large SEC Form 4 purchases filed within 24 hours
    if any(s["type"] == "form4_cluster" for s in signals_list):
        points += 1
        flags.append(("✅", "Whale buying -- 3+ large purchases filed (SEC Form 4)"))
    else:
        flags.append(("⬜", "No whale buying detected"))

    # Factor 6: Short squeeze potential -- trapped shorts amplify upward moves
    sf = fv_sig["meta"].get("short_float") if fv_sig else None
    sr = fv_sig["meta"].get("short_ratio") if fv_sig else None
    if sf is not None and sr is not None and sf > 10 and sr > 5:
        points += 1
        flags.append(("✅", f"Squeeze setup: {sf:.1f}% short float, {sr:.1f} days to cover"))
    elif sf is not None:
        flags.append(("⬜", f"Short float {sf:.1f}% -- no squeeze setup"))

    return points, flags


def infer_signal_reason(signals_list, mentions=0, avg_score=None):
    """Return a short plain-English description of why this ticker is on the radar."""
    parts = []
    if any(s["type"] == "form4_cluster" for s in signals_list):
        parts.append("whale buying")
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

nav1, nav2, _ = st.columns([1, 1, 4])
with nav1:
    st.page_link("app.py", label="Back to SentiFeed", icon=":material/arrow_back:")
with nav2:
    st.page_link("pages/company_deep_dive.py", label="Company Deep Dive",
                 icon=":material/search:")

st.markdown("## Trader Zone")

conn = get_connection()


# Small key/value store for state that must outlive a browser session. The
# ticker links in the table are plain anchors, so clicking one is a full browser
# navigation that destroys the Streamlit session and everything in
# session_state. Filters therefore live here, not only in session_state.
def _meta_get(conn, key, default=None):
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS meta "
                     "(key TEXT PRIMARY KEY, value TEXT)")
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default
    except Exception:
        return default


def _meta_set(conn, key, value):
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS meta "
                     "(key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO meta (key, value) VALUES (?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, str(value)))
        conn.commit()
    except Exception:
        pass

# ============================================================================
# CONTROLS
# ============================================================================
c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 2])

# Every control carries an explicit key. Keys alone are not enough, though:
# Streamlit garbage-collects session_state entries for widgets that are not
# currently rendered, so navigating to Company Deep Dive unmounts this page and
# drops all of them. Mirroring the values into shadow keys that no widget owns
# keeps them alive across the round trip -- restored below before the widgets
# are built, saved again once they exist.
PAGE_SIZE = 25

# Rows scoring at or above this are highlighted. Was a slider, but it only ever
# sat at 0.6 and the control implied a tunable that nothing else depends on.
ALERT_THRESHOLD = 0.6

_PERSIST_KEYS = ("tz_window", "tz_window_custom", "tz_density", "tz_ticker",
                 "tz_sort", "tz_vol", "tz_cap", "tz_refresh_secs",
                 "tz_auto_pipeline", "tz_pipeline_mins", "tz_page")

# Restore order: session_state shadows first (fast, same session), then the
# database (survives the full page load caused by clicking a ticker link).
for _k in _PERSIST_KEYS:
    _shadow = f"_keep_{_k}"
    if _shadow in st.session_state and _k not in st.session_state:
        st.session_state[_k] = st.session_state[_shadow]

if not any(_k in st.session_state for _k in _PERSIST_KEYS):
    try:
        _saved = json.loads(_meta_get(conn, "tz_filters", "{}") or "{}")
        for _k, _v in _saved.items():
            if _k in _PERSIST_KEYS and _v is not None:
                st.session_state[_k] = _v
    except Exception:
        pass


def _reset_page():
    """Any filter or sort change invalidates the current page number."""
    st.session_state["tz_page"] = 1


with c1:
    window_label = st.radio(
        "Time window",
        ["10 min", "30 min", "1 hour", "4 hours", "12 hours", "24 hours", "Custom"],
        index=2, key="tz_window", on_change=_reset_page,
    )
    if window_label == "Custom":
        _today = datetime.now(ZoneInfo("America/New_York")).date()
        custom_range = st.date_input(
            "Date range",
            value=(_today - timedelta(days=2), _today),
            max_value=_today,
            key="tz_window_custom", on_change=_reset_page,
            help="Articles are purged after 7 days, so anything older than that "
                 "returns nothing.",
        )
with c2:
    use_weighted = st.radio(
        "Density mode", ["Raw mentions", "Weighted"],
        key="tz_density", on_change=_reset_page,
    ) == "Weighted"
    ticker_filter = st.text_input(
        "Filter ticker", placeholder="e.g. AAPL",
        help="Show only this ticker",
        key="tz_ticker", on_change=_reset_page,
    ).strip().upper()
with c3:
    sort_label = st.radio(
        "Rank by", ["Composite", "Score", "Density", "Herd"],
        help="Composite = sentiment x log(1 + density) x herd multiplier",
        key="tz_sort", on_change=_reset_page,
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
        key="tz_vol", on_change=_reset_page,
    )
with c4:
    cap_filter = st.multiselect(
        "Market cap filter",
        ["Mega $200B+", "Large $10B-$200B", "Mid $2B-$10B",
         "Small $300M-$2B", "Micro $50M-$300M", "Nano <$50M"],
        default=[],
        help="Leave empty to show all. Selecting a size fetches market caps for "
             "every ticker, which makes the first load after a change slower.",
        placeholder="All sizes",
        key="tz_cap", on_change=_reset_page,
    )
with c5:
    refresh_secs = st.select_slider(
        "Auto-refresh interval", options=[30, 60, 120, 300], value=60,
        format_func=lambda x: f"{x}s" if x < 60 else f"{x//60}m",
        key="tz_refresh_secs",
    )
    auto_pipeline = st.toggle(
        "Auto-run pipeline",
        key="tz_auto_pipeline",
        help="Runs the pipeline on the interval below while this page is open. "
             "Never starts a run while another is still going."
    )
    pipeline_mins = st.select_slider(
        "Pipeline interval", options=[15, 20, 30, 45, 60], value=20,
        format_func=lambda x: f"{x} min",
        key="tz_pipeline_mins",
        help="Stocktwits allows 200 requests/hour and the pipeline uses roughly "
             "one per ticker, so intervals under 15 minutes risk rate limiting.",
    )


# ============================================================================
# AUTO-REFRESH
# ============================================================================

def _pipeline_is_running(project_dir):
    """True if a pipeline run is in progress, based on its lock file.
    Locks older than 20 minutes are treated as stale and cleared."""
    lock_path = os.path.join(project_dir, "pipeline.lock")
    if not os.path.exists(lock_path):
        return False
    try:
        if (time.time() - os.path.getmtime(lock_path)) > 1200:
            os.remove(lock_path)
            return False
    except Exception:
        return False
    return True


if _HAS_AUTOREFRESH:
    count = _st_autorefresh(interval=refresh_secs * 1000, key="tz_refresh")
    if auto_pipeline:
        _proj     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        _interval = pipeline_mins * 60
        try:
            last_run = float(_meta_get(conn, "last_pipeline_ts", 0) or 0)
        except (TypeError, ValueError):
            last_run = 0.0
        elapsed = time.time() - last_run

        if _pipeline_is_running(_proj):
            st.caption("Auto-run: a run is in progress.")
        elif elapsed < _interval:
            wait = max(1, int((_interval - elapsed) // 60) + 1)
            st.caption(f"Auto-run on -- next run in ~{wait} min "
                       f"(every {pipeline_mins} min).")
        else:
            # Claim the slot before launching, so a second rerun landing in the
            # same instant cannot start a duplicate run.
            _meta_set(conn, "last_pipeline_ts", time.time())
            subprocess.Popen([sys.executable, "pipeline.py"], cwd=_proj)
            st.cache_data.clear()
            st.caption("Auto-run: pipeline started.")
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

# Widgets exist now, so copy their values into the shadow keys (survives a page
# unmount within the same session) and into the database (survives the session
# being destroyed entirely by a ticker link's full page navigation).
for _k in _PERSIST_KEYS:
    if _k in st.session_state:
        st.session_state[f"_keep_{_k}"] = st.session_state[_k]

try:
    _meta_set(conn, "tz_filters", json.dumps(
        {_k: st.session_state[_k] for _k in _PERSIST_KEYS
         if _k in st.session_state}
    ))
except Exception:
    pass

# ============================================================================
# DATA LOADING
# ============================================================================

window_map = {"10 min": 10, "30 min": 30, "1 hour": 60,
              "4 hours": 240, "12 hours": 720, "24 hours": 1440}

# until_iso stays None for the preset windows and for any custom range ending
# today -- those are "since X, up to now", which is what every other part of the
# page assumes.
until_iso   = None
custom_days = False

if window_label == "Custom":
    _tz  = ZoneInfo("America/New_York")
    _now = datetime.now(_tz)
    if isinstance(custom_range, (list, tuple)) and len(custom_range) == 2:
        _start, _end = custom_range
    else:
        _start = _end = (custom_range if not isinstance(custom_range, (list, tuple))
                         else custom_range[0])

    _start_dt = datetime.combine(_start, dt_time.min, tzinfo=_tz)
    _end_dt   = datetime.combine(_end,   dt_time.max, tzinfo=_tz)
    since_dt  = _start_dt.astimezone(timezone.utc)
    if _end < _now.date():
        until_iso = _end_dt.astimezone(timezone.utc).isoformat()
    minutes      = max(1, int((_now - _start_dt).total_seconds() // 60))
    window_label = (f"{_start:%b %d}" if _start == _end
                    else f"{_start:%b %d} to {_end:%b %d}")
    custom_days  = True
else:
    minutes = window_map[window_label]
since_iso  = (since_dt.isoformat() if custom_days else
              (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat())

tz_rows = load_trader_zone(conn, since_iso, use_weighted, sort_mode, until_iso)

if not tz_rows:
    st.info(
        f"No scored articles in the last {window_label}. "
        "Run the pipeline to fetch fresh data."
    )
    st.stop()

# Every ticker in the window. The old code sliced this to 100 *before* sorting,
# so which tickers had data was decided by the pre-sort order while the display
# order was something else -- that is why rows showed "--" in scattered places
# rather than in a contiguous block at the bottom.
all_tks = tuple(r[0] for r in tz_rows if r[1] is not None)

# Unusual volume ticker set -- 24h lookback regardless of selected time window
uv_cutoff  = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
uv_tickers = {r[0] for r in conn.execute("""
    SELECT DISTINCT ticker FROM signals
    WHERE signal_type IN ('unusual_volume','unusual_volume_squeeze')
      AND detected_at >= ?
""", (uv_cutoff,)).fetchall()}

# These loaders all read SQLite, so they are cheap enough to run across every
# ticker regardless of how many there are. Only the yfinance price fetch is
# expensive, and that now happens per page further down.
social_badges = load_social_badges(all_tks)
signals_batch = load_all_signals_batch(all_tks, since_iso, until_iso)
# Herd data follows the selected time window so every number on a row describes
# the same period. Floored at 1h because pipeline runs are the real resolution
# limit -- a 10-minute herd window would usually be empty.
herd_hours       = max(1, minutes / 60)
herd_window_lbl  = f"{int(herd_hours)}h"
herd_data        = load_herd_data(all_tks, lookback_hours=herd_hours)
relvol_map    = load_latest_relvol(all_tks)
show_rs       = sort_mode == "composite"

# Composite and herd sorting need the herd data, so they sort here
if sort_mode == "composite":
    tz_rows = sorted(
        tz_rows,
        key=lambda r: compute_composite(
            r[1], r[2],
            herd_data.get(r[0], {}).get("herd_hits", 0),
            herd_data.get(r[0], {}).get("bullish_pct"),
        ),
        reverse=True,
    )
elif sort_mode == "herd":
    tz_rows = sorted(
        tz_rows,
        key=lambda r: herd_data.get(r[0], {}).get("herd_hits", 0),
        reverse=True,
    )

# ============================================================================
# FILTER, THEN PAGINATE, THEN FETCH PRICES FOR THE PAGE ONLY
# ============================================================================

# Market cap lives in the yfinance snapshot, so filtering by it needs caps for
# every ticker -- not just the current page. That fetch only runs when a cap
# filter is actually selected, so the default path stays fast.
cap_map = {}
if cap_filter:
    with st.spinner(f"Fetching market caps for {len(all_tks)} tickers..."):
        cap_map = load_market_caps(all_tks)

filtered_rows = []
for _r in tz_rows:
    _tk, _score = _r[0], _r[1]
    if _score is None:
        continue
    if ticker_filter and ticker_filter not in _tk:
        continue
    if cap_filter:
        _, _, _tier_short = get_cap_tier(cap_map.get(_tk))
        if _tier_short not in cap_filter:
            continue
    _on_vol = _tk in uv_tickers
    if vol_filter == "On volume list" and not _on_vol:
        continue
    if vol_filter == "Pre-signal only" and _on_vol:
        continue
    filtered_rows.append(_r)

total_rows  = len(filtered_rows)
total_pages = max(1, math.ceil(total_rows / PAGE_SIZE))

# Clamp -- a filter change can leave the stored page number out of range.
page_num = min(max(1, st.session_state.get("tz_page", 1)), total_pages)
st.session_state["tz_page"] = page_num

start_idx = (page_num - 1) * PAGE_SIZE
page_rows = filtered_rows[start_idx:start_idx + PAGE_SIZE]
page_tks  = tuple(r[0] for r in page_rows)

if not filtered_rows:
    st.info("No tickers match the current filters. Widen the time window or "
            "clear the ticker / market cap / volume filters.")
    st.stop()

with st.spinner(f"Loading live prices for {len(page_tks)} tickers..."):
    price_snap = load_price_snapshot(page_tks)


def _render_pager(location):
    """Page controls. Rendered above and below the table; the location string
    keeps the button keys unique between the two copies."""
    if total_pages <= 1:
        return
    p1, p2, p3 = st.columns([1, 2, 1])
    with p1:
        if st.button("< Prev", key=f"tz_prev_{location}",
                     disabled=page_num <= 1, use_container_width=True):
            st.session_state["tz_page"] = page_num - 1
            st.rerun()
    with p2:
        first = start_idx + 1
        last  = min(start_idx + PAGE_SIZE, total_rows)
        st.markdown(
            f"<div style='text-align:center;color:#8b949e;font-size:13px;"
            f"padding-top:6px;'>{first}-{last} of {total_rows} &nbsp;·&nbsp; "
            f"page {page_num} of {total_pages}</div>",
            unsafe_allow_html=True,
        )
    with p3:
        if st.button("Next >", key=f"tz_next_{location}",
                     disabled=page_num >= total_pages, use_container_width=True):
            st.session_state["tz_page"] = page_num + 1
            st.rerun()

# ============================================================================
# COLUMN HEADERS
# ============================================================================

if until_iso:
    st.caption(
        "Historical range: sentiment, density and signals are bounded by the "
        "dates above. Price, day change, RelVol and herd columns are fetched "
        "live and always show current values."
    )

_render_pager("top")

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
        <span style="flex:1;min-width:90px;">Sentiment</span>
        <span style="min-width:72px;text-align:right;">Price</span>
        <span style="min-width:62px;text-align:right;">Day chg</span>
        <span style="min-width:48px;text-align:right;">RelVol</span>
        <span style="min-width:54px;text-align:right;">News</span>
        <span style="min-width:88px;text-align:right;" title="Bullish/bearish message counts and herd keyword hits in the selected window">Herd {herd_window_lbl} (B/S)</span>
        <span style="min-width:60px;text-align:right;">Mkt Cap</span>
        <span style="min-width:42px;text-align:right;">Signal</span>
        {rs_hdr}
    </div>
""", unsafe_allow_html=True)

# ============================================================================
# TICKER ROWS
# ============================================================================

# Filters were applied before pagination, so this loop just renders. Rank is
# offset by the page so numbering stays continuous across pages.
display_rank = start_idx
for tk, score, density, raw_count in page_rows:
    # Market cap and tier from yfinance snapshot
    pd_snap                      = price_snap.get(tk, {})
    mc                           = pd_snap.get("market_cap") or cap_map.get(tk)
    tier_key, tier_label, tier_short = get_cap_tier(mc)

    on_vol_list = tk in uv_tickers

    display_rank += 1
    rank = display_rank

    score_cls   = ("tz-score-pos" if score > 0.05 else
                   "tz-score-neg" if score < -0.05 else "tz-score-neu")
    density_val = f"{density:.1f}" if use_weighted else str(int(density))


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
    row_cls = "tz-alert-row" if score >= ALERT_THRESHOLD else "tz-row"

    # Six-factor signal score
    sig_list       = signals_batch.get(tk, [])
    # relvol is resolved here rather than further down because the signal
    # breakdown below needs it to explain a missed volume threshold.
    # Prefer the stored Finviz value (which now covers every ticker Finviz
    # returns, not just threshold-clearing ones), falling back to a live
    # calculation from the yfinance snapshot.
    finviz_rv = relvol_map.get(tk)
    live_rv   = None
    if pd_snap.get("volume") and pd_snap.get("avg_vol") and pd_snap["avg_vol"] > 0:
        live_rv = pd_snap["volume"] / pd_snap["avg_vol"]
    # Finviz and yfinance compute relvol differently, so mark which source this
    # came from -- a yfinance-derived figure will not correspond to the Finviz
    # unusual volume list, which is why a high "~" value may not appear there.
    relvol_val = finviz_rv if finviz_rv else live_rv

    sig_pts, flags = compute_signal_score(
        tk, score, sig_list, herd_data.get(tk, {}),
        relvol=relvol_val, tier_key=tier_key)
    sig_cls        = "sig-high" if sig_pts >= 4 else "sig-med" if sig_pts >= 2 else "sig-low"
    sig_badge      = f'<span class="{sig_cls}">{sig_pts}/6</span>'


    if relvol_val:
        is_finviz = finviz_rv is not None
        rv_color  = ("#3fb950" if relvol_val >= 3 else
                     "#e3b341" if relvol_val >= 1.5 else "#6e7681")
        prefix    = "" if is_finviz else "~"
        title     = ("Finviz relative volume" if is_finviz
                     else "Estimated from yfinance -- not a Finviz figure, "
                          "so it will not match the unusual volume list")
        rv_html   = (f'<span title="{title}" style="color:{rv_color};font-size:11px;'
                     f'min-width:48px;text-align:right;">{prefix}{relvol_val:.1f}x</span>')
    else:
        rv_html = '<span style="min-width:48px;"></span>'

    # Herd: bullish/bearish message counts plus herd keyword hits
    hd = herd_data.get(tk, {})
    if hd.get("total_posts"):
        bull_ct  = hd["bullish_ct"]
        bear_ct  = hd["bearish_ct"]
        herd_ct  = hd["herd_hits"]
        bp       = hd.get("bullish_pct")
        hd_color = ("#3fb950" if bp and bp >= 0.6 else
                    "#f85149" if bp and bp <= 0.4 else "#8b949e")
        herd_flag = (f' <span style="color:#e3b341;">+{herd_ct}</span>'
                     if herd_ct else "")
        herd_html = (f'<span title="Last {herd_window_lbl}: {bull_ct} bullish / '
                     f'{bear_ct} bearish messages, {herd_ct} herd keyword hits" '
                     f'style="min-width:88px;text-align:right;font-size:11px;'
                     f'color:{hd_color};">{bull_ct}/{bear_ct}{herd_flag}</span>')
    else:
        herd_html = '<span style="min-width:88px;"></span>'

    # Composite rank score, now herd-aware
    rs_html = ""
    if show_rs:
        rs = compute_composite(score, density, hd.get("herd_hits", 0),
                               hd.get("bullish_pct"))
        rs_html = f'<span class="tz-rs">{rs:.3f}</span>'

    # Market cap display string
    mc_html = (
        f'<span style="color:#6e7681;font-size:11px;'
        f'min-width:60px;text-align:right;">'
        f'{tier_short} {fmt_mc(mc)}</span>'
    )

    st.markdown(f"""
        <div class="{row_cls}">
            <span class="tz-rank">{rank}</span>
            <span class="tz-ticker"><a href="/company_deep_dive?ticker={tk}" target="_self"
                style="color:#58a6ff;text-decoration:none;">{tk}</a></span>
            <span class="{score_cls}">{score:+.2f}</span>
            {sentiment_bar_html(score)}
            <span class="tz-price">{price_str}</span>
            <span class="{chg_cls}">{chg_str}</span>
            {rv_html}
            <span class="tz-density">{density_val}</span>
            {herd_html}
            {mc_html}
            {sig_badge}
            {rs_html}
        </div>
    """, unsafe_allow_html=True)

    # ── Drill-down expander ────────────────────────────────────────────────────
    articles = load_ticker_articles(conn, tk, since_iso, until_iso)
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
    

        st.divider()

        # Fundamentals from yfinance
        if any(fund.values()):
            f1, f2, f3, f4, f5 = st.columns(5)
            with f1:
                st.markdown(
                    f'<span class="fund-label">Forward P/E</span>'
                    f'<span class="fund-value">{_fmt_num(fund.get("fwd_pe"))}</span>',
                    unsafe_allow_html=True
                )
            with f2:
                st.markdown(
                    f'<span class="fund-label">P/E (TTM)</span>'
                    f'<span class="fund-value">{_fmt_num(fund.get("pe"))}</span>',
                    unsafe_allow_html=True
                )
            with f3:
                st.markdown(
                    f'<span class="fund-label">Beta</span>'
                    f'<span class="fund-value">{_fmt_num(fund.get("beta"), "{:.2f}")}</span>',
                    unsafe_allow_html=True
                )
            with f4:
                st.markdown(
                    f'<span class="fund-label">Short float</span>'
                    f'<span class="fund-value">{_fmt_num(fund.get("short_float"), "{:.1%}")}</span>',
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
                            {fmt_time(ingested_at)} &nbsp;·&nbsp; {source}
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

_render_pager("bottom")

density_lbl = "weighted" if use_weighted else "raw"
refresh_lbl = f" · auto-refresh {refresh_secs}s" if _HAS_AUTOREFRESH else ""
filt_lbl    = (f"{total_rows} of {len(tz_rows)} tickers"
               if total_rows != len(tz_rows) else f"{total_rows} tickers")
st.caption(
    f"{filt_lbl} · last {window_label} · "
    f"{density_lbl} density · ranked by {sort_mode}{refresh_lbl}"
)