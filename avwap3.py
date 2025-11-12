#!/usr/bin/env python3
import time
import logging
import threading
from datetime import datetime, timedelta

import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from shared.avwap_utils import (
    bounce_down_at_level,
    bounce_up_at_level,
    calc_anchored_vwap_bands,
    collect_earnings_dates,
    fetch_daily_bars,
    fetch_past_earnings_from_yfinance,
    get_atr20,
    load_cache,
    load_tickers_from_file,
    save_cache,
)

# ── Configuration ────────────────────────────────────────────────
LONGS_FILE          = "longs.txt"
SHORTS_FILE         = "shorts.txt"
EARNINGS_CACHE_FILE = "earnings_cache.json"
LOG_FILE            = "combined_avwap.txt"

API_URL = "https://api.nasdaq.com/api/calendar/earnings?date={date}"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*"
}

MAX_LOOKBACK_DAYS   = 150       # Nasdaq earnings scan window
RECENT_DAYS         = 10        # If most recent earnings < RECENT_DAYS, use previous
FETCH_INTERVAL      = 45 * 60   # seconds between runs

# ── Output Filters ──────────────────────────────────────────────
OUTPUT_TIER1                = True
OUTPUT_TIER2                = True
OUTPUT_TIER3                = False
OUTPUT_VWAP                 = True
OUTPUT_CROSS_UPS_LONG       = True
OUTPUT_CROSS_DOWNS_SHORT    = True
OUTPUT_BOUNCES              = True

# ── Bounce Sensitivity (ATR-based) ──────────────────────────────
ATR_LENGTH        = 20
ATR_MULT          = 0.05    # eps/push = 0.05 * ATR(20)

# ── Logging Setup ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

# ── Choose best earnings date ───────────────────────────────────
def select_best_date(dates):
    if not dates:
        return None
    today = datetime.now().date()
    most = datetime.fromisoformat(dates[0]).date()
    if (today - most).days <= RECENT_DAYS and len(dates) > 1:
        return datetime.fromisoformat(dates[1]).date()
    return most

# ── yfinance fallback ───────────────────────────────────────────
def get_anchor_date(symbol: str, cache: dict):
    dates = fetch_past_earnings_from_yfinance(symbol)
    if not dates:
        return None

    chosen = dates[0]
    cache[symbol] = chosen.isoformat()
    logging.info(f"Cached via yfinance {symbol} -> {chosen}")
    return chosen

# ── IBKR API Wrapper ────────────────────────────────────────────
class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}
        self.ready = {}

    def historicalData(self, reqId, bar):
        self.data.setdefault(reqId, []).append({
            "time":   bar.date,
            "open":   bar.open,
            "high":   bar.high,
            "low":    bar.low,
            "close":  bar.close,
            "volume": bar.volume
        })

    def historicalDataEnd(self, reqId, start, end):
        self.ready[reqId] = True

    def error(self, reqId, code, msg):
        if code not in (2104, 2106, 2158, 2176):
            logging.error(f"IB Error {code}[{reqId}]: {msg}")

# ── Bounce Detection Wrapper ────────────────────────────────────
def detect_bounces_for_symbol(sym: str,
                              df: pd.DataFrame,
                              vwap: float,
                              bands: dict,
                              is_long: bool,
                              is_short: bool):
    """
    Longs:
      - BOUNCE_LOWER_2, BOUNCE_LOWER_1, BOUNCE_VWAP, BOUNCE_UPPER_1
    Shorts:
      - BOUNCE_UPPER_2, BOUNCE_UPPER_1, BOUNCE_VWAP, BOUNCE_LOWER_1
    All using ATR(20)-based touch + push.
    """
    results = []
    if df is None or df.empty or len(df) < ATR_LENGTH + 3:
        return results

    atr = get_atr20(df, length=ATR_LENGTH)
    if atr is None:
        return results

    last_date = df.iloc[-1]["datetime"].date()
    dstr = last_date.strftime("%m/%d")

    u1 = bands.get("UPPER_1")
    u2 = bands.get("UPPER_2")
    l1 = bands.get("LOWER_1")
    l2 = bands.get("LOWER_2")

    # Longs
    if is_long:
        if l2 is not None and bounce_up_at_level(df, l2, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_LOWER_2", "LONG"))
        if l1 is not None and bounce_up_at_level(df, l1, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_LOWER_1", "LONG"))
        if vwap is not None and not pd.isna(vwap) and bounce_up_at_level(df, vwap, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_VWAP", "LONG"))
        if u1 is not None and bounce_up_at_level(df, u1, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_UPPER_1", "LONG"))

    # Shorts
    if is_short:
        if u2 is not None and bounce_down_at_level(df, u2, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_UPPER_2", "SHORT"))
        if u1 is not None and bounce_down_at_level(df, u1, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_UPPER_1", "SHORT"))
        if vwap is not None and not pd.isna(vwap) and bounce_down_at_level(df, vwap, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_VWAP", "SHORT"))
        # LOWER_1 as resistance after breakdown
        if l1 is not None and bounce_down_at_level(df, l1, atr=atr, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
            results.append((sym, dstr, "BOUNCE_LOWER_1", "SHORT"))

    return results

# ── Single Run ──────────────────────────────────────────────────
def run_once():
    longs  = load_tickers_from_file(LONGS_FILE)
    shorts = load_tickers_from_file(SHORTS_FILE)
    symbols = sorted(set(longs + shorts))

    if not symbols:
        logging.warning("No symbols found.")
        return

    cache = load_cache(EARNINGS_CACHE_FILE)

    # Fill cache via Nasdaq for uncached
    uncached = [s for s in symbols if s not in cache]
    if uncached:
        logging.info(f"Fetching earnings for {len(uncached)} uncached symbols…")
        all_dates = collect_earnings_dates(
            uncached,
            max_lookback_days=MAX_LOOKBACK_DAYS,
            api_url=API_URL,
            headers=HEADERS,
            throttle_seconds=1.0,
            stop_when_all_found=True,
        )
        for s, dates in all_dates.items():
            bd = select_best_date(dates)
            if bd:
                cache[s] = bd.isoformat()

    # IB connection
    ib = IBApi()
    ib.connect("127.0.0.1", 7496, clientId=999)
    threading.Thread(target=ib.run, daemon=True).start()
    time.sleep(1.5)

    today = datetime.now().date()

    # Buckets
    tier3 = []
    tier2 = []
    tier1 = []
    vwap_crosses = []
    cross_ups_long = []
    cross_downs_short = []
    bounces = []

    for sym in symbols:
        is_long = sym in longs
        is_short = sym in shorts
        logging.info(f"→ Processing {sym} ({'LONG' if is_long else 'SHORT' if is_short else 'NA'})")

        # Anchor date from cache or yfinance
        ed_str = cache.get(sym)
        ed = datetime.fromisoformat(ed_str).date() if ed_str else get_anchor_date(sym, cache)
        if not ed:
            logging.warning(f"No earnings date for {sym}")
            continue

        days = max(ATR_LENGTH + 3, (today - ed).days + 3)
        df = fetch_daily_bars(ib, sym, days)
        if df.empty:
            logging.warning(f"No price data for {sym}")
            continue

        idxs = df.index[df["datetime"].dt.date == ed]
        if idxs.empty:
            logging.warning(f"No candle on earnings date {ed} for {sym}")
            continue
        anchor_idx = int(idxs[0])

        if len(df) - anchor_idx < 3:
            logging.warning(f"Not enough bars after anchor for {sym}")
            continue

        vwap, sd, bands = calc_anchored_vwap_bands(df, anchor_idx)
        if pd.isna(vwap) or pd.isna(sd) or not bands:
            logging.warning(f"NaN bands for {sym}, skipping.")
            continue

        last_row  = df.iloc[-1]
        last_date = last_row["datetime"].date()
        close     = last_row["close"]
        dstr      = last_date.strftime("%m/%d")

        # ── Tier classification ────────────────────────────────
        if is_long:
            if close > bands["UPPER_3"]:
                tier3.append((sym, dstr, "UPPER_3", "LONG"))
            elif close > bands["UPPER_2"]:
                tier2.append((sym, dstr, "UPPER_2", "LONG"))
            elif close > bands["UPPER_1"]:
                tier1.append((sym, dstr, "UPPER_1", "LONG"))

        if is_short:
            if close < bands["LOWER_3"]:
                tier3.append((sym, dstr, "LOWER_3", "SHORT"))
            elif close < bands["LOWER_2"]:
                tier2.append((sym, dstr, "LOWER_2", "SHORT"))
            elif close < bands["LOWER_1"]:
                tier1.append((sym, dstr, "LOWER_1", "SHORT"))

        # ── VWAP crosses (last 2 days) ─────────────────────────
        recent_dates = sorted(df["datetime"].dt.date.unique())[-2:]
        hits = {d: set() for d in recent_dates}
        levels = {"VWAP": vwap, **bands}

        recent_df = df[df["datetime"].dt.date.isin(recent_dates)]
        for _, row in recent_df.iterrows():
            d = row["datetime"].date()
            for lvl_name, lvl_val in levels.items():
                if pd.notna(lvl_val) and row["low"] <= lvl_val <= row["high"]:
                    hits[d].add(lvl_name)

        for d, touched in hits.items():
            if {"VWAP", "UPPER_1", "LOWER_1"}.issubset(touched):
                continue
            if "VWAP" in touched:
                side = "LONG" if is_long else "SHORT" if is_short else None
                if side:
                    vwap_crosses.append((sym, d.strftime("%m/%d"), "VWAP", side))

        # ── Directional crosses ────────────────────────────────
        if len(df) >= 2:
            prev_close = df.iloc[-2]["close"]
            curr_close = df.iloc[-1]["close"]

            if is_long:
                for k in (1, 2, 3):
                    lvl = bands.get(f"UPPER_{k}")
                    if pd.notna(lvl) and prev_close <= lvl < curr_close:
                        cross_ups_long.append((sym, dstr, f"CROSS_UP_UPPER_{k}", "LONG"))

            if is_short:
                for k in (1, 2, 3):
                    lvl = bands.get(f"LOWER_{k}")
                    if pd.notna(lvl) and prev_close >= lvl > curr_close:
                        cross_downs_short.append((sym, dstr, f"CROSS_DOWN_LOWER_{k}", "SHORT"))

        # ── ATR-based Bounce Detection ─────────────────────────
        if OUTPUT_BOUNCES and (is_long or is_short):
            sym_bounces = detect_bounces_for_symbol(sym, df, vwap, bands, is_long, is_short)
            bounces.extend(sym_bounces)

    # ── Writer: LONGS first then SHORTS ─────────────────────────
    def write_category(f, items):
        if not items:
            return
        for s, d, lvl, side in items:
            if side == "LONG":
                f.write(f"{s},{d},{lvl},{side}\n")
        for s, d, lvl, side in items:
            if side == "SHORT":
                f.write(f"{s},{d},{lvl},{side}\n")

    # ── Write LOG_FILE ─────────────────────────────────────────
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        if OUTPUT_TIER3:
            write_category(f, tier3)
            f.write("\n")
        if OUTPUT_TIER2:
            write_category(f, tier2)
            f.write("\n")
        if OUTPUT_TIER1:
            write_category(f, tier1)
            f.write("\n")
        if OUTPUT_VWAP:
            write_category(f, vwap_crosses)
            f.write("\n")
        if OUTPUT_CROSS_UPS_LONG:
            write_category(f, cross_ups_long)
            f.write("\n")
        if OUTPUT_CROSS_DOWNS_SHORT:
            write_category(f, cross_downs_short)
            f.write("\n")
        if OUTPUT_BOUNCES:
            write_category(f, bounces)
            f.write("\n")
        f.write(f"Run completed at {datetime.now().strftime('%H:%M:%S')}\n")

    ib.disconnect()
    save_cache(cache, EARNINGS_CACHE_FILE)
    logging.info(f"Run complete. Log: {LOG_FILE}, Cache: {EARNINGS_CACHE_FILE}")

# ── Main Loop ───────────────────────────────────────────────────
if __name__ == "__main__":
    while True:
        run_once()
        logging.info(f"Sleeping {FETCH_INTERVAL/60:.0f}m…")
        time.sleep(FETCH_INTERVAL)
