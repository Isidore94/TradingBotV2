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
    load_cache,
    load_tickers_from_file,
    save_cache,
)

# ── Configuration ────────────────────────────────────────────────
LONGS_FILE                = "longs.txt"
SHORTS_FILE               = "shorts.txt"
PREV_EARNINGS_CACHE_FILE  = "prev_earnings_cache.json"
LOG_FILE                  = "prev_avwap_bouncers.txt"

API_URL = "https://api.nasdaq.com/api/calendar/earnings?date={date}"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*"
}

MAX_LOOKBACK_DAYS = 250        # how far back to scan Nasdaq for earnings
FETCH_INTERVAL    = 45 * 60    # seconds between runs

# ATR-based bounce sensitivity
ATR_LENGTH        = 20
ATR_MULT          = 0.05       # eps/push = 0.05 * ATR(20)

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

# ── Previous anchor selection ───────────────────────────────────
def pick_previous_earnings_anchor(dates):
    """
    dates: ISO strings sorted desc (most recent first).
    Return SECOND most recent past earnings date as date, else None.
    """
    if not dates or len(dates) < 2:
        return None
    return datetime.fromisoformat(dates[1]).date()

def get_previous_anchor_date(symbol: str,
                             cache: dict,
                             all_dates: dict | None = None):
    """
    Order:
      1) cached
      2) all_dates (Nasdaq)
      3) yfinance 2nd most recent past earnings
    """
    today = datetime.now().date()

    if symbol in cache:
        try:
            d = datetime.fromisoformat(cache[symbol]).date()
            if d <= today:
                return d
        except Exception:
            pass

    if all_dates is not None and symbol in all_dates:
        prev_anchor = pick_previous_earnings_anchor(all_dates[symbol])
        if prev_anchor and prev_anchor <= today:
            cache[symbol] = prev_anchor.isoformat()
            return prev_anchor

    dates = fetch_past_earnings_from_yfinance(symbol)
    if len(dates) >= 2:
        prev_anchor = dates[1]
        cache[symbol] = prev_anchor.isoformat()
        logging.info(f"{symbol}: prev anchor via yfinance -> {prev_anchor}")
        return prev_anchor

    return None

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

# ── Single Run ─────────────────────────────────────────────────-
def run_once():
    longs  = load_tickers_from_file(LONGS_FILE)
    shorts = load_tickers_from_file(SHORTS_FILE)
    symbols = sorted(set(longs + shorts))

    if not symbols:
        logging.warning("No symbols found in longs/shorts lists.")
        return

    prev_cache = load_cache(PREV_EARNINGS_CACHE_FILE)

    # Pre-fetch earnings for symbols missing from cache
    need_dates = [s for s in symbols if s not in prev_cache]
    all_dates = {}
    if need_dates:
        logging.info(f"Fetching earnings history for {len(need_dates)} symbols (Nasdaq)…")
        all_dates = collect_earnings_dates(
            need_dates,
            max_lookback_days=MAX_LOOKBACK_DAYS,
            api_url=API_URL,
            headers=HEADERS,
            throttle_seconds=0.6,
            stop_when_all_found=False,
        )

    # IB connection
    ib = IBApi()
    ib.connect("127.0.0.1", 7496, clientId=1001)
    threading.Thread(target=ib.run, daemon=True).start()
    time.sleep(1.5)

    today = datetime.now().date()

    prev_bounce_longs = []      # (sym, MM/DD, PREV_BOUNCE_UPPER_1, LONG)
    prev_bounce_shorts = []     # (sym, MM/DD, PREV_BOUNCE_LOWER_1, SHORT)
    prev_cross_ups_long = []    # (sym, MM/DD, PREV_CROSS_UP_UPPER_X, LONG)
    prev_cross_downs_short = [] # (sym, MM/DD, PREV_CROSS_DOWN_LOWER_X, SHORT)

    for sym in symbols:
        is_long = sym in longs
        is_short = sym in shorts
        if not (is_long or is_short):
            continue

        logging.info(f"→ Processing {sym} for PREV-earnings AVWAP bounces")

        prev_anchor = get_previous_anchor_date(sym, prev_cache, all_dates)
        if not prev_anchor:
            logging.warning(f"{sym}: no previous earnings anchor found.")
            continue

        days = max(ATR_LENGTH + 5, (today - prev_anchor).days + 5)
        df = fetch_daily_bars(ib, sym, days)
        if df.empty:
            logging.warning(f"{sym}: no price data.")
            continue

        idxs = df.index[df["datetime"].dt.date == prev_anchor]
        if idxs.empty:
            logging.warning(f"{sym}: no candle on prev earnings date {prev_anchor}.")
            continue
        anchor_idx = int(idxs[0])

        if len(df) - anchor_idx < ATR_LENGTH + 3:
            logging.warning(f"{sym}: not enough bars after prev anchor {prev_anchor} for ATR/bounce.")
            continue

        vwap, sd, bands = calc_anchored_vwap_bands(df, anchor_idx)
        if pd.isna(vwap) or pd.isna(sd) or not bands:
            logging.warning(f"{sym}: invalid AVWAP/bands from prev anchor.")
            continue

        last_date = df.iloc[-1]["datetime"].date()
        dstr = last_date.strftime("%m/%d")

        upper_1 = bands.get("UPPER_1")
        lower_1 = bands.get("LOWER_1")

        # ── Directional crosses of stdev bands ─────────────────
        if len(df) >= 2:
            prev_close = df.iloc[-2]["close"]
            curr_close = df.iloc[-1]["close"]

            if is_long:
                for k in (1, 2, 3):
                    lvl = bands.get(f"UPPER_{k}")
                    if pd.notna(lvl) and prev_close <= lvl < curr_close:
                        prev_cross_ups_long.append((sym, dstr, f"PREV_CROSS_UP_UPPER_{k}", "LONG"))

            if is_short:
                for k in (1, 2, 3):
                    lvl = bands.get(f"LOWER_{k}")
                    if pd.notna(lvl) and prev_close >= lvl > curr_close:
                        prev_cross_downs_short.append((sym, dstr, f"PREV_CROSS_DOWN_LOWER_{k}", "SHORT"))

        # LONGS: bounce off previous UPPER_1 and move higher
        if is_long and upper_1 is not None:
            if bounce_up_at_level(df, upper_1, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
                prev_bounce_longs.append((sym, dstr, "PREV_BOUNCE_UPPER_1", "LONG"))

        # SHORTS: bounce (reject) off previous LOWER_1 and move lower
        if is_short and lower_1 is not None:
            if bounce_down_at_level(df, lower_1, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
                prev_bounce_shorts.append((sym, dstr, "PREV_BOUNCE_LOWER_1", "SHORT"))

    # ── Write output ────────────────────────────────────────────
    def write_items(f, items):
        for s, d, lbl, side in items:
            if side == "LONG":
                f.write(f"{s},{d},{lbl},{side}\n")
        for s, d, lbl, side in items:
            if side == "SHORT":
                f.write(f"{s},{d},{lbl},{side}\n")

    with open(LOG_FILE, "w", encoding="utf-8") as f:
        write_items(f, prev_bounce_longs)
        f.write("\n")
        write_items(f, prev_bounce_shorts)
        f.write("\n")
        write_items(f, prev_cross_ups_long)
        f.write("\n")
        write_items(f, prev_cross_downs_short)
        f.write("\n")
        f.write(f"Run completed at {datetime.now().strftime('%H:%M:%S')}\n")

    ib.disconnect()
    save_cache(prev_cache, PREV_EARNINGS_CACHE_FILE)
    logging.info(f"Run complete. Log: {LOG_FILE}, Cache: {PREV_EARNINGS_CACHE_FILE}")

# ── Main Loop ───────────────────────────────────────────────────
if __name__ == "__main__":
    while True:
        run_once()
        logging.info(f"Sleeping {FETCH_INTERVAL/60:.0f}m…")
        time.sleep(FETCH_INTERVAL)
