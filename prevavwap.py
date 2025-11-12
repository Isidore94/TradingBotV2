diff --git a/prevavwap.py b/prevavwap.py
index e14223f0ef3b9f4063969d28bbf325cbbaa23152..22af08e3b7770510c1ccb041bbac1774ad7dbfa3 100644
--- a/prevavwap.py
+++ b/prevavwap.py
@@ -1,400 +1,166 @@
 #!/usr/bin/env python3
-import os
 import time
-import json
 import logging
 import threading
 from datetime import datetime, timedelta
 
-import requests
 import pandas as pd
-import yfinance as yf
 from ibapi.client import EClient
 from ibapi.wrapper import EWrapper
-from ibapi.contract import Contract
+
+from shared.avwap_utils import (
+    bounce_down_at_level,
+    bounce_up_at_level,
+    calc_anchored_vwap_bands,
+    collect_earnings_dates,
+    fetch_daily_bars,
+    fetch_past_earnings_from_yfinance,
+    load_cache,
+    load_tickers_from_file,
+    save_cache,
+)
 
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
 
-# ── Utils: Load tickers ─────────────────────────────────────────
-def load_tickers_from_file(path: str):
-    if not os.path.exists(path):
-        logging.warning(f"Ticker file not found: {path}")
-        return []
-    out = []
-    with open(path, "r", encoding="utf-8") as f:
-        for line in f:
-            v = line.strip()
-            if not v or v.upper().startswith("SYMBOLS FROM TC2000"):
-                continue
-            out.append(v.upper())
-    return out
-
-# ── Cache prev-earnings anchors ─────────────────────────────────
-def load_prev_cache():
-    if os.path.exists(PREV_EARNINGS_CACHE_FILE):
-        try:
-            with open(PREV_EARNINGS_CACHE_FILE, "r", encoding="utf-8") as f:
-                return json.load(f)
-        except json.JSONDecodeError:
-            logging.warning("Prev earnings cache corrupt; starting fresh.")
-    return {}
-
-def save_prev_cache(cache: dict):
-    with open(PREV_EARNINGS_CACHE_FILE, "w", encoding="utf-8") as f:
-        json.dump(cache, f, indent=2)
-
-# ── Nasdaq earnings fetch ───────────────────────────────────────
-def fetch_earnings_for_date(date_str: str):
-    try:
-        resp = requests.get(API_URL.format(date=date_str),
-                            headers=HEADERS, timeout=10)
-        resp.raise_for_status()
-        return resp.json().get("data", {}).get("rows", []) or []
-    except Exception as e:
-        logging.warning(f"Failed fetch earnings for {date_str}: {e}")
-        time.sleep(0.5)
-        return []
-
-def collect_earnings_dates(symbols):
-    """
-    Return dict: sym -> sorted list of past earnings dates (YYYY-MM-DD), most recent first.
-    """
-    symbol_dates = {sym: [] for sym in symbols}
-    today = datetime.now().date()
-
-    for delta in range(MAX_LOOKBACK_DAYS):
-        date = today - timedelta(days=delta)
-        rows = fetch_earnings_for_date(date.isoformat())
-        time.sleep(0.6)  # throttle
-        for row in rows:
-            sym = row.get("symbol", "").upper()
-            if sym in symbol_dates:
-                ds = date.isoformat()
-                if ds not in symbol_dates[sym]:
-                    symbol_dates[sym].append(ds)
-
-    # keep only <= today, sort desc
-    for sym, dates in symbol_dates.items():
-        past = [d for d in dates if datetime.fromisoformat(d).date() <= today]
-        past.sort(reverse=True)
-        symbol_dates[sym] = past
-
-    return symbol_dates
-
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
 
-    # yfinance fallback
-    try:
-        t = yf.Ticker(symbol)
-        ed = t.get_earnings_dates(limit=8)
-        ed.index = ed.index.tz_localize(None)
-        past = ed[ed.index < pd.Timestamp.today().tz_localize(None)]
-        if len(past.index) >= 2:
-            sorted_past = sorted(past.index, reverse=True)
-            prev_anchor = sorted_past[1].date()
-            cache[symbol] = prev_anchor.isoformat()
-            logging.info(f"{symbol}: prev anchor via yfinance -> {prev_anchor}")
-            return prev_anchor
-    except Exception as e:
-        logging.warning(f"{symbol}: yfinance prev-earnings lookup failed: {e}")
+    dates = fetch_past_earnings_from_yfinance(symbol)
+    if len(dates) >= 2:
+        prev_anchor = dates[1]
+        cache[symbol] = prev_anchor.isoformat()
+        logging.info(f"{symbol}: prev anchor via yfinance -> {prev_anchor}")
+        return prev_anchor
 
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
 
-# ── Contract Helper ─────────────────────────────────────────────
-def create_contract(symbol: str) -> Contract:
-    c = Contract()
-    c.symbol   = symbol
-    c.secType  = "STK"
-    c.exchange = "SMART"
-    c.currency = "USD"
-    return c
-
-# ── Fetch Daily Bars ────────────────────────────────────────────
-def fetch_daily_bars(ib: IBApi, symbol: str, days: int) -> pd.DataFrame:
-    reqId = int(time.time() * 1000) % (2**31 - 1)
-    ib.data[reqId] = []
-    ib.ready[reqId] = False
-
-    if days > 365:
-        dur = f"{max(1, days // 365)} Y"
-    else:
-        dur = f"{max(2, days)} D"
-
-    ib.reqHistoricalData(
-        reqId=reqId,
-        contract=create_contract(symbol),
-        endDateTime="",
-        durationStr=dur,
-        barSizeSetting="1 day",
-        whatToShow="TRADES",
-        useRTH=1,
-        formatDate=1,
-        keepUpToDate=False,
-        chartOptions=[]
-    )
-
-    for _ in range(60):
-        if ib.ready.get(reqId):
-            break
-        time.sleep(0.5)
-
-    bars = ib.data.pop(reqId, [])
-    ib.ready.pop(reqId, None)
-
-    df = pd.DataFrame(bars)
-    if df.empty:
-        return df
-
-    df["datetime"] = pd.to_datetime(df["time"], format="%Y%m%d", errors="coerce")
-    df = df.sort_values("datetime").reset_index(drop=True)
-    return df
-
-# ── AVWAP + Bands ───────────────────────────────────────────────
-def calc_anchored_vwap_bands(df: pd.DataFrame, anchor_idx: int):
-    """
-    Anchored VWAP + 1/2/3σ bands from anchor_idx → end.
-    """
-    cumVol = 0.0
-    cumVP = 0.0
-    cumSD = 0.0
-
-    for i in range(anchor_idx, len(df)):
-        row = df.iloc[i]
-        v = float(row["volume"])
-        if v <= 0:
-            continue
-        tp = (row["open"] + row["high"] + row["low"] + row["close"]) / 4.0
-        cumVol += v
-        cumVP += tp * v
-        vw = cumVP / cumVol
-        dev = tp - vw
-        cumSD += dev * dev * v
-
-    if cumVol == 0:
-        return float("nan"), float("nan"), {}
-
-    final_vwap = cumVP / cumVol
-    final_stdev = (cumSD / cumVol) ** 0.5
-
-    bands = {
-        "UPPER_1": final_vwap + final_stdev,
-        "LOWER_1": final_vwap - final_stdev,
-        "UPPER_2": final_vwap + 2 * final_stdev,
-        "LOWER_2": final_vwap - 2 * final_stdev,
-        "UPPER_3": final_vwap + 3 * final_stdev,
-        "LOWER_3": final_vwap - 3 * final_stdev,
-    }
-    return final_vwap, final_stdev, bands
-
-# ── ATR(20) Helper ─────────────────────────────────────────────
-def get_atr20(df: pd.DataFrame, length: int = ATR_LENGTH):
-    """
-    Compute 20-day ATR using standard True Range.
-    Returns latest ATR value, or None if insufficient data.
-    """
-    if df is None or df.empty or len(df) < length + 1:
-        return None
-
-    highs = df["high"].values
-    lows = df["low"].values
-    closes = df["close"].values
-
-    trs = []
-    prev_close = closes[0]
-    for i in range(1, len(df)):
-        h = highs[i]
-        l = lows[i]
-        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
-        trs.append(tr)
-        prev_close = closes[i]
-
-    if len(trs) < length:
-        return None
-
-    atr_series = pd.Series(trs).rolling(length).mean()
-    atr = atr_series.iloc[-1]
-    if pd.isna(atr) or atr <= 0:
-        return None
-    return float(atr)
-
-# ── Bounce Logic Using ATR ─────────────────────────────────────
-def bounce_up_at_level(df: pd.DataFrame, level: float) -> bool:
-    """
-    Long bounce off level using ATR-based thresholds:
-      eps  = 0.05 * ATR20
-      push = 0.05 * ATR20
-    Pattern:
-      - B.low <= level + eps
-      - B.close >= level
-      - C.close > B.close and C.close >= level + push
-    """
-    if level is None or pd.isna(level) or len(df) < ATR_LENGTH + 3:
-        return False
-
-    atr = get_atr20(df)
-    if atr is None:
-        return False
-
-    eps = ATR_MULT * atr
-    push = ATR_MULT * atr
-
-    A, B, C = df.iloc[-3], df.iloc[-2], df.iloc[-1]
-
-    touched = B.low <= level + eps
-    reclaimed = B.close >= level
-    confirm = C.close > B.close and C.close >= level + push
-
-    return bool(touched and reclaimed and confirm)
-
-def bounce_down_at_level(df: pd.DataFrame, level: float) -> bool:
-    """
-    Short bounce (rejection) off level using ATR-based thresholds:
-      eps  = 0.05 * ATR20
-      push = 0.05 * ATR20
-    Pattern:
-      - B.high >= level - eps
-      - B.close <= level
-      - C.close < B.close and C.close <= level - push
-    """
-    if level is None or pd.isna(level) or len(df) < ATR_LENGTH + 3:
-        return False
-
-    atr = get_atr20(df)
-    if atr is None:
-        return False
-
-    eps = ATR_MULT * atr
-    push = ATR_MULT * atr
-
-    A, B, C = df.iloc[-3], df.iloc[-2], df.iloc[-1]
-
-    touched = B.high >= level - eps
-    rejected = B.close <= level
-    confirm = C.close < B.close and C.close <= level - push
-
-    return bool(touched and rejected and confirm)
-
 # ── Single Run ─────────────────────────────────────────────────-
 def run_once():
     longs  = load_tickers_from_file(LONGS_FILE)
     shorts = load_tickers_from_file(SHORTS_FILE)
     symbols = sorted(set(longs + shorts))
 
     if not symbols:
         logging.warning("No symbols found in longs/shorts lists.")
         return
 
-    prev_cache = load_prev_cache()
+    prev_cache = load_cache(PREV_EARNINGS_CACHE_FILE)
 
     # Pre-fetch earnings for symbols missing from cache
     need_dates = [s for s in symbols if s not in prev_cache]
     all_dates = {}
     if need_dates:
         logging.info(f"Fetching earnings history for {len(need_dates)} symbols (Nasdaq)…")
-        all_dates = collect_earnings_dates(need_dates)
+        all_dates = collect_earnings_dates(
+            need_dates,
+            max_lookback_days=MAX_LOOKBACK_DAYS,
+            api_url=API_URL,
+            headers=HEADERS,
+            throttle_seconds=0.6,
+            stop_when_all_found=False,
+        )
 
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
@@ -424,63 +190,63 @@ def run_once():
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
-            if bounce_up_at_level(df, upper_1):
+            if bounce_up_at_level(df, upper_1, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
                 prev_bounce_longs.append((sym, dstr, "PREV_BOUNCE_UPPER_1", "LONG"))
 
         # SHORTS: bounce (reject) off previous LOWER_1 and move lower
         if is_short and lower_1 is not None:
-            if bounce_down_at_level(df, lower_1):
+            if bounce_down_at_level(df, lower_1, atr_length=ATR_LENGTH, atr_mult=ATR_MULT):
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
-    save_prev_cache(prev_cache)
+    save_cache(prev_cache, PREV_EARNINGS_CACHE_FILE)
     logging.info(f"Run complete. Log: {LOG_FILE}, Cache: {PREV_EARNINGS_CACHE_FILE}")
 
 # ── Main Loop ───────────────────────────────────────────────────
 if __name__ == "__main__":
     while True:
         run_once()
         logging.info(f"Sleeping {FETCH_INTERVAL/60:.0f}m…")
         time.sleep(FETCH_INTERVAL)
