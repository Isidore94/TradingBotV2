#!/usr/bin/env python3
"""Single orchestration script for current and previous AVWAP analyses."""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

import earnings

# ── Configuration ────────────────────────────────────────────────
LONGS_FILE = "longs.txt"
SHORTS_FILE = "shorts.txt"
LOG_FILE = "combined_avwap.txt"

FETCH_INTERVAL = 45 * 60  # seconds between runs
RECENT_DAYS = 10

# ── Bounce Sensitivity (ATR-based) ──────────────────────────────
ATR_LENGTH = 20
ATR_MULT = 0.05  # eps/push = 0.05 * ATR(20)

# ── Logging Setup ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


# ── Utility: Load tickers ───────────────────────────────────────
def load_tickers_from_file(path: str) -> List[str]:
    if not os.path.exists(path):
        logging.warning(f"Ticker file not found: {path}")
        return []
    tickers: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            val = line.strip()
            if not val or val.upper().startswith("SYMBOLS FROM TC2000"):
                continue
            tickers.append(val.upper())
    return tickers


# ── IBKR API Wrapper ────────────────────────────────────────────
class IBApi(EWrapper, EClient):
    def __init__(self) -> None:
        EClient.__init__(self, self)
        self.data: Dict[int, List[dict]] = {}
        self.ready: Dict[int, bool] = {}

    def historicalData(self, reqId, bar):  # noqa: N802 (IBKR callback name)
        self.data.setdefault(reqId, []).append(
            {
                "time": bar.date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
        )

    def historicalDataEnd(self, reqId, start, end):  # noqa: N802
        self.ready[reqId] = True

    def error(self, reqId, code, msg):  # noqa: N802
        if code not in (2104, 2106, 2158, 2176):
            logging.error(f"IB Error {code}[{reqId}]: {msg}")


# ── Contract Helper ─────────────────────────────────────────────
def create_contract(symbol: str) -> Contract:
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


# ── Fetch Daily Bars ────────────────────────────────────────────
def fetch_daily_bars(ib: IBApi, symbol: str, days: int) -> pd.DataFrame:
    reqId = int(time.time() * 1000) % (2**31 - 1)
    ib.data[reqId] = []
    ib.ready[reqId] = False

    if days > 365:
        dur = f"{max(1, days // 365)} Y"
    else:
        dur = f"{max(2, days)} D"

    ib.reqHistoricalData(
        reqId=reqId,
        contract=create_contract(symbol),
        endDateTime="",
        durationStr=dur,
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=1,
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[],
    )

    for _ in range(60):
        if ib.ready.get(reqId):
            break
        time.sleep(0.5)

    bars = ib.data.pop(reqId, [])
    ib.ready.pop(reqId, None)

    df = pd.DataFrame(bars)
    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["time"], format="%Y%m%d", errors="coerce")
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


# ── AVWAP + Bands ───────────────────────────────────────────────
def calc_anchored_vwap_bands(df: pd.DataFrame, anchor_idx: int) -> Tuple[float, float, Dict[str, float]]:
    cumVol = 0.0
    cumVP = 0.0
    cumSD = 0.0

    for i in range(anchor_idx, len(df)):
        row = df.iloc[i]
        v = float(row["volume"])
        if v <= 0:
            continue
        tp = (row["open"] + row["high"] + row["low"] + row["close"]) / 4.0
        cumVol += v
        cumVP += tp * v
        vw = cumVP / cumVol
        dev = tp - vw
        cumSD += dev * dev * v

    if cumVol == 0:
        return float("nan"), float("nan"), {}

    final_vwap = cumVP / cumVol
    final_stdev = (cumSD / cumVol) ** 0.5

    bands = {
        "UPPER_1": final_vwap + final_stdev,
        "LOWER_1": final_vwap - final_stdev,
        "UPPER_2": final_vwap + 2 * final_stdev,
        "LOWER_2": final_vwap - 2 * final_stdev,
        "UPPER_3": final_vwap + 3 * final_stdev,
        "LOWER_3": final_vwap - 3 * final_stdev,
    }
    return final_vwap, final_stdev, bands


# ── ATR(20) Helper ─────────────────────────────────────────────
def get_atr20(df: pd.DataFrame, length: int = ATR_LENGTH) -> Optional[float]:
    if df is None or df.empty or len(df) < length + 1:
        return None

    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values

    trs: List[float] = []
    prev_close = closes[0]
    for i in range(1, len(df)):
        h = highs[i]
        l = lows[i]
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = closes[i]

    if len(trs) < length:
        return None

    atr_series = pd.Series(trs).rolling(length).mean()
    atr = atr_series.iloc[-1]
    if pd.isna(atr) or atr <= 0:
        return None
    return float(atr)


# ── Bounce Helpers (ATR-based) ──────────────────────────────────
def bounce_up_at_level(df: pd.DataFrame, level: float, atr: Optional[float] = None) -> bool:
    if atr is None:
        atr = get_atr20(df)
    if atr is None or level is None or pd.isna(level) or len(df) < ATR_LENGTH + 3:
        return False

    eps = ATR_MULT * atr
    push = ATR_MULT * atr

    _, B, C = df.iloc[-3], df.iloc[-2], df.iloc[-1]

    touched = B.low <= level + eps
    reclaimed = B.close >= level
    confirm = C.close > B.close and C.close >= level + push

    return bool(touched and reclaimed and confirm)


def bounce_down_at_level(df: pd.DataFrame, level: float, atr: Optional[float] = None) -> bool:
    if atr is None:
        atr = get_atr20(df)
    if atr is None or level is None or pd.isna(level) or len(df) < ATR_LENGTH + 3:
        return False

    eps = ATR_MULT * atr
    push = ATR_MULT * atr

    _, B, C = df.iloc[-3], df.iloc[-2], df.iloc[-1]

    touched = B.high >= level - eps
    rejected = B.close <= level
    confirm = C.close < B.close and C.close <= level - push

    return bool(touched and rejected and confirm)


def detect_bounces_for_symbol(
    sym: str,
    df: pd.DataFrame,
    vwap: float,
    bands: Dict[str, float],
    is_long: bool,
    is_short: bool,
) -> List[Tuple[str, str, str, str]]:
    results: List[Tuple[str, str, str, str]] = []
    if df is None or df.empty or len(df) < ATR_LENGTH + 3:
        return results

    atr = get_atr20(df)
    if atr is None:
        return results

    last_date = df.iloc[-1]["datetime"].date()
    dstr = last_date.strftime("%m/%d")

    u1 = bands.get("UPPER_1")
    u2 = bands.get("UPPER_2")
    l1 = bands.get("LOWER_1")
    l2 = bands.get("LOWER_2")

    if is_long:
        if l2 is not None and bounce_up_at_level(df, l2, atr):
            results.append((sym, dstr, "BOUNCE_LOWER_2", "LONG"))
        if l1 is not None and bounce_up_at_level(df, l1, atr):
            results.append((sym, dstr, "BOUNCE_LOWER_1", "LONG"))
        if vwap is not None and not pd.isna(vwap) and bounce_up_at_level(df, vwap, atr):
            results.append((sym, dstr, "BOUNCE_VWAP", "LONG"))
        if u1 is not None and bounce_up_at_level(df, u1, atr):
            results.append((sym, dstr, "BOUNCE_UPPER_1", "LONG"))

    if is_short:
        if u2 is not None and bounce_down_at_level(df, u2, atr):
            results.append((sym, dstr, "BOUNCE_UPPER_2", "SHORT"))
        if u1 is not None and bounce_down_at_level(df, u1, atr):
            results.append((sym, dstr, "BOUNCE_UPPER_1", "SHORT"))
        if vwap is not None and not pd.isna(vwap) and bounce_down_at_level(df, vwap, atr):
            results.append((sym, dstr, "BOUNCE_VWAP", "SHORT"))
        if l1 is not None and bounce_down_at_level(df, l1, atr):
            results.append((sym, dstr, "BOUNCE_LOWER_1", "SHORT"))

    return results


# ── Analysis Helpers ────────────────────────────────────────────
def _find_anchor_index(df: pd.DataFrame, anchor: date) -> Optional[int]:
    if anchor is None or df is None or df.empty:
        return None
    matches = df.index[df["datetime"].dt.date == anchor]
    if matches.empty:
        return None
    return int(matches[0])


def _analyze_current_anchor(
    sym: str,
    df: pd.DataFrame,
    anchor_idx: int,
    is_long: bool,
    is_short: bool,
) -> Tuple[
    Optional[float],
    Dict[str, float],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
]:
    vwap, sd, bands = calc_anchored_vwap_bands(df, anchor_idx)
    if pd.isna(vwap) or pd.isna(sd) or not bands:
        return None, {}, [], [], [], [], [], [], []

    last_row = df.iloc[-1]
    last_date = last_row["datetime"].date()
    close = last_row["close"]
    dstr = last_date.strftime("%m/%d")

    tier3: List[Tuple[str, str, str, str]] = []
    tier2: List[Tuple[str, str, str, str]] = []
    tier1: List[Tuple[str, str, str, str]] = []
    vwap_crosses: List[Tuple[str, str, str, str]] = []
    cross_ups_long: List[Tuple[str, str, str, str]] = []
    cross_downs_short: List[Tuple[str, str, str, str]] = []
    bounces: List[Tuple[str, str, str, str]] = []

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

    if is_long or is_short:
        bounces = detect_bounces_for_symbol(sym, df, vwap, bands, is_long, is_short)

    return (
        vwap,
        bands,
        tier3,
        tier2,
        tier1,
        vwap_crosses,
        cross_ups_long,
        cross_downs_short,
        bounces,
    )


def _analyze_previous_anchor(
    sym: str,
    df: pd.DataFrame,
    anchor_idx: int,
    is_long: bool,
    is_short: bool,
) -> Tuple[
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
    List[Tuple[str, str, str, str]],
]:
    vwap, sd, bands = calc_anchored_vwap_bands(df, anchor_idx)
    if pd.isna(vwap) or pd.isna(sd) or not bands:
        return [], [], [], []

    last_date = df.iloc[-1]["datetime"].date()
    dstr = last_date.strftime("%m/%d")

    prev_bounce_longs: List[Tuple[str, str, str, str]] = []
    prev_bounce_shorts: List[Tuple[str, str, str, str]] = []
    prev_cross_ups_long: List[Tuple[str, str, str, str]] = []
    prev_cross_downs_short: List[Tuple[str, str, str, str]] = []

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

    atr = get_atr20(df)
    if atr is not None:
        upper_1 = bands.get("UPPER_1")
        lower_1 = bands.get("LOWER_1")
        if is_long and upper_1 is not None and bounce_up_at_level(df, upper_1, atr):
            prev_bounce_longs.append((sym, dstr, "PREV_BOUNCE_UPPER_1", "LONG"))
        if is_short and lower_1 is not None and bounce_down_at_level(df, lower_1, atr):
            prev_bounce_shorts.append((sym, dstr, "PREV_BOUNCE_LOWER_1", "SHORT"))

    return (
        prev_bounce_longs,
        prev_bounce_shorts,
        prev_cross_ups_long,
        prev_cross_downs_short,
    )


def _write_section(f, lines: Iterable[Tuple[str, str, str, str]]) -> None:
    for sym, d, lvl, side in lines:
        f.write(f"{sym},{d},{lvl},{side}\n")


def run_once() -> None:
    longs = load_tickers_from_file(LONGS_FILE)
    shorts = load_tickers_from_file(SHORTS_FILE)
    symbols = sorted(set(longs + shorts))

    if not symbols:
        logging.warning("No symbols found.")
        return

    cache = earnings.load_cache()

    missing = [s for s in symbols if len(earnings.get_cached_dates(cache, s)) < 2]
    nasdaq_dates: Dict[str, List[date]] = {}
    if missing:
        logging.info(f"Fetching Nasdaq earnings for {len(missing)} symbols…")
        nasdaq_dates = earnings.collect_nasdaq_dates(missing, min_count=2)

    anchor_dates: Dict[str, List[date]] = {}
    for sym in symbols:
        anchor_dates[sym] = earnings.get_anchor_dates(
            sym,
            cache=cache,
            nasdaq_dates=nasdaq_dates.get(sym),
            min_count=2,
        )

    ib = IBApi()
    ib.connect("127.0.0.1", 7496, clientId=999)
    threading.Thread(target=ib.run, daemon=True).start()
    time.sleep(1.5)

    today = datetime.now().date()

    tier3: List[Tuple[str, str, str, str]] = []
    tier2: List[Tuple[str, str, str, str]] = []
    tier1: List[Tuple[str, str, str, str]] = []
    vwap_crosses: List[Tuple[str, str, str, str]] = []
    cross_ups_long: List[Tuple[str, str, str, str]] = []
    cross_downs_short: List[Tuple[str, str, str, str]] = []
    bounces: List[Tuple[str, str, str, str]] = []

    prev_bounce_longs: List[Tuple[str, str, str, str]] = []
    prev_bounce_shorts: List[Tuple[str, str, str, str]] = []
    prev_cross_ups_long: List[Tuple[str, str, str, str]] = []
    prev_cross_downs_short: List[Tuple[str, str, str, str]] = []

    for sym in symbols:
        is_long = sym in longs
        is_short = sym in shorts
        logging.info(f"→ Processing {sym} ({'LONG' if is_long else 'SHORT' if is_short else 'NA'})")

        anchors = anchor_dates.get(sym, [])
        anchors = [d for d in anchors if isinstance(d, date)]
        if not anchors:
            logging.warning(f"No earnings anchors for {sym}")
            continue

        latest = anchors[0] if anchors else None
        previous = anchors[1] if len(anchors) > 1 else None

        current_anchor = None
        previous_anchor = None

        if latest and (today - latest).days > RECENT_DAYS:
            current_anchor = latest
            previous_anchor = previous
        else:
            if latest:
                logging.info(
                    f"{sym}: skipping most recent earnings {latest} (<={RECENT_DAYS}d); using previous anchor."
                )
            current_anchor = previous
            previous_anchor = None

        relevant_anchors = [d for d in [current_anchor, previous_anchor] if d]
        if not relevant_anchors:
            logging.warning(f"{sym}: no eligible anchors after RECENT_DAYS filter")
            continue

        earliest = min(relevant_anchors)
        days = max(ATR_LENGTH + 3, (today - earliest).days + 3)
        df = fetch_daily_bars(ib, sym, days)
        if df.empty:
            logging.warning(f"No price data for {sym}")
            continue

        anchor_indices: Dict[date, int] = {}
        for d in relevant_anchors:
            idx = _find_anchor_index(df, d)
            if idx is None:
                logging.warning(f"{sym}: no candle on earnings date {d}")
                continue
            if len(df) - idx < 3:
                logging.warning(f"{sym}: not enough bars after anchor {d}")
                continue
            anchor_indices[d] = idx

        if current_anchor and current_anchor in anchor_indices:
            (
                _vwap,
                _bands,
                t3,
                t2,
                t1,
                vw_cross,
                cu_long,
                cd_short,
                bounce_signals,
            ) = _analyze_current_anchor(sym, df, anchor_indices[current_anchor], is_long, is_short)

            tier3.extend(t3)
            tier2.extend(t2)
            tier1.extend(t1)
            vwap_crosses.extend(vw_cross)
            cross_ups_long.extend(cu_long)
            cross_downs_short.extend(cd_short)
            bounces.extend(bounce_signals)
        elif current_anchor:
            logging.warning(f"{sym}: unable to analyse current anchor {current_anchor}")

        if previous_anchor and previous_anchor in anchor_indices:
            (
                prev_b_long,
                prev_b_short,
                prev_cu,
                prev_cd,
            ) = _analyze_previous_anchor(sym, df, anchor_indices[previous_anchor], is_long, is_short)

            prev_bounce_longs.extend(prev_b_long)
            prev_bounce_shorts.extend(prev_b_short)
            prev_cross_ups_long.extend(prev_cu)
            prev_cross_downs_short.extend(prev_cd)
        elif previous_anchor:
            logging.warning(f"{sym}: unable to analyse previous anchor {previous_anchor}")

    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("# CURRENT ANCHOR\n")
        _write_section(f, tier3)
        if tier3:
            f.write("\n")
        _write_section(f, tier2)
        if tier2:
            f.write("\n")
        _write_section(f, tier1)
        if tier1:
            f.write("\n")
        _write_section(f, vwap_crosses)
        if vwap_crosses:
            f.write("\n")
        _write_section(f, cross_ups_long)
        if cross_ups_long:
            f.write("\n")
        _write_section(f, cross_downs_short)
        if cross_downs_short:
            f.write("\n")
        _write_section(f, bounces)
        if bounces:
            f.write("\n")

        f.write("# PREVIOUS ANCHOR\n")
        _write_section(f, prev_bounce_longs)
        if prev_bounce_longs:
            f.write("\n")
        _write_section(f, prev_bounce_shorts)
        if prev_bounce_shorts:
            f.write("\n")
        _write_section(f, prev_cross_ups_long)
        if prev_cross_ups_long:
            f.write("\n")
        _write_section(f, prev_cross_downs_short)
        if prev_cross_downs_short:
            f.write("\n")

        f.write(f"Run completed at {datetime.now().strftime('%H:%M:%S')}\n")

    ib.disconnect()
    earnings.save_cache(cache)
    logging.info(f"Run complete. Log: {LOG_FILE}, Cache: {earnings.EARNINGS_CACHE_FILE}")


def main_loop() -> None:
    while True:
        run_once()
        logging.info(f"Sleeping {FETCH_INTERVAL/60:.0f}m…")
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main_loop()
