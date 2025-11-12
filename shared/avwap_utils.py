"""Shared helper functions for AVWAP workflows."""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Mapping, Sequence

import pandas as pd
import requests
import yfinance as yf
from ibapi.contract import Contract


def load_tickers_from_file(path: str) -> list[str]:
    """Return uppercase tickers from ``path`` while skipping comment headers."""
    if not os.path.exists(path):
        logging.warning("Ticker file not found: %s", path)
        return []

    tickers: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            val = line.strip()
            if not val or val.upper().startswith("SYMBOLS FROM TC2000"):
                continue
            tickers.append(val.upper())
    return tickers


def load_cache(path: str) -> dict:
    """Load a JSON cache from ``path`` or return an empty dict on failure."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.warning("Cache file %s is corrupt; starting fresh.", path)
    return {}


def save_cache(cache: Mapping, path: str) -> None:
    """Persist ``cache`` as JSON to ``path``."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def fetch_earnings_for_date(
    date_str: str,
    *,
    api_url: str,
    headers: Mapping[str, str] | None = None,
    timeout: float = 10,
    sleep_on_error: float = 0.5,
) -> list[dict]:
    """Fetch Nasdaq earnings rows for ``date_str`` and handle errors gently."""
    try:
        resp = requests.get(api_url.format(date=date_str), headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json().get("data", {}).get("rows", []) or []
    except Exception as exc:  # pragma: no cover - defensive logging
        logging.warning("Failed fetch earnings for %s: %s", date_str, exc)
        if sleep_on_error:
            time.sleep(sleep_on_error)
        return []


def collect_earnings_dates(
    symbols: Sequence[str],
    *,
    max_lookback_days: int,
    api_url: str,
    headers: Mapping[str, str] | None = None,
    throttle_seconds: float = 1.0,
    stop_when_all_found: bool = False,
    include_future_dates: bool = False,
) -> dict[str, list[str]]:
    """Collect earnings dates via Nasdaq API for ``symbols``.

    Returns a dict mapping symbols to ISO formatted date strings sorted most
    recent first.  Future dates are filtered unless ``include_future_dates`` is
    True.
    """
    today = datetime.now().date()
    symbol_dates: dict[str, list[str]] = {sym: [] for sym in symbols}

    for delta in range(max_lookback_days):
        date = today - timedelta(days=delta)
        rows = fetch_earnings_for_date(
            date.isoformat(), api_url=api_url, headers=headers
        )
        if throttle_seconds:
            time.sleep(throttle_seconds)
        for row in rows:
            sym = row.get("symbol", "").upper()
            if sym not in symbol_dates:
                continue
            ds = date.isoformat()
            if ds not in symbol_dates[sym]:
                symbol_dates[sym].append(ds)
        if stop_when_all_found and all(symbol_dates[s] for s in symbols):
            break

    for sym, dates in symbol_dates.items():
        if include_future_dates:
            filtered = dates
        else:
            filtered = [
                d for d in dates if datetime.fromisoformat(d).date() <= today
            ]
        filtered.sort(reverse=True)
        symbol_dates[sym] = filtered

    return symbol_dates


def fetch_past_earnings_from_yfinance(symbol: str, limit: int = 8) -> list[datetime.date]:
    """Return past earnings dates (most recent first) using yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        earnings = ticker.get_earnings_dates(limit=limit)
        earnings.index = earnings.index.tz_localize(None)
        now = pd.Timestamp.today().tz_localize(None)
        past = earnings[earnings.index < now]
        return sorted((idx.date() for idx in past.index), reverse=True)
    except Exception as exc:  # pragma: no cover - defensive logging
        logging.warning("yfinance lookup failed for %s: %s", symbol, exc)
        return []


def create_contract(symbol: str) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


def fetch_daily_bars(ib, symbol: str, days: int):
    """Fetch historical daily bars via IBKR API using ``ib`` connection."""
    req_id = int(time.time() * 1000) % (2**31 - 1)
    ib.data[req_id] = []
    ib.ready[req_id] = False

    if days > 365:
        duration = f"{max(1, days // 365)} Y"
    else:
        duration = f"{max(2, days)} D"

    ib.reqHistoricalData(
        reqId=req_id,
        contract=create_contract(symbol),
        endDateTime="",
        durationStr=duration,
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=1,
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[],
    )

    for _ in range(60):
        if ib.ready.get(req_id):
            break
        time.sleep(0.5)

    bars = ib.data.pop(req_id, [])
    ib.ready.pop(req_id, None)

    df = pd.DataFrame(bars)
    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["time"], format="%Y%m%d", errors="coerce")
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


def calc_anchored_vwap_bands(df: pd.DataFrame, anchor_idx: int):
    """Compute anchored VWAP and standard deviation bands from ``anchor_idx``."""
    cum_vol = 0.0
    cum_vp = 0.0
    cum_sd = 0.0

    for i in range(anchor_idx, len(df)):
        row = df.iloc[i]
        volume = float(row["volume"])
        if volume <= 0:
            continue
        typical_price = (
            row["open"] + row["high"] + row["low"] + row["close"]
        ) / 4.0
        cum_vol += volume
        cum_vp += typical_price * volume
        vwap = cum_vp / cum_vol
        deviation = typical_price - vwap
        cum_sd += deviation * deviation * volume

    if cum_vol == 0:
        return float("nan"), float("nan"), {}

    final_vwap = cum_vp / cum_vol
    final_stdev = (cum_sd / cum_vol) ** 0.5
    bands = {
        "UPPER_1": final_vwap + final_stdev,
        "LOWER_1": final_vwap - final_stdev,
        "UPPER_2": final_vwap + 2 * final_stdev,
        "LOWER_2": final_vwap - 2 * final_stdev,
        "UPPER_3": final_vwap + 3 * final_stdev,
        "LOWER_3": final_vwap - 3 * final_stdev,
    }
    return final_vwap, final_stdev, bands


def get_atr20(df: pd.DataFrame, length: int = 20):
    """Return a standard ATR of ``length`` days using True Range."""
    if df is None or df.empty or len(df) < length + 1:
        return None

    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values

    true_ranges: list[float] = []
    prev_close = closes[0]
    for i in range(1, len(df)):
        high = highs[i]
        low = lows[i]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
        prev_close = closes[i]

    if len(true_ranges) < length:
        return None

    atr_series = pd.Series(true_ranges).rolling(length).mean()
    atr = atr_series.iloc[-1]
    if pd.isna(atr) or atr <= 0:
        return None
    return float(atr)


def bounce_up_at_level(
    df: pd.DataFrame,
    level: float,
    *,
    atr: float | None = None,
    atr_length: int = 20,
    atr_mult: float = 0.05,
) -> bool:
    """Return True if price bounced up from ``level`` using ATR thresholds."""
    if level is None or pd.isna(level) or len(df) < atr_length + 3:
        return False

    atr_value = atr if atr is not None else get_atr20(df, length=atr_length)
    if atr_value is None:
        return False

    eps = atr_mult * atr_value
    push = atr_mult * atr_value

    _, bar_b, bar_c = df.iloc[-3], df.iloc[-2], df.iloc[-1]

    touched = bar_b.low <= level + eps
    reclaimed = bar_b.close >= level
    confirm = bar_c.close > bar_b.close and bar_c.close >= level + push
    return bool(touched and reclaimed and confirm)


def bounce_down_at_level(
    df: pd.DataFrame,
    level: float,
    *,
    atr: float | None = None,
    atr_length: int = 20,
    atr_mult: float = 0.05,
) -> bool:
    """Return True if price rejected ``level`` downward using ATR thresholds."""
    if level is None or pd.isna(level) or len(df) < atr_length + 3:
        return False

    atr_value = atr if atr is not None else get_atr20(df, length=atr_length)
    if atr_value is None:
        return False

    eps = atr_mult * atr_value
    push = atr_mult * atr_value

    _, bar_b, bar_c = df.iloc[-3], df.iloc[-2], df.iloc[-1]

    touched = bar_b.high >= level - eps
    rejected = bar_b.close <= level
    confirm = bar_c.close < bar_b.close and bar_c.close <= level - push
    return bool(touched and rejected and confirm)


__all__ = [
    "load_tickers_from_file",
    "load_cache",
    "save_cache",
    "fetch_earnings_for_date",
    "collect_earnings_dates",
    "fetch_past_earnings_from_yfinance",
    "create_contract",
    "fetch_daily_bars",
    "calc_anchored_vwap_bands",
    "get_atr20",
    "bounce_up_at_level",
    "bounce_down_at_level",
]
