"""Shared helpers for retrieving and caching earnings anchor dates."""

from __future__ import annotations

import json

import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
import yfinance as yf

EARNINGS_CACHE_FILE = "earnings_cache.json"

API_URL = "https://api.nasdaq.com/api/calendar/earnings?date={date}"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}

MAX_LOOKBACK_DAYS = 250
NASDAQ_THROTTLE_SECONDS = 1.0


def load_cache(path: str = EARNINGS_CACHE_FILE) -> Dict[str, dict]:
    """Load the earnings cache from *path*.

    Older single-date caches are tolerated by coercing them into the new
    dictionary structure.
    """
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError:
        logging.warning("Earnings cache file is corrupt; starting with empty cache.")
        return {}
    except FileNotFoundError:
        return {}

    cache: Dict[str, dict] = {}
    for sym, entry in raw.items():
        dates = _coerce_entry_to_dates(entry)
        if not dates:
            continue
        cache[sym] = _serialise_dates(dates)
    return cache


def save_cache(cache: Dict[str, dict], path: str = EARNINGS_CACHE_FILE) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def _coerce_entry_to_dates(entry) -> List[date]:
    """Normalise cache entries of varying legacy formats into date objects."""
    values: List[str] = []
    if entry is None:
        return []
    if isinstance(entry, str):
        values = [entry]
    elif isinstance(entry, dict):
        if "dates" in entry and isinstance(entry["dates"], list):
            values = [str(v) for v in entry["dates"]]
        else:
            for key in ("current", "previous", "latest", "prior"):
                if key in entry:
                    values.append(str(entry[key]))
    elif isinstance(entry, list):
        values = [str(v) for v in entry]

    out: List[date] = []
    seen = set()
    for val in values:
        try:
            d = datetime.fromisoformat(val).date()
        except (ValueError, TypeError):
            continue
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    out.sort(reverse=True)
    return out


def _serialise_dates(dates: Iterable[date]) -> dict:
    ordered = sorted({d for d in dates if isinstance(d, date)}, reverse=True)
    payload: dict = {}
    if ordered:
        payload["current"] = ordered[0].isoformat()
    if len(ordered) > 1:
        payload["previous"] = ordered[1].isoformat()
    if len(ordered) > 2:
        payload["dates"] = [d.isoformat() for d in ordered]
    return payload


def get_cached_dates(cache: Dict[str, dict], symbol: str) -> List[date]:
    if symbol not in cache:
        return []
    return _coerce_entry_to_dates(cache[symbol])


def fetch_earnings_for_date(date_str: str) -> List[dict]:
    try:
        resp = requests.get(API_URL.format(date=date_str), headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("rows", [])
        return data or []
    except Exception as exc:  # noqa: BLE001 - surface in logs but continue
        logging.warning(f"Failed to fetch earnings for {date_str}: {exc}")
        time.sleep(0.5)
        return []


def collect_nasdaq_dates(symbols: Iterable[str], min_count: int = 2) -> Dict[str, List[date]]:
    symbols = list({s.upper() for s in symbols})
    if not symbols:
        return {}

    results: Dict[str, List[date]] = {s: [] for s in symbols}
    today = datetime.now().date()

    for delta in range(MAX_LOOKBACK_DAYS):
        query_date = today - timedelta(days=delta)
        rows = fetch_earnings_for_date(query_date.isoformat())
        time.sleep(NASDAQ_THROTTLE_SECONDS)
        if not rows:
            continue
        for row in rows:
            sym = str(row.get("symbol", "")).upper()
            if sym not in results:
                continue
            iso = query_date.isoformat()
            if iso not in [d.isoformat() for d in results[sym]]:
                results[sym].append(query_date)
        if all(len(v) >= min_count for v in results.values()):
            break

    for sym, dates in results.items():
        filtered = [d for d in dates if d <= today]
        filtered.sort(reverse=True)
        results[sym] = filtered
    return results


def _merge_dates(*collections: Iterable[date]) -> List[date]:
    seen = set()
    ordered: List[date] = []
    for col in collections:
        if not col:
            continue
        for d in col:
            if not isinstance(d, date):
                continue
            if d in seen:
                continue
            seen.add(d)
            ordered.append(d)
    ordered.sort(reverse=True)
    return ordered


def _yfinance_dates(symbol: str, limit: int = 8) -> List[date]:
    try:
        t = yf.Ticker(symbol)
        ed = t.get_earnings_dates(limit=limit)
        if ed is None or ed.empty:
            return []
        ed.index = ed.index.tz_localize(None)
        past = ed[ed.index < pd.Timestamp.today()]
        if past.empty:
            return []
        return sorted([ts.date() for ts in past.index], reverse=True)
    except Exception as exc:  # noqa: BLE001 - surface in logs but continue
        logging.warning(f"yfinance earnings lookup failed for {symbol}: {exc}")
        return []


def get_anchor_dates(
    symbol: str,
    cache: Optional[Dict[str, dict]] = None,
    nasdaq_dates: Optional[Iterable[date]] = None,
    min_count: int = 2,
) -> List[date]:
    """Return up to *min_count* past earnings dates for *symbol*.

    The lookup order is: cache → Nasdaq scrape → yfinance fallback.  Cache entries
    are updated in place when newer information is discovered.
    """
    today = datetime.now().date()
    cache = cache if cache is not None else {}

    cached = get_cached_dates(cache, symbol)
    cached = [d for d in cached if d <= today]

    nasdaq = [d for d in (nasdaq_dates or []) if isinstance(d, date) and d <= today]
    merged = _merge_dates(cached, nasdaq)

    if len(merged) < min_count:
        yf_dates = [d for d in _yfinance_dates(symbol) if d <= today]
        merged = _merge_dates(merged, yf_dates)

    if merged:
        cache[symbol] = _serialise_dates(merged)

    return merged[:min_count]
