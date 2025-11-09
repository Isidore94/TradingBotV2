#!/usr/bin/env python3
import requests
import time
import logging
from datetime import datetime, timedelta
import os
import sys

# ── Configuration ──────────────────────────────────────────────────────────────
SYMBOLS_FILE      = "symbols.txt"
OUTPUT_FILE       = "earnings_date.txt"
API_URL           = "https://api.nasdaq.com/api/calendar/earnings?date={date}"
HEADERS           = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*"
}
MAX_LOOKBACK_DAYS = 150   # only check back 90 days
RECENT_DAYS       = 10   # if earnings within this window, pick the previous date

# ── Logging Setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

def load_symbols(filename):
    if not os.path.exists(filename):
        logging.error(f"Symbols file not found: {filename}")
        sys.exit(1)
    with open(filename, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]

def fetch_earnings_for_date(date_str):
    try:
        resp = requests.get(API_URL.format(date=date_str), headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return resp.json().get("data", {}).get("rows", []) or []
    except Exception as e:
        logging.warning(f"Failed to fetch {date_str}: {e}")
        time.sleep(0.5)
        return []

def collect_earnings_dates(symbols, lookback_days):
    """
    Build a list of all earnings dates (up to lookback_days) per symbol.
    """
    symbol_dates = {sym: [] for sym in symbols}
    today = datetime.now().date()

    for delta in range(lookback_days):
        check_date = today - timedelta(days=delta)
        date_str = check_date.strftime("%Y-%m-%d")
        # progress log every 15 days
        if delta % 15 == 0:
            logging.info(f"Checking {date_str} (back {delta} days)...")
        rows = fetch_earnings_for_date(date_str)
        for row in rows:
            sym = row.get("symbol", "").upper()
            if sym in symbol_dates and date_str not in symbol_dates[sym]:
                symbol_dates[sym].append(date_str)
        # if every symbol has at least two dates, you could break early:
        if all(len(dates) >= 2 for dates in symbol_dates.values()):
            logging.info("Collected ≥2 dates for all symbols; stopping search.")
            break

    return symbol_dates

def select_final_dates(symbol_dates):
    """
    For each symbol, if its most recent date is within RECENT_DAYS,
    choose the previous date; otherwise choose the most recent.
    """
    today = datetime.now().date()
    final = {}
    for sym, dates in symbol_dates.items():
        if not dates:
            continue
        # dates are in descending order by construction
        most_recent = datetime.fromisoformat(dates[0]).date()
        if (today - most_recent).days <= RECENT_DAYS and len(dates) > 1:
            final[sym] = dates[1]
            logging.info(f"{sym}: recent earnings {dates[0]} within {RECENT_DAYS}d, using previous {dates[1]}")
        else:
            final[sym] = dates[0]
            logging.info(f"{sym}: using earnings date {dates[0]}")
    return final

def write_earnings_dates(output_file, earnings_dict):
    with open(output_file, "w") as f:
        for sym in sorted(earnings_dict):
            f.write(f"{sym},{earnings_dict[sym]}\n")
    logging.info(f"Wrote {len(earnings_dict)} entries to '{output_file}'.")

def main():
    symbols      = load_symbols(SYMBOLS_FILE)
    symbol_dates = collect_earnings_dates(symbols, MAX_LOOKBACK_DAYS)
    final_dates  = select_final_dates(symbol_dates)
    write_earnings_dates(OUTPUT_FILE, final_dates)

    missing = set(symbols) - final_dates.keys()
    if missing:
        logging.warning(f"No earnings date found for: {', '.join(sorted(missing))}")

if __name__ == "__main__":
    main()
