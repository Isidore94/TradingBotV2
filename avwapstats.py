#!/usr/bin/env python3
import argparse, csv, json, logging, os, threading, time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%H:%M:%S")

DEFAULT_SIGNALS_FILE = "master_signals.txt"
DEFAULT_RESULTS_FILE = "results_master.csv"
DEFAULT_STATS_FILE   = "stats_by_setup.csv"
DEFAULT_EARNINGS_CACHE = "earnings_cache.json"

@dataclass(frozen=True)
class Signal:
    symbol: str
    signal_date: date
    raw_level: str
    side: str

@dataclass
class Bands:
    VWAP: float
    UPPER_1: float
    LOWER_1: float
    UPPER_2: float
    LOWER_2: float
    UPPER_3: float
    LOWER_3: float

@dataclass
class TradeResult:
    trade_id: str
    symbol: str
    setup: str
    strategy: str
    raw_level: str
    side: str
    signal_date: str
    entry_date: str
    exit_date: str
    outcome: str
    holding_days: int
    entry_px: float
    exit_px: float
    pct_return: float
    mfe_pct: float
    mae_pct: float
    notes: str

class IB(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self._data, self._ready, self._err = {}, {}, []

    def error(self, reqId, code, msg, advancedOrderRejectJson=""):
        if code not in (2104,2106,2158,2176):
            logging.warning(f"IB error {code}[{reqId}]: {msg}")
            self._err.append((reqId, code, msg))

    def historicalData(self, reqId, bar):
        self._data.setdefault(reqId, []).append({"date": bar.date, "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume})

    def historicalDataEnd(self, reqId, start, end):
        self._ready[reqId] = True

def ib_contract(symbol: str) -> Contract:
    c = Contract(); c.symbol=symbol; c.secType="STK"; c.currency="USD"; c.exchange="SMART"; return c

def ib_fetch_daily(ib: IB, symbol: str, start_date: date, end_date: date, timeout_s=20) -> pd.DataFrame:
    dur_days = (end_date - start_date).days + 8
    reqId = int(time.time()*1000) % 2_000_000_000
    ib._data[reqId]=[]; ib._ready[reqId]=False
    ib.reqHistoricalData(reqId=reqId, contract=ib_contract(symbol), endDateTime="", durationStr=f"{max(dur_days,5)} D",
                         barSizeSetting="1 day", whatToShow="TRADES", useRTH=1, formatDate=1, keepUpToDate=False, chartOptions=[])
    t0=time.time()
    while not ib._ready.get(reqId, False) and time.time()-t0<timeout_s: time.sleep(0.25)
    bars=ib._data.pop(reqId, []); ib._ready.pop(reqId, None)
    if not bars: return pd.DataFrame()
    df=pd.DataFrame(bars)
    df["Date"]=pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"}, inplace=True)
    df=df[["Date","Open","High","Low","Close","Volume"]].dropna()
    return df[(df["Date"]>=start_date)&(df["Date"]<=end_date)].reset_index(drop=True)

def load_earnings_cache(path: str) -> Dict[str, str]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    logging.warning(f"Earnings cache not found at {path}."); return {}

def yf_last_past_earnings(symbol: str) -> Optional[date]:
    try:
        t=yf.Ticker(symbol); ed=t.get_earnings_dates(limit=8)
        if ed is None or ed.empty: return None
        ed.index=ed.index.tz_localize(None)
        past=ed[ed.index<pd.Timestamp.today().tz_localize(None)]
        if past.empty: return None
        return past.index.max().date()
    except Exception as e:
        logging.warning(f"yfinance earnings lookup failed for {symbol}: {e}"); return None

def get_anchor_date(symbol: str, cache: Dict[str, str]) -> Optional[date]:
    if symbol in cache:
        try: return datetime.fromisoformat(cache[symbol]).date()
        except Exception: pass
    return yf_last_past_earnings(symbol)

def infer_year(mmdd: str, today: date) -> date:
    m,d=map(int, mmdd.split("/")); dt=date(today.year,m,d)
    if dt>today+timedelta(days=3): dt=date(today.year-1,m,d)
    return dt

def parse_signals(path: str, today: Optional[date]=None) -> List[Signal]:
    if not os.path.exists(path): logging.error(f"Signals file not found: {path}"); return []
    if today is None: today=datetime.now().date()
    out=[]
    with open(path,"r",encoding="utf-8") as f:
        for raw in f:
            line=raw.strip()
            if not line or line.startswith("#"): continue
            parts=[p.strip() for p in line.split(",")]
            if len(parts)!=4: continue
            sym,mmdd,lvl,side=parts
            try: dt=infer_year(mmdd, today)
            except Exception: logging.warning(f"Bad date in line: {line}"); continue
            out.append(Signal(sym.upper(), dt, lvl.upper(), side.upper()))
    return out

def calc_bands_asof(df: pd.DataFrame, anchor_date: date, asof_date: date) -> Optional[Bands]:
    if df.empty: return None
    try: aidx=df.index[df["Date"]>=anchor_date][0]
    except IndexError: return None
    try: eidx=df.index[df["Date"]<=asof_date][-1]
    except IndexError: return None
    if eidx<aidx: return None
    seg=df.loc[aidx:eidx]
    v=seg["Volume"].fillna(0).astype(float).values
    tp=((seg["Open"]+seg["High"]+seg["Low"]+seg["Close"])/4.0).astype(float).values
    v_sum=v.sum()
    if v_sum<=0: return None
    vwap=(tp*v).sum()/v_sum
    stdev=((v*((tp-vwap)*(tp-vwap))).sum()/v_sum)**0.5
    return Bands(VWAP=vwap, UPPER_1=vwap+1*stdev, LOWER_1=vwap-1*stdev, UPPER_2=vwap+2*stdev, LOWER_2=vwap-2*stdev, UPPER_3=vwap+3*stdev, LOWER_3=vwap-3*stdev)

def normalize_level(raw_level: str) -> str:
    return raw_level.replace("CROSS_UP_","",1) if raw_level.startswith("CROSS_UP_") else raw_level

def expand_strategies(sig: Signal, bands: Bands) -> List[Tuple[str, Dict]]:
    lvl=normalize_level(sig.raw_level); L=[]
    if sig.side=="LONG":
        if lvl=="UPPER_3":
            L.append(("u3_run_revert", {"type":"REVERT","pivot":bands.UPPER_3}))
        elif lvl=="UPPER_2":
            L.append(("u2_stop_u2_tp_u3", {"type":"TP_STOP","stop":bands.UPPER_2,"tp":bands.UPPER_3}))
        elif lvl=="UPPER_1":
            L.append(("u1_stop_u1_tp_u2", {"type":"TP_STOP","stop":bands.UPPER_1,"tp":bands.UPPER_2}))
            L.append(("u1_stop_vwap_tp_u2", {"type":"TP_STOP","stop":bands.VWAP,"tp":bands.UPPER_2}))
        elif lvl=="VWAP":
            L.append(("vwap_stop_vwap_tp_u1", {"type":"TP_STOP","stop":bands.VWAP,"tp":bands.UPPER_1}))
            L.append(("vwap_stop_l1_tp_u1", {"type":"TP_STOP","stop":bands.LOWER_1,"tp":bands.UPPER_1}))
    elif sig.side=="SHORT":
        if lvl=="LOWER_3":
            L.append(("l3_run_revert", {"type":"REVERT","pivot":bands.LOWER_3}))
        elif lvl=="LOWER_2":
            L.append(("l2_stop_l2_tp_l3", {"type":"TP_STOP","stop":bands.LOWER_2,"tp":bands.LOWER_3}))
        elif lvl=="LOWER_1":
            L.append(("l1_stop_l1_tp_l2", {"type":"TP_STOP","stop":bands.LOWER_1,"tp":bands.LOWER_2}))
            L.append(("l1_stop_vwap_tp_l2", {"type":"TP_STOP","stop":bands.VWAP,"tp":bands.LOWER_2}))
        elif lvl=="VWAP":
            L.append(("vwap_stop_vwap_tp_l1", {"type":"TP_STOP","stop":bands.VWAP,"tp":bands.LOWER_1}))
            L.append(("vwap_stop_u1_tp_l1", {"type":"TP_STOP","stop":bands.UPPER_1,"tp":bands.LOWER_1}))
    return L

def next_trading_open(df: pd.DataFrame, d: date) -> Optional[Tuple[date,float]]:
    after=df[df["Date"]>d]
    if after.empty: return None
    row=after.iloc[0]; return row["Date"], float(row["Open"])

def slice_hold_window(df: pd.DataFrame, entry_date: date, max_days=5) -> pd.DataFrame:
    after=df[df["Date"]>=entry_date]
    return after.iloc[:max_days].copy() if not after.empty else after

def simulate_trade(sig: Signal, strategy_name: str, rule: Dict, df: pd.DataFrame, bands: Bands, entry_date: date, entry_px: float) -> Tuple[str, date, float, int, float, float, str]:
    window=slice_hold_window(df, entry_date, max_days=5)
    if window.empty: return ("NO_DATA", entry_date, entry_px, 0, 0.0, 0.0, "no forward bars")
    mfe=-1e9; mae=1e9
    if sig.side=="LONG":
        for i,row in window.iterrows():
            mfe=max(mfe, (float(row["High"])-entry_px)/entry_px)
            mae=min(mae, (float(row["Low"])-entry_px)/entry_px)
            close=float(row["Close"]); d=row["Date"]
            if rule["type"]=="TP_STOP":
                stop=float(rule["stop"]); tp=float(rule["tp"])
                if close<stop: return ("STOP", d, close, window.index.get_loc(i)+1, mfe, mae, f"close<{stop:.4f}")
                if close>=tp: return ("TP", d, close, window.index.get_loc(i)+1, mfe, mae, f"close>=tp {tp:.4f}")
            else:
                pivot=float(rule["pivot"])
                if close<pivot: return ("STOP", d, close, window.index.get_loc(i)+1, mfe, mae, f"revert close<{pivot:.4f}")
        last=window.iloc[-1]; return ("TIMEOUT", last["Date"], float(last["Close"]), len(window), mfe, mae, "5-day timeout")
    else:
        for i,row in window.iterrows():
            mfe=max(mfe, (entry_px-float(row["Low"]))/entry_px)
            mae=min(mae, (entry_px-float(row["High"]))/entry_px)
            close=float(row["Close"]); d=row["Date"]
            if rule["type"]=="TP_STOP":
                stop=float(rule["stop"]); tp=float(rule["tp"])
                if close>stop: return ("STOP", d, close, window.index.get_loc(i)+1, mfe, mae, f"close>{stop:.4f}")
                if close<=tp: return ("TP", d, close, window.index.get_loc(i)+1, mfe, mae, f"close<=tp {tp:.4f}")
            else:
                pivot=float(rule["pivot"])
                if close>pivot: return ("STOP", d, close, window.index.get_loc(i)+1, mfe, mae, f"revert close>{pivot:.4f}")
        last=window.iloc[-1]; return ("TIMEOUT", last["Date"], float(last["Close"]), len(window), mfe, mae, "5-day timeout")

RESULT_HEADER=["trade_id","symbol","setup","strategy","raw_level","side","signal_date","entry_date","exit_date","outcome","holding_days","entry_px","exit_px","pct_return","mfe_pct","mae_pct","notes"]

def read_existing_trade_ids(path: str) -> set:
    if not os.path.exists(path): return set()
    ids=set()
    with open(path,"r",encoding="utf-8") as f:
        for row in csv.DictReader(f): ids.add(row["trade_id"])
    return ids

def append_results(path: str, rows: List[TradeResult]) -> None:
    exists=os.path.exists(path)
    with open(path,"a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=RESULT_HEADER)
        if not exists: w.writeheader()
        for r in rows: w.writerow(r.__dict__)

def rebuild_stats(results_csv: str, stats_csv: str) -> None:
    if not os.path.exists(results_csv): return
    df=pd.read_csv(results_csv)
    if df.empty: return

    def outcome_win(row):
        if row["strategy"] in ("u3_run_revert","l3_run_revert"):
            return 1 if row["pct_return"]>0 else 0
        return 1 if row["outcome"]=="TP" else 0

    df["is_win"]=df.apply(outcome_win, axis=1)

    def r_rate(sub, label):
        return (sub["outcome"]==label).mean()

    stats=(df.groupby(["setup","strategy"])
             .apply(lambda g: pd.Series({
                 "n": len(g),
                 "win_rate": g["is_win"].mean(),
                 "avg_ret": g["pct_return"].mean(),
                 "med_ret": g["pct_return"].median(),
                 "avg_days": g["holding_days"].mean(),
                 "tp_rate": r_rate(g,"TP"),
                 "stop_rate": r_rate(g,"STOP"),
                 "timeout_rate": r_rate(g,"TIMEOUT"),
                 "avg_mfe": g["mfe_pct"].mean(),
                 "avg_mae": g["mae_pct"].mean(),
             }))
             .reset_index())

    for c in ["win_rate","avg_ret","med_ret","avg_days","tp_rate","stop_rate","timeout_rate","avg_mfe","avg_mae"]:
        stats[c]=stats[c].round(4 if c!="avg_days" else 2)

    stats.to_csv(stats_csv, index=False)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--signals", default=DEFAULT_SIGNALS_FILE)
    ap.add_argument("--earnings-cache", default=DEFAULT_EARNINGS_CACHE)
    ap.add_argument("--results", default=DEFAULT_RESULTS_FILE)
    ap.add_argument("--stats", default=DEFAULT_STATS_FILE)
    ap.add_argument("--ib-host", default="127.0.0.1")
    ap.add_argument("--ib-port", type=int, default=7496)
    ap.add_argument("--ib-client", type=int, default=1001)
    args=ap.parse_args()

    signals=parse_signals(args.signals)
    if not signals: logging.error("No signals parsed. Exiting."); return
    cache=load_earnings_cache(args.earnings_cache)

    ib=IB()
    try:
        ib.connect(args.ib_host, args.ib_port, clientId=args.ib_client)
        threading.Thread(target=ib.run, daemon=True).start()
        time.sleep(1.25); ib_connected=True; logging.info("Connected to IB.")
    except Exception as e:
        logging.warning(f"Failed to connect to IB: {e}"); ib_connected=False

    seen_ids=read_existing_trade_ids(args.results)
    new_rows: List[TradeResult]=[]

    for sig in signals:
        setup_key=f"{normalize_level(sig.raw_level)}_{sig.side}"
        anchor=get_anchor_date(sig.symbol, cache)
        end_needed=sig.signal_date+timedelta(days=10)
        start_needed=(anchor or (sig.signal_date-timedelta(days=180)))

        if ib_connected and anchor:
            df=ib_fetch_daily(ib, sig.symbol, start_needed, end_needed)
        else:
            df=yf.download(sig.symbol, start=start_needed-timedelta(days=7), end=end_needed+timedelta(days=2), interval="1d", auto_adjust=False, progress=False)
            if not df.empty:
                df=df.rename(columns=str.title)[['Open','High','Low','Close','Volume']].copy()
                df.index=df.index.tz_localize(None); df["Date"]=df.index.date; df=df.reset_index(drop=True)

        if df.empty:
            trade_id=f"{sig.symbol}|{sig.signal_date.isoformat()}|{sig.raw_level}|{sig.side}|NO_DATA"
            if trade_id not in seen_ids:
                new_rows.append(TradeResult(trade_id, sig.symbol, setup_key, "NO_DATA", sig.raw_level, sig.side, sig.signal_date.isoformat(), "", "", "NO_DATA", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "no bars"))
                seen_ids.add(trade_id)
            continue

        if not anchor: anchor=df["Date"].min()
        bands=calc_bands_asof(df, anchor, sig.signal_date)
        if bands is None:
            trade_id=f"{sig.symbol}|{sig.signal_date.isoformat()}|{sig.raw_level}|{sig.side}|NO_BANDS"
            if trade_id not in seen_ids:
                new_rows.append(TradeResult(trade_id, sig.symbol, setup_key, "NO_BANDS", sig.raw_level, sig.side, sig.signal_date.isoformat(), "", "", "SKIPPED", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "could not compute bands"))
                seen_ids.add(trade_id)
            continue

        strategies=expand_strategies(sig, bands)
        if not strategies:
            trade_id=f"{sig.symbol}|{sig.signal_date.isoformat()}|{sig.raw_level}|{sig.side}|UNSUPPORTED"
            if trade_id not in seen_ids:
                new_rows.append(TradeResult(trade_id, sig.symbol, setup_key, "UNSUPPORTED", sig.raw_level, sig.side, sig.signal_date.isoformat(), "", "", "SKIPPED", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "signal+side not specified in rules"))
                seen_ids.add(trade_id)
            continue

        nxt=next_trading_open(df, sig.signal_date)
        if nxt is None:
            for sname,_ in strategies:
                trade_id=f"{sig.symbol}|{sig.signal_date.isoformat()}|{sig.raw_level}|{sig.side}|{sname}"
                if trade_id in seen_ids: continue
                new_rows.append(TradeResult(trade_id, sig.symbol, setup_key, sname, sig.raw_level, sig.side, sig.signal_date.isoformat(), "", "", "NO_DATA", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "no next bar"))
                seen_ids.add(trade_id)
            continue

        entry_date, entry_px = nxt
        for sname, rule in strategies:
            trade_id=f"{sig.symbol}|{sig.signal_date.isoformat()}|{sig.raw_level}|{sig.side}|{sname}"
            if trade_id in seen_ids: continue
            outcome, exit_date, exit_px, hold_days, mfe, mae, notes = simulate_trade(sig, sname, rule, df, bands, entry_date, entry_px)
            if outcome=="NO_DATA": pct_ret=0.0
            else:
                pct_ret=((exit_px-entry_px)/entry_px) if sig.side=="LONG" else ((entry_px-exit_px)/entry_px)
            new_rows.append(TradeResult(
                trade_id=trade_id, symbol=sig.symbol, setup=setup_key, strategy=sname, raw_level=sig.raw_level, side=sig.side,
                signal_date=sig.signal_date.isoformat(),
                entry_date=entry_date.isoformat(),
                exit_date=exit_date.isoformat() if isinstance(exit_date, date) else "",
                outcome=outcome, holding_days=hold_days,
                entry_px=round(float(entry_px),6),
                exit_px=round(float(exit_px),6) if isinstance(exit_px,(int,float)) else 0.0,
                pct_return=round(float(pct_ret),6),
                mfe_pct=round(float(mfe),6) if isinstance(mfe,(int,float)) else 0.0,
                mae_pct=round(float(mae),6) if isinstance(mae,(int,float)) else 0.0,
                notes=notes
            ))
            seen_ids.add(trade_id)

    if new_rows:
        append_results(args.results, new_rows)
        logging.info(f"Appended {len(new_rows)} trades to {args.results}")
    else:
        logging.info("No new trades to append.")

    rebuild_stats(args.results, args.stats)
    logging.info(f"Stats written to {args.stats}")

    try: ib.disconnect()
    except Exception: pass

if __name__ == "__main__":
    main()