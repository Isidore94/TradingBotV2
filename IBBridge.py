from flask import Flask, jsonify
import threading, time, os
from datetime import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData

# Adjustable parameter for the significant movers threshold.
MOVERS_THRESHOLD = 2.0

# ------------------------------
# IB API Client and Wrapper
# ------------------------------
class IBPriceFetcher(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.historical_data = []  # List to store historical bars from a request.
        self.data_ready = threading.Event()

    def historicalData(self, reqId, bar: BarData):
        # Append the bar to our list (do not print every bar)
        self.historical_data.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        # Signal that the historical data has finished coming in.
        self.data_ready.set()
        
# ------------------------------
# Utility Functions
# ------------------------------
def read_positions(file_path="positions.txt"):
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found.")
        return []
    with open(file_path, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    return tickers

def wait_for_candle_close():
    """
    Wait until the current 5-minute candle is complete.
    E.g., if the current time is 10:03, wait until 10:05:00.
    """
    now = time.localtime()
    elapsed = (now.tm_min % 5) * 60 + now.tm_sec
    seconds_to_wait = 300 - elapsed
    print(f"Waiting {seconds_to_wait} seconds for the 5-minute candle to close...")
    time.sleep(seconds_to_wait)

# Use a lock to serialize IB historical data requests.
fetch_lock = threading.Lock()

def fetch_latest_info(app, symbol="SPY"):
    """
    Request 2 days of 5-minute bar data for the given symbol from IB.
    Returns a dict with:
      - symbol
      - today_close: The close price of the latest complete 5-min candle (today)
      - today_time: The timestamp of that candle (as a datetime)
      - yesterday_close: The last 5-min candle close from the previous trading day
      - percent_change: ((today_close - yesterday_close) / yesterday_close)*100
    """
    with fetch_lock:
        # Clear out previous data.
        app.historical_data = []
        app.data_ready.clear()

        # Create IB contract.
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # Request 2 days of historical data at 5-min resolution.
        reqId = 1
        app.reqHistoricalData(
            reqId=reqId,
            contract=contract,
            endDateTime="",
            durationStr="2 D",
            barSizeSetting="5 mins",
            whatToShow="TRADES",
            useRTH=1,         # Changed to 1 to use only Regular Trading Hours
            formatDate=1,     # Return date as "YYYYMMDD  HH:MM:SS"
            keepUpToDate=False,
            chartOptions=[]
        )

        # Wait for data (timeout after 20 seconds).
        if not app.data_ready.wait(timeout=20):
            print(f"{symbol}: Historical data request timed out.")
            return None
        if not app.historical_data:
            print(f"{symbol}: No historical data received.")
            return None

        # Process received bars.
        processed = []
        for bar in app.historical_data:
            try:
                bar_time = datetime.strptime(bar.date.strip(), "%Y%m%d  %H:%M:%S")
            except Exception as e:
                print(f"{symbol}: Error parsing bar date '{bar.date}': {e}")
                continue
            processed.append((bar_time, bar.close))
        processed.sort(key=lambda x: x[0])
        if not processed:
            print(f"{symbol}: No valid bars parsed.")
            return None

        # Group bars by trading day.
        bars_by_date = {}
        for dt, close in processed:
            bars_by_date.setdefault(dt.date(), []).append((dt, close))
        sorted_days = sorted(bars_by_date.keys())
        if len(sorted_days) < 2:
            print(f"{symbol}: Not enough trading days' data returned.")
            return None

        today_day = sorted_days[-1]
        yesterday_day = sorted_days[-2]
        today_bars = bars_by_date[today_day]
        yesterday_bars = bars_by_date[yesterday_day]

        latest_today_time, today_close = today_bars[-1]
        
        # Find the market close bar for yesterday (typically around 16:00 ET)
        market_close_time = None
        yesterday_close = None
        
        # Look for a bar close to 16:00 (market close)
        for dt, close in yesterday_bars:
            if dt.hour == 16 and dt.minute < 15:  # Market close is at 16:00, allow some flexibility
                market_close_time = dt
                yesterday_close = close
                break
        
        # If we couldn't find a clear market close bar, fall back to the last bar of the day
        if yesterday_close is None:
            market_close_time, yesterday_close = yesterday_bars[-1]
            print(f"{symbol}: Couldn't find exact market close, using last bar of yesterday.")

        pct_change = ((today_close - yesterday_close) / yesterday_close) * 100

        return {
            "symbol": symbol,
            "current_price": today_close,
            "today_time": latest_today_time.strftime("%Y-%m-%d %H:%M:%S"),
            "yesterday_close": yesterday_close,
            "percent_change": round(pct_change, 2)
        }

# ------------------------------
# Flask App and Endpoints
# ------------------------------
app = Flask(__name__)

# Create a global IB API instance and connect.
ib_app = IBPriceFetcher()
# Connect to IB at host 127.0.0.1, port 7496 (adjust clientId as needed).
ib_app.connect("127.0.0.1", 7496, clientId=123)
# Run the IB API in a background thread.
ib_thread = threading.Thread(target=ib_app.run, daemon=True)
ib_thread.start()
# Allow a brief pause to ensure the connection is established.
time.sleep(2)

@app.route("/position/<ticker>", methods=["GET"])
def get_position(ticker):
    """Endpoint that returns price data for a given ticker."""
    info = fetch_latest_info(ib_app, symbol=ticker.upper())
    if info is None:
        return jsonify({"error": f"Failed to retrieve data for {ticker}."}), 500
    return jsonify(info)

@app.route("/movers/<float:threshold>", methods=["GET"])
def get_movers(threshold):
    """
    Endpoint that returns significant movers from positions.txt.
    For each ticker (excluding SPY), if the absolute percent change is greater
    than the given threshold, the ticker and its percent change are included.
    """
    tickers = read_positions("positions.txt")
    movers = {}
    for ticker in tickers:
        # Exclude SPY from the movers list.
        if ticker.upper() == "SPY":
            continue
        info = fetch_latest_info(ib_app, symbol=ticker.upper())
        if info and abs(info["percent_change"]) >= threshold:
            movers[ticker.upper()] = info["percent_change"]
    return jsonify(movers)


# ------------------------------
# Run Flask App
# ------------------------------
if __name__ == "__main__":
    # Run on all available interfaces at port 5000.
    app.run(host="0.0.0.0", port=5000, debug=False)
