#!/usr/bin/env python3

import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

try:
    from tabulate import tabulate
    USE_TABULATE = True
except ImportError:
    USE_TABULATE = False

##############################################################################
# 1) CONFIG
##############################################################################
load_dotenv("../config/.env")  # or adjust if needed
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

SCHEMA_NAME = "fronttest"

SYMBOLS = ["es", "eurusd", "spy"]
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "1d"]  # skipping "4h"

YFINANCE_SYMBOLS = {
    "es":     "ES=F",
    "eurusd": "EURUSD=X",
    "spy":    "SPY",
}

CANDLE_MATCH_THRESHOLDS = {
    "es":     0.25,
    "eurusd": 0.0005,
    "spy":    0.1
}

##############################################################################
# 2) HELPER FUNCTIONS
##############################################################################
def compute_candle_color(o, c):
    if c > o:
        return "green"
    elif c < o:
        return "red"
    else:
        return "doji"

def is_close_enough(val1, val2, threshold):
    return abs(val1 - val2) <= threshold

def map_tf_to_yf_interval(tf):
    """For yfinance: '1h' => '60m'; otherwise same."""
    if tf == "1h":
        return "60m"
    else:
        return tf

def adjust_daily_timestamp(symbol, orig_dt):
    """Daily bars => SPY=14:30 UTC, others=00:00 UTC."""
    if symbol == "spy":
        return datetime(orig_dt.year, orig_dt.month, orig_dt.day, 14, 30, 0, tzinfo=timezone.utc)
    else:
        return datetime(orig_dt.year, orig_dt.month, orig_dt.day, 0, 0, 0, tzinfo=timezone.utc)

##############################################################################
# 3) FETCH THE LATEST YFINANCE CANDLE (FOR "UP TO DATE?" CHECK)
##############################################################################
def fetch_latest_yf_candle(symbol, timeframe):
    """
    Pull a short period from yfinance to get the newest candle for comparison.
    Returns {timestamp, open, high, low, close, volume} or None if error/empty.
    """
    import yfinance as yf
    yf_symbol = YFINANCE_SYMBOLS[symbol]
    yf_interval = map_tf_to_yf_interval(timeframe)

    # For daily => 1mo, else 5d
    period = "1mo" if timeframe == "1d" else "5d"

    ticker = yf.Ticker(yf_symbol)
    try:
        hist = ticker.history(period=period, interval=yf_interval)
    except Exception as e:
        print(f"[ERROR] fetch_latest_yf_candle: {symbol} {timeframe} => {e}")
        return None

    if hist.empty:
        return None

    # Convert to UTC
    if hist.index.tz is None:
        hist.index = hist.index.tz_localize(timezone.utc)
    else:
        hist.index = hist.index.tz_convert(timezone.utc)

    last_dt = hist.index[-1]
    row = hist.iloc[-1]

    # Adjust daily
    if timeframe == "1d":
        last_dt = adjust_daily_timestamp(symbol, last_dt)
    # If eurusd & 5m => shift +5h if consistent with your data logic
    if symbol == "eurusd" and timeframe == "5m":
        last_dt = last_dt + timedelta(hours=5)

    return {
        "timestamp": last_dt,
        "open":   float(row["Open"]),
        "high":   float(row["High"]),
        "low":    float(row["Low"]),
        "close":  float(row["Close"]),
        "volume": int(row["Volume"]) if not row.isna().Volume else 0,
    }

##############################################################################
# 4) GET THE LATEST FRONTTEST DB TIMESTAMP
##############################################################################
def get_fronttest_latest_ts(symbol, timeframe):
    table_name = f"{SCHEMA_NAME}.{symbol}_{timeframe}"
    sql = text(f"SELECT MAX(timestamp) AS max_ts FROM {table_name}")
    with engine.begin() as conn:
        row = conn.execute(sql).fetchone()
    max_ts = row.max_ts if row else None
    if not max_ts:
        return None
    if not max_ts.tzinfo:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    else:
        max_ts = max_ts.astimezone(timezone.utc)
    return max_ts

##############################################################################
# 5) CHECK IF FRONTTEST TABLE IS UP TO DATE
##############################################################################
def is_up_to_date(symbol, timeframe):
    """
    Compare newest fronttest candle vs. newest yfinance candle
    with ~90-second tolerance.
    Return (db_ts_str, bool).
    """
    yf_candle = fetch_latest_yf_candle(symbol, timeframe)
    if not yf_candle:
        return ("NO YF DATA", False)

    db_ts = get_fronttest_latest_ts(symbol, timeframe)
    if not db_ts:
        return ("NO FRONTTEST DATA", False)

    yf_dt = yf_candle["timestamp"]
    diff_sec = abs((yf_dt - db_ts).total_seconds())
    tolerance_sec = 90
    up_to_date = (diff_sec <= tolerance_sec)

    db_ts_str = db_ts.strftime("%Y-%m-%d %H:%M:%S %Z")
    return (db_ts_str, up_to_date)

##############################################################################
# 6) FETCH FULL YFINANCE HISTORY (PERIOD=MAX)
##############################################################################
def fetch_full_yf_history(symbol, timeframe):
    """
    Returns a list of dicts: 
      {timestamp, open, high, low, close, volume, candle_color}
    """
    import yfinance as yf
    yf_symbol = YFINANCE_SYMBOLS[symbol]
    yf_interval = map_tf_to_yf_interval(timeframe)

    ticker = yf.Ticker(yf_symbol)
    try:
        hist = ticker.history(period="max", interval=yf_interval)
    except Exception as e:
        print(f"[ERROR] fetch_full_yf_history: {symbol} {timeframe} => {e}")
        return []

    if hist.empty:
        return []

    if hist.index.tz is None:
        hist.index = hist.index.tz_localize(timezone.utc)
    else:
        hist.index = hist.index.tz_convert(timezone.utc)

    rows = []
    for dt, row in hist.iterrows():
        # Adjust daily
        if timeframe == "1d":
            dt = adjust_daily_timestamp(symbol, dt)
        # If eurusd & 5m => shift +5h if consistent
        if symbol == "eurusd" and timeframe == "5m":
            dt = dt + timedelta(hours=5)

        o = float(row["Open"])
        h = float(row["High"])
        l = float(row["Low"])
        c = float(row["Close"])
        v = int(row["Volume"]) if not row.isna().Volume else 0

        color = compute_candle_color(o, c)
        rows.append({
            "timestamp": dt,
            "open": o,
            "high": h,
            "low":  l,
            "close": c,
            "volume": v,
            "candle_color": color,
        })
    return rows

##############################################################################
# 7) UPSERT INTO fronttest.<symbol>_<timeframe> (INCLUDES CANDLE_COLOR)
##############################################################################
UPSERT_SQL = """
INSERT INTO {table_name}
    (symbol, timestamp, open, high, low, close, volume, candle_color)
VALUES
    (:symbol, :ts, :o, :h, :l, :c, :v, :cc)
ON CONFLICT (symbol, timestamp) DO UPDATE
    SET open         = EXCLUDED.open,
        high         = EXCLUDED.high,
        low          = EXCLUDED.low,
        close        = EXCLUDED.close,
        volume       = EXCLUDED.volume,
        candle_color = EXCLUDED.candle_color
"""

def upsert_rows(symbol, timeframe, candles):
    """
    Upsert each candle (which must have candle_color).
    Returns how many inserted/updated.
    """
    table_name = f"{SCHEMA_NAME}.{symbol}_{timeframe}"
    sql_str = UPSERT_SQL.format(table_name=table_name)
    count = 0
    with engine.begin() as conn:
        for c in candles:
            conn.execute(text(sql_str), {
                "symbol": symbol,
                "ts":     c["timestamp"],
                "o":      c["open"],
                "h":      c["high"],
                "l":      c["low"],
                "c":      c["close"],
                "v":      c["volume"],
                "cc":     c["candle_color"],
            })
            count += 1
    return count

##############################################################################
# 8) CHECK PUBLIC SCHEMA FOR 1m DEADZONE FILL
##############################################################################
def fetch_public_1m_data(symbol, start_ts, end_ts):
    """
    Query public.<symbol>_1m for a range, also compute candle_color.
    Return list of {timestamp, open, high, low, close, volume, candle_color}.
    """
    table_name = f"public.{symbol}_1m"
    sql = text(f"""
        SELECT timestamp, open, high, low, close, volume
          FROM {table_name}
         WHERE timestamp >= :start_ts
           AND timestamp <= :end_ts
         ORDER BY timestamp ASC
    """)

    data = []
    with engine.begin() as conn:
        rows = conn.execute(sql, {"start_ts": start_ts, "end_ts": end_ts}).fetchall()

    for r in rows:
        ts_utc = r.timestamp
        if not ts_utc.tzinfo:
            ts_utc = ts_utc.replace(tzinfo=timezone.utc)
        o = r.open
        c = r.close
        color = compute_candle_color(o, c)

        data.append({
            "timestamp": ts_utc,
            "open": o,
            "high": r.high,
            "low":  r.low,
            "close": c,
            "volume": r.volume,
            "candle_color": color,
        })
    return data

##############################################################################
# 9) FETCH LATEST FRONTTEST CANDLE (DETAILS)
##############################################################################
def fetch_fronttest_candle(symbol, timeframe, ts):
    """
    Get open, close, high, low, volume, candle_color
    from fronttest.<symbol>_<timeframe> at a given timestamp.
    Return dict or None if not found.
    """
    table_name = f"{SCHEMA_NAME}.{symbol}_{timeframe}"
    sql = text(f"""
        SELECT open, high, low, close, volume, candle_color
          FROM {table_name}
         WHERE timestamp = :ts
         LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"ts": ts}).fetchone()
    if not row:
        return None
    return {
        "timestamp": ts,
        "open": row.open,
        "high": row.high,
        "low": row.low,
        "close": row.close,
        "volume": row.volume,
        "candle_color": row.candle_color,
    }

##############################################################################
# 10) MAIN LOGIC
##############################################################################
def main():
    # 1) Check each fronttest table's status
    results = []
    out_of_date_list = []

    for sym in SYMBOLS:
        for tf in TIMEFRAMES:
            db_ts_str, up_to_date = is_up_to_date(sym, tf)
            table_name = f"{sym}_{tf}"
            status_icon = "✅" if up_to_date else "❌"
            results.append((table_name, db_ts_str, status_icon))
            if not up_to_date:
                out_of_date_list.append((sym, tf))

    # 2) Print table
    print("\n---- FRONTTEST TABLE STATUS ----")
    headers = ["Table", "Latest DB Candle", "Status"]
    if USE_TABULATE:
        from tabulate import tabulate  # already imported at top
        print(tabulate(results, headers=headers, tablefmt="github"))
    else:
        print("{:<20}  {:<25}  {:<5}".format(*headers))
        for row in results:
            print("{:<20}  {:<25}  {:<5}".format(*row))

    if not out_of_date_list:
        print("\n[INFO] All fronttest tables appear up-to-date.\n")
        return

    print()
    # 3) For each out-of-date symbol/timeframe, ask user if they'd like to update
    for sym, tf in out_of_date_list:
        # CHANGED HERE: Always answer "yes"
        ans = "yes"
        print(f"Update fronttest.{sym}_{tf}? (yes/no): {ans}")
        # if the script used to do something with ans not in ("yes", "y"), we just mimic acceptance:
        if ans not in ("yes", "y"):
            print(f"Skipping {sym}_{tf}.")
            continue

        # 3a) Fetch yfinance full data
        print(f"[INFO] Fetching period='max' from yfinance for {sym}_{tf}...")
        yf_rows = fetch_full_yf_history(sym, tf)
        if not yf_rows:
            print(f"[WARNING] No data fetched from yfinance for {sym}_{tf}. Skipping.")
            continue
        yf_rows.sort(key=lambda x: x["timestamp"])
        oldest_yf_dt = yf_rows[0]["timestamp"]
        newest_yf_dt = yf_rows[-1]["timestamp"]

        # 3b) Check the existing fronttest latest candle for mismatch
        db_latest_ts = get_fronttest_latest_ts(sym, tf)
        if db_latest_ts:
            match_yf = next((r for r in yf_rows if r["timestamp"] == db_latest_ts), None)
            if match_yf:
                db_candle = fetch_fronttest_candle(sym, tf, db_latest_ts)
                if db_candle:
                    threshold = CANDLE_MATCH_THRESHOLDS.get(sym, 0.01)
                    open_ok = is_close_enough(db_candle["open"], match_yf["open"], threshold)
                    close_ok = is_close_enough(db_candle["close"], match_yf["close"], threshold)
                    if open_ok and close_ok:
                        print(f"[INFO] For {sym}_{tf} at {db_latest_ts}, fronttest & yfinance match within {threshold}.")
                    else:
                        print(f"[WARNING] For {sym}_{tf} at {db_latest_ts}, mismatch between fronttest & yfinance.")
                        print(f"  fronttest => open={db_candle['open']}, close={db_candle['close']}")
                        print(f"  yfinance  => open={match_yf['open']}, close={match_yf['close']}")
                        # CHANGED HERE: Always pick "2"
                        choice = "2"
                        print(f"Which candle do you want to keep? (1=fronttest, 2=yfinance): {choice}")
                        if choice == "1":
                            match_yf["open"]   = db_candle["open"]
                            match_yf["close"]  = db_candle["close"]
                            match_yf["high"]   = db_candle["high"]
                            match_yf["low"]    = db_candle["low"]
                            match_yf["volume"] = db_candle["volume"]
                            match_yf["candle_color"] = db_candle["candle_color"]
                            print("  [INFO] Overwrote yfinance row in memory with fronttest data.")
                        else:
                            print("  [INFO] Kept yfinance candle. fronttest will be overwritten upon upsert.")

            if oldest_yf_dt > db_latest_ts:
                print(f"[WARNING] Deadzone detected for fronttest.{sym}_{tf}!")
                print(f"  The newest fronttest candle is {db_latest_ts},")
                print(f"  but yfinance's oldest candle is {oldest_yf_dt} => GAP in between.")

                if tf == "1m":
                    # CHANGED HERE: Always pick "yes"
                    ans_pub = "yes"
                    print(f"Check public schema for missing 1m data? (yes/no): {ans_pub}")
                    if ans_pub in ("yes","y"):
                        gap_start = db_latest_ts + timedelta(seconds=1)
                        gap_end   = oldest_yf_dt - timedelta(seconds=1)
                        if gap_end <= gap_start:
                            print("  [INFO] The gap is zero or negative range. Skipping public fill.")
                        else:
                            public_data = fetch_public_1m_data(sym, gap_start, gap_end)
                            if public_data:
                                print(f"  [INFO] Found {len(public_data)} 1m candles in public.{sym}_1m covering the gap.")
                                inserted_count = upsert_rows(sym, tf, public_data)
                                print(f"  [INFO] Inserted/updated {inserted_count} from public => fronttest.")
                            else:
                                print(f"  [INFO] No data found in public.{sym}_1m for that gap.")
                else:
                    print("  [INFO] Non-1m timeframe deadzone => no automatic fill from public schema.")

        else:
            print(f"[DEBUG] fronttest.{sym}_{tf} is empty. No deadzone check needed.")

        # 3c) Upsert final YF data into fronttest
        inserted = upsert_rows(sym, tf, yf_rows)
        print(f"[INFO] Inserted/updated {inserted} candles from yfinance into fronttest.{sym}_{tf} "
              f"(range: {oldest_yf_dt} -> {newest_yf_dt}).")

    print("\n[INFO] Done checking/updating fronttest tables.\n")


if __name__ == "__main__":
    main()
