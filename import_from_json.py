#!/usr/bin/env python3

import os
import json
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
from dotenv import load_dotenv

# Same symbol/timeframes
SYMBOLS = ["es", "eurusd", "spy"]
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]

def main():
    """
    1. Connect to the DB that already has empty 'backtest' and 'fronttest' tables (restored from .dump).
    2. Parse backtest_data.json => insert data into backtest.* tables.
    3. Parse fronttest_data.json => insert data into fronttest.* tables.
    """

    load_dotenv("../config/.env")  # or adjust if needed
    DB_URL = os.getenv("DB_URL") or "postgresql://postgres@localhost:5432/trading_data"
    engine = create_engine(DB_URL)

    # 1) Import backtest_data.json
    try:
        with open("backtest_data.json", "r", encoding="utf-8") as f:
            backtest_json = json.load(f)
    except FileNotFoundError:
        print("[ERROR] Could not find backtest_data.json. Exiting.")
        return

    print("\n[IMPORT] Now inserting rows into backtest tables...\n")
    for table_info in backtest_json["tables"]:
        table_short_name = table_info["table"]  # e.g. "es_1m"
        rows = table_info["rows"]
        full_table_name = f"backtest.{table_short_name}"

        if not rows:
            print(f"  [INFO] {full_table_name} has no rows in JSON. Skipping.")
            continue

        print(f"  [IMPORT] Inserting {len(rows)} rows into {full_table_name} ...")
        insert_rows(engine, full_table_name, rows)

    print("\n[INFO] Finished loading backtest_data.json.\n")

    # 2) Import fronttest_data.json
    try:
        with open("fronttest_data.json", "r", encoding="utf-8") as f:
            fronttest_json = json.load(f)
    except FileNotFoundError:
        print("[ERROR] Could not find fronttest_data.json. Exiting.")
        return

    print("[IMPORT] Now inserting rows into fronttest tables...\n")
    for table_info in fronttest_json["tables"]:
        table_short_name = table_info["table"]
        rows = table_info["rows"]
        full_table_name = f"fronttest.{table_short_name}"

        if not rows:
            print(f"  [INFO] {full_table_name} has no rows in JSON. Skipping.")
            continue

        print(f"  [IMPORT] Inserting {len(rows)} rows into {full_table_name} ...")
        insert_rows(engine, full_table_name, rows)

    print("\n[INFO] Finished loading fronttest_data.json.\n")


def insert_rows(engine, full_table_name, rows):
    """
    Insert each row into e.g. "backtest.es_1m", ignoring duplicates if already present.
    We'll do an upsert:
        ON CONFLICT (symbol, timestamp) DO UPDATE ...
    because your tables likely have PRIMARY KEY(symbol, timestamp).

    If the table is truly empty, it just inserts everything.
    """
    upsert_sql = text(f"""
        INSERT INTO {full_table_name}
            (symbol, timestamp, open, high, low, close, volume, candle_color)
        VALUES
            (:symbol, :timestamp, :open, :high, :low, :close, :volume, :candle_color)
        ON CONFLICT (symbol, timestamp) DO UPDATE
            SET open         = EXCLUDED.open,
                high         = EXCLUDED.high,
                low          = EXCLUDED.low,
                close        = EXCLUDED.close,
                volume       = EXCLUDED.volume,
                candle_color = EXCLUDED.candle_color
    """)

    with engine.begin() as conn:
        count = 0
        for r in rows:
            symbol = r["symbol"]
            # parse timestamp from ISO8601
            dt = parse_timestamp(r["timestamp"])
            o = r["open"]
            h = r["high"]
            l = r["low"]
            c = r["close"]
            v = r["volume"]
            color = r["candle_color"]

            conn.execute(upsert_sql, {
                "symbol": symbol,
                "timestamp": dt,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "candle_color": color
            })
            count += 1
        print(f"    [DONE] Inserted/Updated {count} rows in {full_table_name}.")


def parse_timestamp(ts_str):
    """
    Convert the ISO string (e.g. '2025-03-26T08:00:00+00:00') back to a Python datetime object.
    We'll let python parse it automatically. If your DB specifically needs naive UTC or 
    some other format, adjust accordingly.
    """
    # For standard library, we can do:
    return datetime.fromisoformat(ts_str)

if __name__ == "__main__":
    main()
