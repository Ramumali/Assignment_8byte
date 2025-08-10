# workspace/libs/stock_loader.py
import os
import requests
import logging
import time
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

logger = logging.getLogger("stock_loader")
logger.setLevel(logging.INFO)

ALPHA_URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv("KY71G44UNO4W4XC9")

# Database connection settings read from env
DB_HOST = os.getenv("POSTGRES_HOST", os.getenv("POSTGRES_HOST", "postgres"))
DB_NAME = os.getenv("POSTGRES_DB", "pipeline_db")
DB_USER = os.getenv("POSTGRES_USER", "pipeline_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")
DB_PORT = int(os.getenv("POSTGRES_PORT", 5432))

# Table name
TABLE_NAME = "stocks_time_series"

# Retry helper
def _request_with_retries(url, params, retries=3, backoff=2):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning("Request attempt %d failed: %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
            else:
                raise

def fetch_daily_time_series(symbol: str, outputsize: str = "compact") -> Dict:
    """
    Calls Alpha Vantage TIME_SERIES_DAILY_ADJUSTED and returns parsed JSON.
    outputsize: 'compact' (last 100) or 'full' (full-length)
    """
    if not API_KEY:
        raise EnvironmentError("ALPHA_VANTAGE_API_KEY not set")

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": outputsize,
        "datatype": "json",
    }
    resp = _request_with_retries(ALPHA_URL, params)
    j = resp.json()
    # AlphaVantage uses keys like "Time Series (Daily)"
    ts_key = None
    for k in j.keys():
        if "Time Series" in k:
            ts_key = k
            break
    if ts_key is None:
        # Possibly an error message from API in 'Note' or 'Error Message'
        logger.error("Unexpected response: %s", j)
        raise ValueError("Time series not found in API response")
    return j[ts_key]

def parse_time_series_json(time_series_json: Dict, symbol: str) -> List[tuple]:
    """
    Convert AlphaVantage time series to list of tuples matching database schema:
    (symbol, ts, open, high, low, close, adjusted_close, volume)
    Accepts missing values and converts to None.
    """
    rows = []
    for ts_str, values in time_series_json.items():
        try:
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            # fallback
            ts = datetime.strptime(ts_str, "%Y-%m-%d")
        def gv(k):
            v = values.get(k) or values.get(k.replace(" ", "")) or None
            # some AV keys: '1. open', '2. high', etc. Fallback to any matching numeric-like.
            return v

        # common keys in AlphaVantage daily adjusted:
        open_v = values.get("1. open")
        high_v = values.get("2. high")
        low_v = values.get("3. low")
        close_v = values.get("4. close")
        adj_close_v = values.get("5. adjusted close") or values.get("5. adjusted_close") or values.get("5. adjusted")
        volume_v = values.get("6. volume") or values.get("6.volume") or values.get("5. volume")

        # robust conversions
        def to_num(x):
            try:
                if x is None:
                    return None
                return float(x)
            except Exception:
                return None
        def to_int(x):
            try:
                if x is None:
                    return None
                return int(float(x))
            except Exception:
                return None

        rows.append((
            symbol.upper(),
            ts,
            to_num(open_v),
            to_num(high_v),
            to_num(low_v),
            to_num(close_v),
            to_num(adj_close_v),
            to_int(volume_v),
        ))
    return rows

def upsert_rows(rows: List[tuple]):
    """
    Upsert into Postgres table. Uses primary key (symbol, ts).
    """
    if not rows:
        logger.info("No rows to upsert.")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with conn:
            with conn.cursor() as cur:
                insert_sql = f"""
                INSERT INTO {TABLE_NAME} (
                    symbol, ts, open, high, low, close, adjusted_close, volume
                ) VALUES %s
                ON CONFLICT (symbol, ts) DO UPDATE SET
                  open = EXCLUDED.open,
                  high = EXCLUDED.high,
                  low = EXCLUDED.low,
                  close = EXCLUDED.close,
                  adjusted_close = EXCLUDED.adjusted_close,
                  volume = EXCLUDED.volume;
                """
                execute_values(cur, insert_sql, rows, template=None, page_size=100)
        logger.info("Upserted %d rows", len(rows))
    except Exception as e:
        logger.exception("Failed to upsert rows: %s", e)
        raise
    finally:
        if conn:
            conn.close()

def fetch_and_store(symbol: str, outputsize: str = "compact"):
    """
    High-level helper: fetch, parse, and upsert. Returns number of rows processed.
    """
    logger.info("Starting fetch_and_store for symbol=%s", symbol)
    try:
        ts_json = fetch_daily_time_series(symbol, outputsize=outputsize)
    except Exception as e:
        logger.error("Failed to fetch data for %s: %s", symbol, e)
        raise

    rows = parse_time_series_json(ts_json, symbol)
    if not rows:
        logger.warning("No parsed rows for %s", symbol)
        return 0

    # sort by timestamp ascending for consistent insertion (not required)
    rows_sorted = sorted(rows, key=lambda r: r[1])
    upsert_rows(rows_sorted)
    return len(rows_sorted)