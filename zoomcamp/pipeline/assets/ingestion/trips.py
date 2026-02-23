"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

# Run after payment_lookup so only one process opens DuckDB at a time (DuckDB does not allow concurrent access).
depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Taxi type (e.g. yellow, green)"
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# PyArrow on Windows needs IANA tz data (UTC); use tzdata package or download to avoid ArrowInvalid
try:
    import pyarrow as pa
    try:
        import tzdata
        # Point PyArrow to the tzdata package so it can resolve UTC
        tzdata_root = os.path.join(os.path.dirname(tzdata.__file__), "zoneinfo")
        if os.path.isdir(tzdata_root):
            pa.set_timezone_db_path(os.path.dirname(tzdata_root))
    except ImportError:
        pa.util.download_tzdata_on_windows()
except Exception:
    pass

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _resolve_duckdb_path():
    """Resolve DuckDB database path from env or project layout (for direct write, bypassing ingestr)."""
    path = os.environ.get("BRUIN_DUCKDB_PATH")
    if path and os.path.isfile(path):
        return path
    conn_json = os.environ.get("BRUIN_CONNECTION_duckdb_default") or os.environ.get("duckdb-default")
    if conn_json:
        try:
            data = json.loads(conn_json) if isinstance(conn_json, str) else conn_json
            path = data.get("path") if isinstance(data, dict) else None
            if path and (os.path.isabs(path) or os.path.isfile(path)):
                return path
            if path:
                return os.path.abspath(path)
        except Exception:
            pass
    for candidate in ("duckdb.db", os.path.join("..", "..", "..", "duckdb.db"), os.path.join("..", "duckdb.db")):
        try:
            abs_path = os.path.abspath(candidate)
            if os.path.isfile(abs_path):
                return abs_path
            if candidate == "duckdb.db":
                return abs_path
        except Exception:
            pass
    return os.path.abspath("duckdb.db")


def _datetime_columns_to_naive(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all datetime columns to naive datetime64[ns]."""
    for col in df.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                ser = df[col]
                if getattr(ser.dtype, "tz", None) is not None:
                    ser = pd.to_datetime(ser, utc=True).dt.tz_localize(None)
                df[col] = ser.astype("datetime64[ns]")
            elif df[col].dtype == object and len(df) > 0:
                try:
                    ser = pd.to_datetime(df[col], utc=True, errors="coerce")
                    if pd.api.types.is_datetime64_any_dtype(ser) and ser.notna().any():
                        df[col] = ser.dt.tz_localize(None).astype("datetime64[ns]")
                except Exception:
                    pass
        except Exception:
            pass
    return df


def _datetime_columns_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to ISO string so ingestr/dlt never touches timezone (avoids tzdata in ingestr process on Windows)."""
    for col in df.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                s = pd.to_datetime(df[col], utc=False).dt.strftime("%Y-%m-%d %H:%M:%S")
                s = s.replace("NaT", None)  # strftime turns NaT into string "NaT"
                df[col] = s
        except Exception:
            pass
    return df


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    vars_json = os.environ.get("BRUIN_VARS", "{}")
    taxi_types = json.loads(vars_json).get("taxi_types", ["yellow"])

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dfs = []
    current = start
    while current <= end:
        year_month = current.strftime("%Y-%m")
        for taxi_type in taxi_types:
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year_month}.parquet"
            try:
                df = pd.read_parquet(url)
                df["taxi_type"] = taxi_type
                df = _datetime_columns_to_naive(df)
                dfs.append(df)
            except Exception:
                pass  # Skip missing or unreachable files
        current += relativedelta(months=1)

    if not dfs:
        return pd.DataFrame()
    result = pd.concat(dfs, ignore_index=True)
    result = _datetime_columns_to_naive(result)
    # Serialize datetimes as ISO strings so ingestr's PyArrow never calls assume_timezone (avoids tzdata error on Windows).
    result = _datetime_columns_to_string(result)
    return result