# preprocessing.py
# Authors: Yaxita Amin & Helen Li
# MSML606 HW3 Extra Credit — NYC Taxi Trip Hash Indexer
# Description: Data loading, cleaning, and key normalization for NYC TLC taxi data.

# ─────────────────────────────────────────────
# SPLIT GUIDE:
#   Helen   → load_data, clean_data
#   Yaxita  → make_key, preprocess
# ─────────────────────────────────────────────

import pandas as pd
import logging

# Set up logging so noisy/bad records are tracked (not silently dropped)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def load_data(filepath):
    """
    Load NYC TLC taxi data from a CSV or Parquet file into a DataFrame.

    Supports both .csv and .parquet formats automatically based on file extension.

    Args:
        filepath (str): Path to the dataset file.

    Returns:
        pd.DataFrame: Raw, unprocessed dataframe.

    Raises:
        ValueError: If file extension is not .csv or .parquet.

    TODO (Helen):
        - Check file extension (.parquet vs .csv)
        - Use pd.read_parquet() or pd.read_csv() accordingly
        - Log how many rows were loaded
        - Raise ValueError for unsupported formats
    """
    if filepath.endswith(".parquet"):
        df = pd.read_parquet(filepath)
    elif filepath.endswith(".csv"):
        df = pd.read_csv(filepath)
    else:
        raise ValueError("Unsupported file format. Use .csv or .parquet")

    logger.info(f"Loaded {len(df):,} rows from {filepath}")
    return df


def clean_data(df):
    """
    Clean raw NYC taxi dataframe by handling noise and missing values.

    Noise to handle:
        - Missing VendorID or tpep_pickup_datetime → drop rows (can't build key)
        - Negative or zero trip_distance           → set to None
        - Missing passenger_count                  → fill with 0
        - Negative fare_amount or total_amount     → set to None
        - Duplicate rows                           → drop

    Args:
        df (pd.DataFrame): Raw dataframe loaded from TLC dataset.

    Returns:
        pd.DataFrame: Cleaned dataframe with consistent, usable fields.

    TODO (Helen):
        - dropna on ['VendorID', 'tpep_pickup_datetime'] — log how many dropped
        - Parse tpep_pickup_datetime with pd.to_datetime(errors='coerce')
        - Standardize datetime to string format "YYYY-MM-DD HH:MM:SS"
        - Fix negative/zero trip_distance → None, log count
        - Fix negative fare_amount / total_amount → None, log count
        - Fill missing passenger_count with 0
        - Drop duplicate rows, log count
        - Return reset_index(drop=True) cleaned df
    """
    original_len = len(df)

    # drop rows missing key fields
    before = len(df)
    df = df.dropna(subset=["VendorID", "tpep_pickup_datetime"])
    dropped = before - len(df)
    logger.info(f"Dropped {dropped:,} rows missing VendorID or pickup datetime")

    # parse datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )

    # drop rows where datetime parsing failed
    before = len(df)
    df = df.dropna(subset=["tpep_pickup_datetime"])
    dropped = before - len(df)
    logger.info(f"Dropped {dropped:,} rows with invalid pickup datetime")

    # standardize datetime format
    df["tpep_pickup_datetime"] = df["tpep_pickup_datetime"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # fix trip_distance (<= 0 -> None)
    if "trip_distance" in df.columns:
        invalid_dist = (df["trip_distance"] <= 0).sum()
        df.loc[df["trip_distance"] <= 0, "trip_distance"] = None
        logger.info(f"Fixed {invalid_dist:,} invalid trip_distance values")

    # fix fare_amount and total_amount (negative -> None)
    for col in ["fare_amount", "total_amount"]:
        if col in df.columns:
            invalid_vals = (df[col] < 0).sum()
            df.loc[df[col] < 0, col] = None
            logger.info(f"Fixed {invalid_vals:,} negative values in {col}")

    # fill missing passenger_count with 0
    if "passenger_count" in df.columns:
        missing_passengers = df["passenger_count"].isna().sum()
        df["passenger_count"] = df["passenger_count"].fillna(0)
        logger.info(f"Filled {missing_passengers:,} missing passenger_count values")

    # drop duplicates
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    logger.info(f"Dropped {dropped:,} duplicate rows")

    logger.info(f"Cleaned dataset: {original_len:,} → {len(df):,} rows")

    return df.reset_index(drop=True)


def make_key(row):
    """
    Create a composite string key from VendorID and tpep_pickup_datetime.

    Format : "{VendorID}_{tpep_pickup_datetime}"
    Example: "2_2024-03-01 14:35:21"

    Args:
        row (pd.Series): A single row from the cleaned dataframe.

    Returns:
        str or None: Composite key string, or None if fields are invalid.

    TODO (Yaxita):
        - Extract VendorID as int → convert to str
        - Extract tpep_pickup_datetime as str
        - Return None if either is missing/invalid
        - Return f"{vendor}_{pickup}"
    """
    try:
        vendor = str(int(row["VendorID"]))
        pickup = str(row["tpep_pickup_datetime"])
 
        if not vendor or not pickup or pickup in ("nan", "NaT", "None"):
            return None
 
        return f"{vendor}_{pickup}"
 
    except (ValueError, TypeError):
        return None


def preprocess(filepath):
    """
    Full preprocessing pipeline: load → clean → build (key, value) pairs.

    Each value is a dict of trip metadata fields useful for lookup results.

    Args:
        filepath (str): Path to the TLC dataset file (.csv or .parquet).

    Returns:
        list of (str, dict): List of (composite_key, metadata_dict) tuples
                             ready to be inserted into the hash table.

    TODO (Yaxita):
        - Call load_data(filepath)
        - Call clean_data(df)
        - Loop over rows, call make_key(row) for each
        - Skip rows where make_key returns None (log count)
        - Build value dict with fields:
            trip_distance, fare_amount, tip_amount, total_amount,
            passenger_count, PULocationID, DOLocationID,
            payment_type, tpep_dropoff_datetime
        - Append (key, value) to records list
        - Log final count and return records
    """
    df = load_data(filepath)
    df = clean_data(df)
 
    records = []
    skipped = 0
 
    for _, row in df.iterrows():
        key = make_key(row)
 
        if key is None:
            skipped += 1
            continue
 
        value = {
            "tpep_dropoff_datetime": row.get("tpep_dropoff_datetime"),
            "passenger_count":       int(row.get("passenger_count", 0)),
            "trip_distance":         row.get("trip_distance"),
            "PULocationID":          row.get("PULocationID"),
            "DOLocationID":          row.get("DOLocationID"),
            "payment_type":          row.get("payment_type"),
            "fare_amount":           row.get("fare_amount"),
            "tip_amount":            row.get("tip_amount"),
            "total_amount":          row.get("total_amount"),
        }
 
        records.append((key, value))
 
    logger.info(f"Preprocessed: {len(records):,} valid records | {skipped:,} skipped")
    return records