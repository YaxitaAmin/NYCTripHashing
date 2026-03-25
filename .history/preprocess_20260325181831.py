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
    pass


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
    pass


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
    