# app.py
# Description: Streamlit demo interface — designed for a non-CS audience.

import streamlit as st
import pandas as pd
import pickle
import time
import sys
import os

sys.path.insert(0, os.path.dirname(_file_))

from src.hash_table import HashTable
from src.preprocess import preprocess


# page config
st.set_page_config(
    page_title="nyc taxi trip finder",
    page_icon="🚕",
    layout="wide"
)

st.title("🚕 nyc taxi trip finder")
st.caption("built by Helen Li and Yaxita Amin   |   UMD")

st.markdown("""
### what does this app do?
imagine you have a filing cabinet with *3.5 million taxi trip records* from new york city.

finding one specific trip by flipping through every page would take forever.

this app uses a technique called *hashing* — think of it like a super smart index at the back of a book.
instead of searching page by page, we jump directly to the right page in milliseconds.

---
""")

# load and cache
FILEPATH    = "data/yellow_tripdata_2024-03.parquet"
PICKLE_PATH = "data/taxi_index.pkl"

if not os.path.exists(FILEPATH):
    st.error("dataset not found. please place yellow_tripdata_2024-03.parquet in the data/ folder.")
    st.stop()

@st.cache_resource(show_spinner=False)
def load_index(filepath):
    if os.path.exists(PICKLE_PATH):
        with open(PICKLE_PATH, "rb") as f:
            table, records = pickle.load(f)
        return table, records

    # 🔨 First time only — preprocess and build the hash table
    records = preprocess(filepath)
    table = HashTable(size=4000037)
    for key, value in records:
        table.insert(key, value)


    with open(PICKLE_PATH, "wb") as f:
        pickle.dump((table, records), f)

    return table, records

# Show different spinner message depending on whether pickle exists
spinner_msg = (
    "loading saved index — this takes just a few seconds..."
    if os.path.exists(PICKLE_PATH)
    else "first time setup — building index from 3.5M records, ~90 seconds, only happens once..."
)

with st.spinner(spinner_msg):
    start = time.time()
    table, records = load_index(FILEPATH)
    elapsed = time.time() - start

st.success(f"ready! indexed {len(records):,} taxi trips in {elapsed:.1f} seconds.")

st.divider()

# helper: safely format a value for display
# This is the KEY fix — converts any value to
# its correct readable string, preventing
# numeric fields from being shown as timestamps.
def format_value(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "N/A"
    # If it's already a plain number, just return it — do NOT touch pd.to_datetime
    if isinstance(v, (int, float)):
        return round(v, 2) if isinstance(v, float) else v
    # Only format as datetime if it's actually a datetime type
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


