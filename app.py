# app.py
# Authors: Yaxita Amin & Helen Li
# MSML606 HW3 Extra Credit — NYC Taxi Trip Hash Indexer
# Description: Streamlit demo interface for the hash table indexer.

import streamlit as st
import pandas as pd
import time
import sys
import os

# allow imports from src/
sys.path.insert(0, os.path.dirname(__file__))

from src.hash_table import HashTable
from src.preprocess import preprocess


# page config
st.set_page_config(
    page_title="nyc taxi hash indexer",
    page_icon="🚕",
    layout="wide"
)

st.title("nyc taxi trip hash indexer")
st.caption("msml606 hw3 extra credit  |  yaxita amin & helen li  |  dataset: nyc tlc yellow taxi march 2024")


# load and cache the hash table
@st.cache_resource(show_spinner=False)
def load_index(filepath):
    """Build and cache the hash table so it only loads once."""
    records = preprocess(filepath)
    table = HashTable(size=10007)
    for key, value in records:
        table.insert(key, value)
    return table, records

FILEPATH = "data/yellow_tripdata_2024-03.parquet"

if not os.path.exists(FILEPATH):
    st.error(f"dataset not found at {FILEPATH}. please download it from the nyc tlc website and place it in the data/ folder.")
    st.stop()

with st.spinner("loading and indexing 3.5 million records into hash table..."):
    start = time.time()
    table, records = load_index(FILEPATH)
    elapsed = time.time() - start

st.success(f"indexed {len(records):,} records in {elapsed:.2f} seconds")

st.divider()


# TABS
tab1, tab2, tab3, tab4 = st.tabs(["trip lookup", "hash table stats", "sample records", "bucket distribution"])


# tab 1: trip lookup 
with tab1:
    st.subheader("look up a trip")
    st.write("enter a vendor id and pickup datetime to retrieve trip details directly from the hash table.")

    col1, col2 = st.columns(2)

    with col1:
        vendor_id = st.selectbox("vendor id", options=[1, 2, 6], help="vendor ids present in march 2024 dataset")

    with col2:
        pickup_input = st.text_input(
            "pickup datetime",
            value="2024-03-01 00:18:51",
            help="format: YYYY-MM-DD HH:MM:SS"
        )

    if st.button("search"):
        key = f"{vendor_id}_{pickup_input}"
        st.write(f"searching for key: {key}")

        result = table.lookup(key)

        if result:
            st.success("trip found")
            result_df = pd.DataFrame(result.items(), columns=["field", "value"])
            st.dataframe(result_df, use_container_width=True, hide_index=True)
        else:
            st.warning("no trip found for this key. try a different vendor id or datetime.")

    st.divider()

    # quick sample keys to try
    st.write("*sample keys to try* (real records from dataset):")
    sample_keys = {
        "vendor 1 — row 0": ("1", "2024-03-01 00:18:51"),
        "vendor 2 — row 2": ("2", "2024-03-01 00:09:22"),
        "vendor 1 — row 9": ("1", "2024-03-01 00:21:43"),
        "vendor 2 — row 5": ("2", "2024-03-01 00:50:42"),
    }
    for label, (vid, pdt) in sample_keys.items():
        st.code(f"vendor id: {vid}   pickup: {pdt}   →   key: {vid}_{pdt}", language=None)


# tab 2: hash table stats 
with tab2:
    st.subheader("hash table performance statistics")
    st.write("these stats show how well the hash function distributes 3.5m records across 10,007 buckets.")

    stats = table.get_stats()

    col1, col2, col3 = st.columns(3)
    col1.metric("total records", f"{stats['total_items']:,}")
    col2.metric("load factor", stats['load_factor'])
    col3.metric("collisions", f"{stats['collision_count']:,}")

    col4, col5, col6 = st.columns(3)
    col4.metric("table size (buckets)", f"{stats['table_size']:,}")
    col5.metric("empty buckets", f"{stats['empty_buckets']:,}")
    col6.metric("max chain length", stats['max_chain_len'])

    st.metric("avg chain length", stats['avg_chain_len'])

    st.divider()
    st.write("*what does load factor mean?*")
    st.write("load factor = total records / number of buckets. a load factor above 1.0 means some buckets have chains (collisions). our table has 10,007 buckets holding ~3.5m records, so chains are expected and handled gracefully via chaining.")


# tab 3: sample records
with tab3:
    st.subheader("sample records from dataset")
    st.write("these are the first 20 records loaded into the hash table.")

    sample_data = []
    for key, value in records[:20]:
        row = {"key": key}
        row.update(value)
        sample_data.append(row)

    sample_df = pd.DataFrame(sample_data)
    st.dataframe(sample_df, use_container_width=True, hide_index=True)

    st.divider()
    st.write(f"*total records indexed:* {len(records):,}")
    st.write("*key format:* VendorID_tpep_pickup_datetime  e.g. 2_2024-03-01 00:09:22")


# tab 4: bucket distribution 
with tab4:
    st.subheader("bucket chain length distribution")
    st.write("this chart shows how many buckets have chains of each length — a measure of how evenly the hash function distributes records.")

    # compute chain length distribution
    chain_dist = {}
    for bucket in table.buckets:
        length = 0
        current = bucket
        while current:
            length += 1
            current = current.next
        chain_dist[length] = chain_dist.get(length, 0) + 1

    dist_df = pd.DataFrame(
        sorted(chain_dist.items()),
        columns=["chain length", "number of buckets"]
    )

    st.bar_chart(dist_df.set_index("chain length"), use_container_width=True)
    st.dataframe(dist_df, use_container_width=True, hide_index=True)
    st.write("chain length 0 = empty buckets. length 1 = no collision. length 2+ = collisions resolved via chaining.")