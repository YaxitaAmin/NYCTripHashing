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


