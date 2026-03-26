# app.py
# Description: Streamlit demo interface — designed for a non-CS audience.

import streamlit as st
import pandas as pd
import pickle
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

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

# tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "🔍 find a trip",
    "📊 how well does it work?",
    "🗂️ browse the data",
    "💡 how does hashing work?"
])


# tab 1: trip lookup 
with tab1:
    st.subheader("find a specific taxi trip")

    st.markdown("""
    every taxi trip in our dataset is identified by two things:
    - *who picked you up* (the taxi company, called vendor id)
    - *exactly when they picked you up* (date and time)

    enter these two details below and we'll find the full trip record instantly!
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("*step 1 — pick the taxi company*")
        vendor_id = st.selectbox(
            "vendor id",
            options=[1, 2, 6],
            format_func=lambda x: f"vendor {x}",
            help="there are 3 taxi vendors in this dataset: 1, 2, and 6"
        )

    with col2:
        st.markdown("*step 2 — enter the pickup time*")
        pickup_input = st.text_input(
            "pickup date and time",
            value="2024-03-01 00:18:51",
            help="format: YYYY-MM-DD HH:MM:SS"
        )

    st.markdown("*step 3 — search!*")
    if st.button("find this trip", use_container_width=True):
        key = f"{vendor_id}_{pickup_input}"

        start_lookup = time.time()
        result = table.lookup(key)
        lookup_time = (time.time() - start_lookup) * 1000

        if result:
            st.success(f"trip found in {lookup_time:.4f} milliseconds out of 3.5 million records!")

            st.markdown("*here are the full trip details:*")

            friendly = {
                "tpep_dropoff_datetime": "dropoff time",
                "passenger_count":       "number of passengers",
                "trip_distance":         "trip distance (miles)",
                "PULocationID":          "pickup zone id",
                "DOLocationID":          "dropoff zone id",
                "payment_type":          "payment method (1=credit card, 2=cash)",
                "fare_amount":           "base fare ($)",
                "tip_amount":            "tip ($)",
                "total_amount":          "total charged ($)",
            }

            # use format_value() so numbers stay numbers, not timestamps
            display_rows = []
            for k, v in result.items():
                label = friendly.get(k, k)
                display_rows.append({"detail": label, "value": format_value(v)})

            result_df = pd.DataFrame(display_rows)
            st.dataframe(result_df, use_container_width=True, hide_index=True)

        else:
            st.warning("no trip found for this combination. try one of the sample keys below!")

    st.divider()
    st.markdown("*not sure what to search? try these real trips from the dataset:*")

    samples = [
        ("1", "2024-03-01 00:18:51", "short trip in manhattan, $8.60 fare"),
        ("2", "2024-03-01 00:09:22", "quick ride, 0.86 miles, $7.90 fare"),
        ("1", "2024-03-01 00:21:43", "2.4 mile trip, $12.10 fare"),
        ("2", "2024-03-01 00:50:42", "longer ride, 5 miles, $25.40 fare"),
    ]

    for vid, pickup, description in samples:
        st.code(f"vendor id: {vid}    pickup time: {pickup}    ({description})", language=None)


# tab 2: stats
with tab2:
    st.subheader("how well does our system perform?")

    st.markdown("""
    these numbers show how efficiently our hash table stores and retrieves 3.5 million trips.
    think of it like measuring how well organized our filing cabinet is!
    """)

    stats = table.get_stats()

    col1, col2, col3 = st.columns(3)
    col1.metric("total trips indexed", f"{stats['total_items']:,}")
    col2.metric("lookup speed", "O(1) average")
    col3.metric("total collisions", f"{stats['collision_count']:,}")

    col4, col5, col6 = st.columns(3)
    col4.metric("number of slots (buckets)", f"{stats['table_size']:,}")
    col5.metric("load factor", stats['load_factor'])
    col6.metric("avg chain length", stats['avg_chain_len'])

    st.divider()
    st.markdown("""
    *what is a collision?*

    sometimes two different trips get assigned the same slot in our index.
    when this happens, we simply chain them together — like putting two files in the same folder with a tab separator.
    this is called *chaining* and it's how our system handles real-world messy data gracefully.

    *what is load factor?*

    load factor = trips stored / number of slots. a value near 1.0 means our slots are well utilized —
    not too empty (wasteful) and not too full (slow).
    """)


# tab 3: browse data 
with tab3:
    st.subheader("browse the dataset")

    st.markdown("""
    here are the first 20 trips loaded into our system.
    each row is one real taxi ride that happened in new york city in march 2024.
    """)

    sample_data = []
    for key, value in records[:20]:

        # use format_value() on every field so no timestamp bleed
        sample_data.append({
            "trip key":           key,
            "dropoff time":       format_value(value.get("tpep_dropoff_datetime")),
            "passengers":         format_value(value.get("passenger_count")),
            "distance (miles)":   format_value(value.get("trip_distance")),
            "fare ($)":           format_value(value.get("fare_amount")),
            "tip ($)":            format_value(value.get("tip_amount")),
            "total ($)":          format_value(value.get("total_amount")),
            "pickup zone":        format_value(value.get("PULocationID")),
            "dropoff zone":       format_value(value.get("DOLocationID")),
        })

    st.dataframe(pd.DataFrame(sample_data), use_container_width=True, hide_index=True)

    st.markdown(f"""
    *dataset summary:*
    - source: nyc taxi & limousine commission (tlc)
    - month: march 2024
    - total trips: {len(records):,}
    - known data issues we fixed: 87,168 bad distances, 58,464 negative fares, 426,190 missing passenger counts
    """)


# tab 4: how hashing works 
with tab4:
    st.subheader("how does hashing work? (no cs background needed!)")

    st.markdown("""
    ### the library analogy

    imagine a library with 4 million books but no catalog system.
    to find one book you'd have to check every shelf — that could take hours!

    a *hash function* is like a smart librarian who looks at the book title
    and instantly tells you: "that book is on shelf 2,847, position 3."

    ---

    ### how we build the key

    every taxi trip gets a unique identifier called a *key*:

    
    key = vendor id + pickup datetime
    example: "2_2024-03-01 00:09:22"
    

    this key is run through our hash function which converts it to a number (a shelf number),
    and the trip gets stored there.

    ---

    ### what happens when two trips get the same slot? (collision)

    just like two books could be assigned the same shelf,
    two trips might get the same slot. we handle this by *chaining* —
    linking them together so none get lost.

    ---

    ### why is this faster than just searching?

    | method | time to find 1 trip in 3.5 million |
    |---|---|
    | searching one by one | up to 3,500,000 checks |
    | using our hash table | ~1-2 checks on average |

    that's the power of hashing!

    ---

    ### real-world noise we handled

    the nyc taxi data is messy — just like real-world data always is.

    | problem | how many rows | what we did |
    |---|---|---|
    | negative trip distances | 87,168 | marked as unknown |
    | negative fare amounts | 58,464 | marked as unknown |
    | missing passenger counts | 426,190 | filled with 0 |
    | negative total amounts | 44,311 | marked as unknown |
    """)
