# main.py
# Authors: Yaxita Amin & Helen Li
# MSML606 HW3 Extra Credit — NYC Taxi Trip Hash Indexer
# Description: Entry point — builds the hash index and runs the query interface.

# ─────────────────────────────────────────────
# SPLIT GUIDE:
#   Helen   → build_index, query
#   Yaxita  → print_stats, run_demo
# ─────────────────────────────────────────────

import sys
import time
from src.hash_table import HashTable
from src.preprocess import preprocess


def build_index(filepath):
    """
    Build the hash table index from the NYC taxi dataset.

    Calls the full preprocessing pipeline and inserts all valid records
    into a HashTable. Measures and reports total build time.

    Args:
        filepath (str): Path to the .csv or .parquet dataset file.

    Returns:
        HashTable: Fully populated hash table ready for queries.

    TODO (Helen):
        - Print a loading message
        - Record start time with time.time()
        - Call preprocess(filepath) to get list of (key, value) pairs
        - Create HashTable(size=10007)
        - Loop and call table.insert(key, value) for each record
        - Print elapsed time and total records indexed
        - Return the table
    """
    print("\nloading and indexing dataset, please wait...")
    start = time.time()

    try:
        records = preprocess(filepath)
    except Exception as e:
        print(f"error during preprocessing: {e}")
        raise

    table = HashTable(size=4000037)

    for key, value in records:
        table.insert(key, value)

    elapsed = time.time() - start
    print(f"done. indexed {len(records):,} records in {elapsed:.2f} seconds")

    return table


def query(table, vendor_id, pickup_datetime):
    """
    Query the hash table for a specific NYC taxi trip.

    Constructs the composite key from vendor_id + pickup_datetime,
    performs an O(1) average-case lookup, and prints the result.

    Args:
        table           (HashTable) : The populated hash table.
        vendor_id       (int/str)   : The taxi vendor ID (e.g., 1 or 2).
        pickup_datetime (str)       : Pickup datetime, format: "YYYY-MM-DD HH:MM:SS"

    Returns:
        dict or None: Trip metadata if found, None otherwise.

    TODO (Helen):
        - Build key as f"{vendor_id}_{pickup_datetime}"
        - Call table.lookup(key)
        - Print the key being searched
        - If result found → print each field in result dict
        - If not found   → print not found message
        - Return result
    """
    key = f"{vendor_id}_{pickup_datetime}"
    print(f"\nsearching for key: {key}")

    result = table.lookup(key)

    if result is not None:
        print("trip found\n")
        for field, val in result.items():
            print(f"  {field:<26} {val}")
    else:
        print("no trip found for this key.")

    return result


def print_stats(table):
    """
    Print hash table performance statistics in a readable format.

    Args:
        table (HashTable): The populated hash table.

    TODO (Yaxita):
        - Call table.get_stats() to get the stats dict
        - Print each stat with a label:
            total_items, table_size, load_factor,
            collision_count, empty_buckets,
            max_chain_len, avg_chain_len
    """
    stats = table.get_stats()
 
    print("\nhash table statistics\n")
    print(f"total records indexed: {stats['total_items']:,}")
    print(f"table size (buckets): {stats['table_size']:,}")
    print(f"load factor: {stats['load_factor']}")
    print(f"collision count: {stats['collision_count']:,}")
    print(f"  empty buckets: {stats['empty_buckets']:,}")
    print(f"max chain length {stats['max_chain_len']}")
    print(f"avg chain length: {stats['avg_chain_len']}")


def run_demo(filepath):
    """
    Full demo: build the index, run sample queries, and print stats.

    This is the main showcase of the application — it demonstrates:
        1. Indexing 1M+ real-world noisy records
        2. O(1) average-case lookups by composite key
        3. Hash table performance statistics

    Args:
        filepath (str): Path to the TLC dataset file.

    TODO (Yaxita):
        - Print a header banner with project title + authors
        - Call build_index(filepath)
        - Run at least 3 sample queries using query()
          (adjust keys to match records in the actual dataset!)
        - Call print_stats(table)
        - Print a closing message
    """
    print("nyc taxi trip hash indexer")
    print("authors : yaxita amin & helen li")
    print("course  : msml606 hw3 extra credit")
    print("dataset : nyc tlc yellow taxi march 2024")
 
    # step 1: build the index
    table = build_index(filepath)
 
    # step 2: sample queries using real keys from the dataset
    print("\nsample queries\n")
 
    # row 0: VendorID=1, pickup=2024-03-01 00:18:51
    query(table, 1, "2024-03-01 00:18:51")
 
    # row 2: VendorID=2, pickup=2024-03-01 00:09:22
    query(table, 2, "2024-03-01 00:09:22")
 
    # row 9: VendorID=1, pickup=2024-03-01 00:21:43
    query(table, 1, "2024-03-01 00:21:43")
 
    # one that won't exist — shows graceful not-found handling
    query(table, 6, "2024-03-01 00:00:00")
 
    # step 3: stats
    print_stats(table)
 
    print("\ndemo complete.")


if __name__ == "__main__":
    default_path = "data/yellow_tripdata_2024-03.parquet"
    filepath = sys.argv[1] if len(sys.argv) > 1 else default_path
    run_demo(filepath)
