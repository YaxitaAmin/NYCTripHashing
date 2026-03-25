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
from src.preprocessing import preprocess


def build_index(filepath):
    """
    Build hash table from the NYC taxi dataset.

    Calls full preprocessing pipeline and inserts all valid records.
    Measures and prints total build time.

    Args:
        filepath (str): Path to .parquet or .csv dataset file

    Returns:
        HashTable: Fully populated hash table
    """
    print("\nloading and indexing dataset, please wait...")
    start = time.time()

    records = preprocess(filepath)
    table = HashTable(size=10007)

    for key, value in records:
        table.insert(key, value)

    elapsed = time.time() - start
    print(f"done. indexed {len(records):,} records in {elapsed:.2f} seconds")
    return table


def query(table, vendor_id, pickup_datetime):
    """
    Look up a NYC taxi trip by vendor ID and pickup datetime.

    Args:
        table           (HashTable) : Populated hash table
        vendor_id       (int/str)   : VendorID — 1, 2, or 6 in this dataset
        pickup_datetime (str)       : Format "YYYY-MM-DD HH:MM:SS"

    Returns:
        dict or None: Trip metadata if found, None otherwise
    """
    key = f"{vendor_id}_{pickup_datetime}"
    print(f"\nsearching for key: {key}")

    result = table.lookup(key)

    if result:
        print("trip found\n")
        for field, val in result.items():
            print(f"  {field:<26} {val}")
    else:
        print("no trip found for this key.")

    return result


def print_stats(table):
    """
    Print hash table performance statistics.

    Args:
        table (HashTable): Populated hash table
    """
    stats = table.get_stats()

    print("\nhash table statistics\n")
    print(f"  total records indexed  : {stats['total_items']:,}")
    print(f"  table size (buckets)   : {stats['table_size']:,}")
    print(f"  load factor            : {stats['load_factor']}")
    print(f"  collision count        : {stats['collision_count']:,}")
    print(f"  empty buckets          : {stats['empty_buckets']:,}")
    print(f"  max chain length       : {stats['max_chain_len']}")
    print(f"  avg chain length       : {stats['avg_chain_len']}")


def run_demo(filepath):
    """
    Full demo: build index → run sample queries → print stats.

    Sample keys are real records from yellow_tripdata_2024-03.parquet:
        Row 0: VendorID=1, pickup=2024-03-01 00:18:51
        Row 2: VendorID=2, pickup=2024-03-01 00:09:22
        Row 9: VendorID=1, pickup=2024-03-01 00:21:43

    Args:
        filepath (str): Path to dataset file
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