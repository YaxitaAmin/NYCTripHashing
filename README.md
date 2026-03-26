# NYC Taxi Trip Hash Indexer

*MSML606 HW3 Extra Credit — Spring 2026*  
Authors:  Helen Li  & Yaxita Amin 
UMD

---

## What This Project Does

This application indexes 3.5 million real NYC taxi trip records using a custom hash table with chaining for collision resolution. Given a vendor ID and pickup timestamp, the app retrieves the full trip record in O(1) average time — instead of scanning millions of rows one by one.

The project demonstrates how core data structures perform on real-world, noisy data at scale.

---

## Dataset

- *Source:* NYC Taxi & Limousine Commission (TLC) — [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- *File:* yellow_tripdata_2024-03.parquet (March 2024)
- *Scale:* 3,582,628 trip records
- *Format:* Parquet with multiple metadata fields per trip (fare, distance, timestamps, zone IDs, etc.)

---

## Project Structure


NYCTripHashing/
│
├── src/
│   ├── hash_table.py        # core hash table + chaining + stats
│   └── preprocess.py        # data loading, cleaning, key normalization
│
├── data/
│   └── yellow_tripdata_2024-03.parquet   # dataset (not included in repo)
│
├── app.py                   # Streamlit UI 
├── main.py                  # command-line demo and sample queries
├── proposal/
│   └── proposal.pdf         # original submitted proposal
├── requirements.txt
└── README.md


---

## How to Run

### 1. Clone the repository

bash
git clone https://github.com/<your-username>/NYCTripHashing.git
cd NYCTripHashing


### 2. Set up a virtual environment

bash
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux


### 3. Install dependencies

bash
pip install -r requirements.txt


### 4. Download the dataset

Download yellow_tripdata_2024-03.parquet from the [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place it in the data/ folder.

### 5. Run the command-line demo

bash
python main.py


### 6. Run the Streamlit app

bash
streamlit run app.py


> *Note:* The first run builds and saves the hash index (~90 seconds). Every subsequent run loads from a cached file and starts in 2-3 seconds.

---

## How It Works

### The Key

Every trip is identified by a composite key:


key = "{VendorID}_{tpep_pickup_datetime}"
example: "2_2024-03-01 00:09:22"


### The Hash Function

A polynomial rolling hash maps each key to a bucket index:


hash_val = sum(ord(char) * 31^i) % table_size


Table size is set to *4,000,037* (a prime number close to the dataset size) to minimize clustering.

### Collision Resolution

When two keys map to the same bucket, they are chained together using a singly linked list. Each bucket is the head of a chain; new nodes are prepended.

### Preprocessing

The raw dataset contains real-world noise that must be handled before indexing:

| Issue | Rows affected | Action taken |
|---|---|---|
| Zero or negative trip distance | 87,168 | Set to None |
| Negative fare amount | 58,464 | Set to None |
| Negative total amount | 44,311 | Set to None |
| Missing passenger count | 426,190 | Filled with 0 |
| Duplicate rows | 0 | Dropped |
| Missing VendorID or pickup time | 0 | Dropped (key cannot be built) |

---

## Performance Results

After indexing 3,582,628 records:

| Metric | Value |
|---|---|
| Total records indexed (unique keys) | 2,216,850 |
| Table size (buckets) | 4,000,037 |
| Load factor | 0.5542 |
| Collision count | 657,447 |
| Max chain length | 7 |
| Avg chain length | 1.42 |

The load factor of ~0.55 and average chain length of 1.42 confirm near-constant time lookups even at 3.5 million records.

The difference between 3.58M preprocessed records and 2.22M indexed records reflects duplicate composite keys in the raw data (two trips with the same vendor and exact pickup time). The hash table handles these by updating the existing entry in place.

---
## AI Usage Disclosure

We used Claude (Anthropic's AI assistant) to help debug our code, structure 
function skeletons. All core algorithm 
implementation, data handling, and analysis are our own work.

---

## Requirements


streamlit
pandas
pyarrow


Install with:
bash
pip install -r requirements.txt