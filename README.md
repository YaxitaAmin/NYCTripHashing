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

