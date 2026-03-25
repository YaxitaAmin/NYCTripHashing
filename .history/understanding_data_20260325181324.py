import pandas as pd

df = pd.read_parquet("data/yellow_tripdata_2024-03.parquet")

print("Shape:", df.shape)
print("\nColumns:", df.columns.tolist())
print("\nSample rows:")
print(df[["VendorID", "tpep_pickup_datetime", "trip_distance", 
          "fare_amount", "passenger_count"]].head(10).to_string())
print("\nNull counts:\n", df.isnull().sum())
print("\nVendorID unique values:", df["VendorID"].unique())