from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "raw"

# Define the datasets
files = {
    "plant_1_generation": "Plant_1_Generation_Data.csv",
    "plant_1_weather": "Plant_1_Weather_Sensor_Data.csv",
    "plant_2_generation": "Plant_2_Generation_Data.csv",
    "plant_2_weather": "Plant_2_Weather_Sensor_Data.csv"
}

# load csv to dataframes
dataframes = {}

for name, filename in files.items():
    file_path = DATA_PATH / filename
    dataframes[name] = pd.read_csv(file_path)

# inspect each df
for name, df in dataframes.items():
    print(f"\n--- {name} ---")
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print("\nColumn types:")
    print(df.dtypes)
    print(df.head())


#exploratory check 

# print("\n--- exploratory check ---")
for name, df in dataframes.items():
    print(f"\n--- {name} nulls---")
    print(df.isnull().sum())
    print(f"\n--- {name} duplicates---")
    print(df.duplicated().sum())

#understand time series data
for name, df in dataframes.items():
    df["DATE_TIME"] = pd.to_datetime(
        df["DATE_TIME"],
        dayfirst=True
    )

    print(f"\n--- {name} ---")
    print("Start:", df["DATE_TIME"].min())
    print("End:", df["DATE_TIME"].max())
    print("Unique timestamps:", df["DATE_TIME"].nunique())

#find how many timestamps per inverter
df = dataframes["plant_1_weather"]
print("total readings per ts")
readings_per_timestamp = (
    df.groupby("DATE_TIME")
    #total number of rows at this timestamp, doesnt check for uniqueness
    .size()
)

print(readings_per_timestamp.head(10))

#find unique inverters
print("Unique inverters:", df["SOURCE_KEY"].nunique())
print("unique inverters per ts")
inverters_per_timestamp = (
    df.groupby("DATE_TIME")["SOURCE_KEY"]
    #check actual unique inverters at timestamp
    .nunique()
)

print(inverters_per_timestamp.head(10))

#check duplicates on combination of timestamp and source key 
duplicate_readings = df.duplicated(
    subset=["DATE_TIME", "SOURCE_KEY"]
)

print("Duplicate inverter/timestamp records:", duplicate_readings.sum())

#verify count of inverters for plant generation data
for name in ["plant_1_generation", "plant_2_generation"]:
    df = dataframes[name]

    print(f"\n{name}")
    print("Number of inverters:", df["SOURCE_KEY"].nunique())
    print("Inverter IDs:", df["SOURCE_KEY"].unique())

    #verify count of inverters for weather sensor data
for name in ["plant_1_weather", "plant_2_weather"]:
    df = dataframes[name]

    print(f"\n{name}")
    print("Number of inverters:", df["SOURCE_KEY"].nunique())
    print("Inverter IDs:", df["SOURCE_KEY"].unique())