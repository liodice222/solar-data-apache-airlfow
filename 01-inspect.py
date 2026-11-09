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
# for name, df in dataframes.items():
#     print(f"\n--- {name} ---")
#     print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
#     print("\nColumn types:")
#     print(df.dtypes)
#     print(df.head())


#exploratory check 

print("\n--- exploratory check ---")
for name, df in dataframes.items():
    print(f"\n--- {name} nulls---")
    print(df.isnull().sum())
    print(f"\n--- {name} duplicates---")
    print(df.duplicated().sum())

#understand time series data
