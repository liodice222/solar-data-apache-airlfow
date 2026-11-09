from importlib.metadata import files
from pathlib import Path
from itertools import combinations

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "raw"

# Each entry maps a dataset name to its file and source role.
files = {
    "plant_1_generation": ("Plant_1_Generation_Data.csv", "generation"),
    "plant_1_weather": ("Plant_1_Weather_Sensor_Data.csv", "weather"),
    "plant_2_generation": ("Plant_2_Generation_Data.csv", "generation"),
    "plant_2_weather": ("Plant_2_Weather_Sensor_Data.csv", "weather"),
}

key_columns = ["PLANT_ID", "SOURCE_KEY", "DATE_TIME"]
    

#load each dataset and inspect keys 
for name, (filename, kind) in files.items():
    df = pd.read_csv(DATA_PATH / filename)
    print(f"\n--- {name} ---")
    for size in range(1, len(key_columns) + 1):
        for columns in combinations(key_columns, size):

            duplicate_count = df.duplicated(
                subset=list(columns)
            ).sum()

            print(
                f"{' + '.join(columns)}: "
                f"{duplicate_count} duplicates"
            )