""" objective for 01-ingest.py:
- make sure airflow and inspect all source files 
- log which files are found and which are missing (by raising exception) 
- succeed only when all four are found
"""

from pathlib import Path
from airflow.sdk import dag, task

DATA_PATH = Path("/opt/airflow/data/raw")

REQUIRED_FILES = [
    "Plant_1_Generation_Data.csv",
    "Plant_1_Weather_Sensor_Data.csv",
    "Plant_2_Generation_Data.csv",
    "Plant_2_Weather_Sensor_Data.csv",
]

#define dag workflow 
@dag(
    # UI display name
    dag_id="solar_ingestion",
    #change after manual testing
    schedule=None,
    catchup=False,
    #for organization and filtering in UI
    tags=["solar", "ingestion"],
)
def solar_ingestion():

#define executable task 
    @task
    def check_source_files():
        for filename in REQUIRED_FILES:
            path = DATA_PATH / filename

            if not path.is_file():
                raise FileNotFoundError(
                    f"Required file not found: {path}"
                )

            print(f"Found: {path.name}")

        print("All source files are available.")

    check_source_files()


solar_ingestion()