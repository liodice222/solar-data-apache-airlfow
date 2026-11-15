

from pathlib import Path
from airflow.sdk import dag, task
import logging 
import pandas as pd

#import operator for executing SQL 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

#add logging instead of print statements 
logger = logging.getLogger(__name__)

DATA_PATH = Path("/opt/airflow/data/raw")

REQUIRED_FILES = [
    "Plant_1_Generation_Data.csv",
    "Plant_1_Weather_Sensor_Data.csv",
    "Plant_2_Generation_Data.csv",
    "Plant_2_Weather_Sensor_Data.csv",
]


@dag(
    dag_id="solar_ingestion_dag",
    description="test data source files",
    schedule=None,
    catchup=False,
    tags=["solar", "ingestion"],
)
def solar_ingestion():

    @task
    def check_source_files():
        for filename in REQUIRED_FILES:
            path = DATA_PATH / filename

            if not path.is_file():
                raise FileNotFoundError(
                    f"Required file not found: {path}"
                )

            logger.info("Found: %s", path.name)

        logger.info("All four solar CSV files are accessible.")


    @task
    def validate_source_data():
        for filename in REQUIRED_FILES:
            path = DATA_PATH / filename
            df = pd.read_csv(path, dtype={"SOURCE_KEY": "string"})

            required_keys = ["PLANT_ID", "SOURCE_KEY", "DATE_TIME"]

            # Check required identifying columns
            missing_columns = set(required_keys) - set(df.columns)

            if missing_columns:
                raise ValueError(
                    f"{filename}: Missing columns {missing_columns}"
                )

            # Standardize timestamps
            df["DATE_TIME"] = pd.to_datetime(
                df["DATE_TIME"],
                format="mixed",
                dayfirst=True
            )

            # Check for null key values
            if df[required_keys].isna().any().any():
                raise ValueError(
                    f"{filename}: Null values found in key columns"
                )

            # Check composite key uniqueness
            duplicates = df.duplicated(subset=required_keys).sum()

            if duplicates > 0:
                raise ValueError(
                    f"{filename}: {duplicates} duplicate keys found"
                )

            logger.info(f"Validated {filename}: {len(df):,} rows")

    file_check = check_source_files()
    validation = validate_source_data()
    # only execute validation if file_check is successful
    file_check >> validation

    #add create_tables to exectute sql script from dags/sql
    create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id="solar_data",
    sql="sql/create_tables.sql",
    #execute five statements separately 
    split_statements=True,
)

    file_check >> validation >> create_tables



solar_ingestion()