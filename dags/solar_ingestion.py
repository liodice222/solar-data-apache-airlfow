

from pathlib import Path
from airflow.sdk import dag, task
import logging 
from contextlib import closing
from datetime import date, timedelta
import pandas as pd

#import operator for executing SQL 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#allow for python to interact with postgres db
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

    @task
    def load_plants():
        #create empty python set to store unique plant ids from csv files
        plant_ids = set()

        for filename in REQUIRED_FILES:
            path = DATA_PATH / filename
            #find plant ids from csv files and drop duplicate plant ids and null values
            df = pd.read_csv(path, usecols=["PLANT_ID"])
            plant_ids.update(df["PLANT_ID"].dropna().unique())

        #convert ids to integer and sort them in ascending order
        rows = [(int(plant_id),) for plant_id in sorted(plant_ids)]

        #create poostgres hook to allow python to interact with postgres db
        hook = PostgresHook(postgres_conn_id="solar_data")

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO plant (plant_id)
                    VALUES (%s)
                    ON CONFLICT (plant_id) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()

        logger.info("Processed %s unique plants", len(rows))

    file_check = check_source_files()
    validation = validate_source_data()

    #load inverters to inverter table in postgres db
    @task
    def load_inverters():
        filenames = [
            "Plant_1_Generation_Data.csv",
            "Plant_2_Generation_Data.csv",
        ]

        equipment = set()

        for filename in filenames:
            df = pd.read_csv(
                DATA_PATH / filename,
                usecols=["PLANT_ID", "SOURCE_KEY"]
            )

            for row in df.itertuples(index=False):
                equipment.add((int(row.PLANT_ID), str(row.SOURCE_KEY)))

        hook = PostgresHook(postgres_conn_id="solar_data")

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO inverter (plant_id, source_key)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    sorted(equipment),
                )

        logger.info("Processed %s inverters", len(equipment))

    #load weather sensors to weather_sensor table in postgres db
    @task
    def load_weather_sensors():
        files = [
            "Plant_1_Weather_Sensor_Data.csv",
            "Plant_2_Weather_Sensor_Data.csv",
        ]

        equipment = set()

        for filename in files:
            df = pd.read_csv(
                DATA_PATH / filename,
                usecols=["PLANT_ID", "SOURCE_KEY"]
            )

            for row in df.itertuples(index=False):
                equipment.add((int(row.PLANT_ID), str(row.SOURCE_KEY)))

        hook = PostgresHook(postgres_conn_id="solar_data")

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO weather_sensor (plant_id, source_key)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    sorted(equipment),
                )

        logger.info("Processed %s weather sensors", len(equipment))

    @task
    def load_daily_readings():
        hook = PostgresHook(postgres_conn_id="solar_data")

        sources = [
            (
                "Plant_1_Generation_Data.csv",
                "generation_reading",
                ["DC_POWER", "AC_POWER", "DAILY_YIELD", "TOTAL_YIELD"],
            ),
            (
                "Plant_2_Generation_Data.csv",
                "generation_reading",
                ["DC_POWER", "AC_POWER", "DAILY_YIELD", "TOTAL_YIELD"],
            ),
            (
                "Plant_1_Weather_Sensor_Data.csv",
                "weather_reading",
                ["AMBIENT_TEMPERATURE", "MODULE_TEMPERATURE", "IRRADIATION"],
            ),
            (
                "Plant_2_Weather_Sensor_Data.csv",
                "weather_reading",
                ["AMBIENT_TEMPERATURE", "MODULE_TEMPERATURE", "IRRADIATION"],
            ),
        ]

        # Only these SQL statements can be selected.
        insert_sql = {
            "generation_reading": """
                INSERT INTO generation_reading (
                    plant_id, inverter_source_key, date_time,
                    dc_power, ac_power, daily_yield, total_yield
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            "weather_reading": """
                INSERT INTO weather_reading (
                    plant_id, sensor_source_key, date_time,
                    ambient_temperature, module_temperature, irradiation
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
        }

        # Closing the connection and managing a single transaction.
        with closing(hook.get_conn()) as conn:
            with conn:
                with conn.cursor() as cursor:

                    # Initialize progress to the day BEFORE the first load.
                    cursor.execute(
                        """
                        INSERT INTO ingestion_watermark (
                            pipeline_name, last_loaded_date
                        )
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        ("solar_readings", date(2020, 5, 14)),
                    )

                    # Lock progress for this transaction.
                    cursor.execute(
                        """
                        SELECT last_loaded_date
                        FROM ingestion_watermark
                        WHERE pipeline_name = %s
                        FOR UPDATE
                        """,
                        ("solar_readings",),
                    )

                    last_loaded_date = cursor.fetchone()[0]
                    load_date = last_loaded_date + timedelta(days=1)

                    # Stop when all historical data is processed.
                    if load_date > date(2020, 6, 17):
                        logger.info("All available dates have been loaded.")
                        return

                    logger.info("Loading readings for %s", load_date)

                    total_rows = 0

                    for filename, table_name, measurements in sources:
                        df = pd.read_csv(
                            DATA_PATH / filename,
                            dtype={"SOURCE_KEY": "string"},
                        )

                        # Detect the raw timestamp format.
                        sample = str(df["DATE_TIME"].dropna().iloc[0])

                        if sample[4] == "-":
                            date_format = "%Y-%m-%d %H:%M:%S"
                        else:
                            date_format = "%d-%m-%Y %H:%M"

                        df["DATE_TIME"] = pd.to_datetime(
                            df["DATE_TIME"],
                            format=date_format,
                            errors="raise",
                        )

                        # Select only the current day's records.
                        daily_df = df[
                            df["DATE_TIME"].dt.date == load_date
                        ]

                        rows = []

                        for record in daily_df.itertuples(index=False):
                            values = tuple(
                                None if pd.isna(getattr(record, column))
                                else float(getattr(record, column))
                                for column in measurements
                            )

                            rows.append((
                                int(record.PLANT_ID),
                                str(record.SOURCE_KEY),
                                record.DATE_TIME.to_pydatetime(),
                                *values,
                            ))

                        if rows:
                            cursor.executemany(
                                insert_sql[table_name],
                                rows,
                            )

                        total_rows += len(rows)

                        logger.info(
                            "%s: processed %s records for %s",
                            filename,
                            len(rows),
                            load_date,
                        )

                    # Never advance if the entire day was empty.
                    if total_rows == 0:
                        raise ValueError(
                            f"No records found for {load_date}"
                        )

                    # Update the watermark in the SAME transaction.
                    cursor.execute(
                        """
                        UPDATE ingestion_watermark
                        SET last_loaded_date = %s
                        WHERE pipeline_name = %s
                        """,
                        (load_date, "solar_readings"),
                    )

                    logger.info(
                        "Completed %s: processed %s records",
                        load_date,
                        total_rows,
                    )


    #add create_tables to exectute sql script from dags/sql
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        #matches connection created from UI
        conn_id="solar_data",
        sql="sql/create_tables.sql",
        #execute five statements separately
        split_statements=True,
    )
    #load plant ids to plant db
    plants = load_plants()
    inverters = load_inverters()
    weather_sensors = load_weather_sensors()
    daily_readings = load_daily_readings()

    file_check >> validation >> create_tables >> plants
    plants >> [inverters, weather_sensors]
    [inverters, weather_sensors] >> daily_readings



solar_ingestion()