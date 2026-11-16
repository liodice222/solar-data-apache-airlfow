# solar-data-apache-airlfow

Architecture: 
CSV Sources from Kaggle dataset https://www.kaggle.com/datasets/anikannal/solar-power-generation-data   --> 



Python and Jupyter Notebooks: Validation and exploratory data analysis --> 


Apache Airflow: Schema creation and table loading --> 

PostgreSQL: Incremental telemetry ingestion with tracking

#Key Objectives: 
Create schema 
Create ingestion orchestration based on specific scheduling
Ensure ingestion is tracked for idempotent inserts and source validation 

#Docker dependencies: 
Apache Airflow 3.3.2 runs through Docker Compose

PostgreSQL stores Airflow metadata and solar telemetry data in separate databases
Redis supports task queuing with CeleryExecutor.

#Python dependencies: see requirements.txt

#Volume mounting: Local dags/, logs/, and data/ directories are mounted into Airflow containers, allowing DAG discovery, persistent logs, and access to source CSV files.

#Environment configuration: Docker Compose manages Airflow services and configuration, keys are stored on .env


#Mermaid Diagram of steps 
flowchart TD
    A["check_source_files"] --> B["validate_source_data"]
    B --> C["create_tables"]
    C --> D["load_plants"]
    D --> E["load_inverters"]
    D --> F["load_weather_sensors"]
    E --> G["load_daily_readings"]
    F --> G
