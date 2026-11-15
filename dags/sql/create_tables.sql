
CREATE TABLE IF NOT EXISTS plant (
    plant_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS inverter (
    plant_id BIGINT NOT NULL REFERENCES plant(plant_id),
    source_key VARCHAR(64) NOT NULL,
    PRIMARY KEY (plant_id, source_key)
);

CREATE TABLE IF NOT EXISTS weather_sensor (
    plant_id BIGINT NOT NULL REFERENCES plant(plant_id),
    source_key VARCHAR(64) NOT NULL,
    PRIMARY KEY (plant_id, source_key)
);

CREATE TABLE IF NOT EXISTS generation_reading (
    plant_id BIGINT NOT NULL,
    inverter_source_key VARCHAR(64) NOT NULL,
    date_time TIMESTAMP NOT NULL,
    dc_power NUMERIC,
    ac_power NUMERIC,
    daily_yield NUMERIC,
    total_yield NUMERIC,
    PRIMARY KEY (plant_id, inverter_source_key, date_time),
    FOREIGN KEY (plant_id, inverter_source_key)
        REFERENCES inverter(plant_id, source_key)
);

CREATE TABLE IF NOT EXISTS weather_reading (
    plant_id BIGINT NOT NULL,
    sensor_source_key VARCHAR(64) NOT NULL,
    date_time TIMESTAMP NOT NULL,
    ambient_temperature NUMERIC,
    module_temperature NUMERIC,
    irradiation NUMERIC,
    PRIMARY KEY (plant_id, sensor_source_key, date_time),
    FOREIGN KEY (plant_id, sensor_source_key)
        REFERENCES weather_sensor(plant_id, source_key)
);
