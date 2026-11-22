
-- Create project schemas
CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi;
CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_silver;
CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_gold;

-- Create source data volume
CREATE VOLUME IF NOT EXISTS workspace.nyc_taxi.source_data;
