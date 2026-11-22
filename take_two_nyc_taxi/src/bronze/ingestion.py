
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------
# Configuration
# --------------------------------------------------

SOURCE_PATH = "/Volumes/workspace/nyc_taxi/source_data"

BRONZE_SCHEMA = "workspace.nyc_taxi_bronze"

MONTHS = [
    "2024-12",
    "2025-01",
    "2025-02"
]


# --------------------------------------------------
# Helper: Write DataFrame to Delta
# --------------------------------------------------

def write_bronze_table(df, table_name):
    """
    Write source data into a Bronze Delta table.
    """

    full_table_name = f"{BRONZE_SCHEMA}.{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print(f"Successfully written: {full_table_name}")


# --------------------------------------------------
# 1. Yellow Taxi Ingestion
# --------------------------------------------------

def ingest_yellow_trips(spark):
    """
    Read all monthly Yellow Taxi Parquet files
    and write them into a Bronze Delta table.
    """

    yellow_dfs = []

    for month in MONTHS:

        file_path = (
            f"{SOURCE_PATH}/Yellow/"
            f"yellow_tripdata_{month}.parquet"
        )

        df = spark.read.parquet(file_path)

        # Add ingestion metadata
        df = (
            df
            .withColumn("source_month", F.lit(month))
            .withColumn("source_file", F.lit(file_path))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

        yellow_dfs.append(df)

        print(f"Read Yellow Taxi data for {month}")

    # Combine all months
    yellow_df = yellow_dfs[0]

    for df in yellow_dfs[1:]:
        yellow_df = yellow_df.unionByName(
            df,
            allowMissingColumns=True
        )

    write_bronze_table(yellow_df, "yellow_trips")

    return yellow_df


# --------------------------------------------------
# 2. Green Taxi Ingestion
# --------------------------------------------------

def ingest_green_trips(spark):
    """
    Read all monthly Green Taxi Parquet files
    and write them into a Bronze Delta table.
    """

    green_dfs = []

    for month in MONTHS:

        file_path = (
            f"{SOURCE_PATH}/green/"
            f"green_tripdata_{month}.parquet"
        )

        df = spark.read.parquet(file_path)

        # Add ingestion metadata
        df = (
            df
            .withColumn("source_month", F.lit(month))
            .withColumn("source_file", F.lit(file_path))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

        green_dfs.append(df)

        print(f"Read Green Taxi data for {month}")

    # Combine all months
    green_df = green_dfs[0]

    for df in green_dfs[1:]:
        green_df = green_df.unionByName(
            df,
            allowMissingColumns=True
        )

    write_bronze_table(green_df, "green_trips")

    return green_df


# --------------------------------------------------
# 3. Taxi Zone Lookup Ingestion
# --------------------------------------------------

def ingest_taxi_zone_lookup(spark):
    """
    Read Taxi Zone Lookup CSV and write
    it into a Bronze Delta table.
    """

    file_path = f"{SOURCE_PATH}/taxi_zone_lookup.csv"

    zone_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path)
    )

    zone_df = (
        zone_df
        .withColumn("source_file", F.lit(file_path))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    write_bronze_table(zone_df, "taxi_zone_lookup")

    return zone_df
