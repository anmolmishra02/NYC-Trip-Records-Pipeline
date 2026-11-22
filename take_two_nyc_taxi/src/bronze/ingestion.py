
from pyspark.sql import functions as F

from config.config_loader import load_config


# --------------------------------------------------
# Configuration
# --------------------------------------------------

CONFIG = load_config()

STORAGE_CONFIG = CONFIG["storage"]
SOURCE_CONFIG = CONFIG["source"]

CATALOG = STORAGE_CONFIG["catalog"]
STORAGE_SCHEMA = STORAGE_CONFIG["schema"]
VOLUME = STORAGE_CONFIG["volume"]

SOURCE_PATH = (
    f"/Volumes/{CATALOG}/{STORAGE_SCHEMA}/{VOLUME}"
)

BRONZE_SCHEMA = f"{CATALOG}.{CONFIG['schemas']['bronze']}"

MONTHS = CONFIG["months"]

TABLES = CONFIG["tables"]


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
            f"{SOURCE_PATH}/{SOURCE_CONFIG['yellow_folder']}/"
            f"yellow_tripdata_{month}.parquet"
        )

        df = spark.read.parquet(file_path)

        df = (
            df
            .withColumn("source_month", F.lit(month))
            .withColumn("source_file", F.lit(file_path))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

        yellow_dfs.append(df)

        print(f"Read Yellow Taxi data for {month}")

    yellow_df = yellow_dfs[0]

    for df in yellow_dfs[1:]:
        yellow_df = yellow_df.unionByName(
            df,
            allowMissingColumns=True
        )

    write_bronze_table(
        yellow_df,
        TABLES["yellow_trips"]
    )

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
            f"{SOURCE_PATH}/{SOURCE_CONFIG['green_folder']}/"
            f"green_tripdata_{month}.parquet"
        )

        df = spark.read.parquet(file_path)

        df = (
            df
            .withColumn("source_month", F.lit(month))
            .withColumn("source_file", F.lit(file_path))
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

        green_dfs.append(df)

        print(f"Read Green Taxi data for {month}")

    green_df = green_dfs[0]

    for df in green_dfs[1:]:
        green_df = green_df.unionByName(
            df,
            allowMissingColumns=True
        )

    write_bronze_table(
        green_df,
        TABLES["green_trips"]
    )

    return green_df


# --------------------------------------------------
# 3. Taxi Zone Lookup Ingestion
# --------------------------------------------------

def ingest_taxi_zone_lookup(spark):
    """
    Read Taxi Zone Lookup CSV and write
    it into a Bronze Delta table.
    """

    file_path = (
        f"{SOURCE_PATH}/{SOURCE_CONFIG['zone_lookup_file']}"
    )

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

    write_bronze_table(
        zone_df,
        TABLES["taxi_zone_lookup"]
    )

    return zone_df
