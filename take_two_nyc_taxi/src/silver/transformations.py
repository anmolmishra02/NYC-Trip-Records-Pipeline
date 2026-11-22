
from pyspark.sql import functions as F



def get_source_column(df, column_name, data_type, alias_name=None):

    output_name = alias_name or column_name

    if column_name in df.columns:
        return (
            F.col(column_name)
            .cast(data_type)
            .alias(output_name)
        )

    return (
        F.lit(None)
        .cast(data_type)
        .alias(output_name)
    )


def standardize_taxi_data(df, service_type):
    """
    Standardize Yellow and Green taxi source schemas
    into one canonical Silver schema.
    """

    is_yellow = service_type == "yellow"

    pickup_column = (
        "tpep_pickup_datetime" if is_yellow
        else "lpep_pickup_datetime"
    )

    dropoff_column = (
        "tpep_dropoff_datetime" if is_yellow
        else "lpep_dropoff_datetime"
    )

    standardized_df = df.select(

        # Trip identifiers and attributes
        get_source_column(df, "VendorID", "int").alias("vendor_id"),

        get_source_column(
            df, "RatecodeID", "double"
        ).alias("ratecode_id"),

        get_source_column(
            df, "passenger_count", "double"
        ).alias("passenger_count"),

        get_source_column(
            df, "trip_distance", "double"
        ).alias("trip_distance"),

        get_source_column(
            df, "store_and_fwd_flag", "string"
        ).alias("store_and_fwd_flag"),

        get_source_column(
            df, "trip_type", "int"
        ).alias("trip_type"),

        # Timestamps
        get_source_column(
        df, pickup_column, "timestamp_ntz"
        ).alias("pickup_datetime"),

        get_source_column(
            df, dropoff_column, "timestamp_ntz"
        ).alias("dropoff_datetime"),

        # Pickup and drop-off locations
        get_source_column(
            df, "PULocationID", "int"
        ).alias("pickup_location_id"),

        get_source_column(
            df, "DOLocationID", "int"
        ).alias("dropoff_location_id"),

        # Payment information
        get_source_column(
            df, "payment_type", "int"
        ).alias("payment_type"),

        # Financial fields
        get_source_column(df, "fare_amount", "double"),
        get_source_column(df, "extra", "double"),
        get_source_column(df, "mta_tax", "double"),
        get_source_column(df, "tip_amount", "double"),
        get_source_column(df, "tolls_amount", "double"),

        get_source_column(
            df, "improvement_surcharge", "double"
        ),

        get_source_column(df, "total_amount", "double"),

        get_source_column(
            df, "congestion_surcharge", "double"
        ),

        get_source_column(df, "Airport_fee", "double")
            .alias("airport_fee"),

        get_source_column(df, "ehail_fee", "double"),

        # Added in January 2025.
        # December 2024 records will have NULL.
        get_source_column(
            df, "cbd_congestion_fee", "double"
        ),

        # Pipeline metadata
        F.lit(service_type).alias("service_type"),

        get_source_column(
            df, "source_month", "string"
        ),

        get_source_column(
            df, "source_file", "string"
        ),

        get_source_column(
            df, "ingestion_timestamp", "timestamp"
        )
    )

    return standardized_df


def build_taxi_zone_dimension(zone_df):
    """
    Standardize the taxi zone lookup into
    a Silver dimension table.
    """

    dimension_df = zone_df.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim(F.col("Borough")).alias("borough"),
        F.trim(F.col("Zone")).alias("zone"),
        F.trim(F.col("service_zone")).alias("service_zone")
    )

    return dimension_df
