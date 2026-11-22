
from pyspark.sql import functions as F


def build_daily_revenue(
    trips_df,
    zones_df,
    start_date=None,
    end_date=None
):
    """
    Build daily revenue by pickup borough and service type.

    Grain:
        pickup_date + pickup_borough + service_type

    Collected revenue:
        Sum of total_amount for successful payment types
        (Flex Fare, Credit Card, Cash).
    """

    # Select pickup-zone attributes
    pickup_zones = zones_df.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("borough").alias("pickup_borough")
    )

    # Enrich trips with pickup borough
    enriched_df = (
        trips_df
        .join(
            F.broadcast(pickup_zones),
            on="pickup_location_id",
            how="left"
        )
        .withColumn(
            "pickup_date",
            F.to_date("pickup_datetime")
        )
        .withColumn(
            "pickup_borough",
            F.coalesce(F.col("pickup_borough"), F.lit("Unknown"))
        )
    )

    # Optional date-range filtering
    if start_date is not None:
        enriched_df = enriched_df.filter(
            F.col("pickup_date") >= F.to_date(F.lit(start_date))
        )

    if end_date is not None:
        enriched_df = enriched_df.filter(
            F.col("pickup_date") <= F.to_date(F.lit(end_date))
        )

    # Payment types representing completed payments
    successful_payment = (
        F.col("payment_type").isin(0, 1, 2)
        & F.col("total_amount").isNotNull()
    )

    # Convert currency to decimal before aggregation
    collected_amount = F.col("total_amount").cast("decimal(18,2)")

    daily_revenue_df = (
        enriched_df
        .groupBy(
            "pickup_date",
            "pickup_borough",
            "service_type"
        )
        .agg(
            F.count("*").alias("trip_count"),

            F.sum(
                F.when(successful_payment, 1).otherwise(0)
            ).alias("collected_trip_count"),

            F.sum(
                F.when(
                    successful_payment,
                    collected_amount
                ).otherwise(F.lit(0).cast("decimal(18,2)"))
            ).alias("collected_revenue"),

            F.sum(
    F.when(successful_payment, 0).otherwise(1)
).alias("excluded_payment_trip_count")
        )
    )

    return daily_revenue_df


def build_airport_analysis(trips_df,zones_df,start_date=None,end_date=None):
    """
    Airport analysis by month, airport, service type and direction.

    Grain:
        month + airport + service_type + direction

    Airport trips are identified using pickup/drop-off
    taxi zone IDs from the zone dimension.
    """

    # Identify the three airport zones using their actual names
    airport_lookup = (
        zones_df
        .filter(
            F.lower(F.col("zone")).isin(
                "jfk airport",
                "laguardia airport",
                "newark airport"
            )
        )
        .select(
            F.col("location_id"),
            F.col("zone").alias("airport")
        )
    )

    # ---------------------------------------
    # Trips TO airports
    # Airport is the drop-off location
    # ---------------------------------------

    to_airport_df = (
        trips_df.alias("t")
        .join(
            F.broadcast(airport_lookup).alias("a"),
            F.col("t.dropoff_location_id") == F.col("a.location_id"),
            "inner"
        )
        .select(
            F.col("a.airport"),
            F.lit("TO_AIRPORT").alias("direction"),
            F.col("t.dropoff_datetime").alias("airport_event_datetime"),
            F.col("t.service_type"),
            F.col("t.fare_amount"),
            F.col("t.payment_type"),
            F.col("t.tip_amount")
        )
    )

    # ---------------------------------------
    # Trips FROM airports
    # Airport is the pickup location
    # ---------------------------------------

    from_airport_df = (
        trips_df.alias("t")
        .join(
            F.broadcast(airport_lookup).alias("a"),
            F.col("t.pickup_location_id") == F.col("a.location_id"),
            "inner"
        )
        .select(
            F.col("a.airport"),
            F.lit("FROM_AIRPORT").alias("direction"),
            F.col("t.pickup_datetime").alias("airport_event_datetime"),
            F.col("t.service_type"),
            F.col("t.fare_amount"),
            F.col("t.payment_type"),
            F.col("t.tip_amount")
        )
    )

    # Combine arrivals and departures
    airport_trips = (
        to_airport_df
        .unionByName(from_airport_df)
    )

    # Filter using the airport event timestamp
    # FROM_AIRPORT -> pickup_datetime
    # TO_AIRPORT   -> dropoff_datetime
    if start_date and end_date:
        airport_trips = airport_trips.filter(
            (F.to_date("airport_event_datetime") >= F.lit(start_date).cast("date")) &
            (F.to_date("airport_event_datetime") <= F.lit(end_date).cast("date"))
        )

    # Derive reporting month after date filtering
    airport_trips = airport_trips.withColumn(
        "month",
        F.date_format(
            F.col("airport_event_datetime"),
            "yyyy-MM"
        )
    )

    # Credit card payment type = 1
    is_credit_card = F.col("payment_type") == 1

    # Only use records with observed credit-card tip values
    observed_card_tip = (
        is_credit_card
        & F.col("tip_amount").isNotNull()
    )

    positive_card_tip = (
        observed_card_tip
        & (F.col("tip_amount") > 0)
    )

    airport_analysis_df = (
        airport_trips
        .groupBy(
            "month",
            "airport",
            "service_type",
            "direction"
        )
        .agg(

            F.count("*").alias("trip_count"),

            F.avg("fare_amount").alias("average_fare"),

            F.sum(
                F.when(is_credit_card, 1).otherwise(0)
            ).alias("credit_card_trip_count"),

            F.sum(
                F.when(observed_card_tip, 1).otherwise(0)
            ).alias("observed_card_tip_count"),

            F.sum(
                F.when(positive_card_tip, 1).otherwise(0)
            ).alias("tipped_card_trip_count"),

            F.avg(
                F.when(is_credit_card, F.col("tip_amount"))
            ).alias("average_card_tip"),

            F.avg(
                F.when(positive_card_tip, F.col("tip_amount"))
            ).alias("average_positive_card_tip")
        )
    )

    # Calculate tipping rate using only observed card tips
    airport_analysis_df = (
        airport_analysis_df
        .withColumn(
            "card_tip_rate_pct",
            F.when(
                F.col("observed_card_tip_count") > 0,
                F.round(
                    F.col("tipped_card_trip_count") * 100.0
                    / F.col("observed_card_tip_count"),
                    2
                )
            )
        )
    )

    return airport_analysis_df


def build_congestion_pricing_impact(
    trips_df,
    start_date="2024-12-01",
    end_date="2025-02-28"
):
    """
    Compare inbound CBD taxi trips before and after
    congestion pricing began on 2025-01-05.

    CBD classification is based on TLC taxi zone IDs,
    which approximate the official CRZ boundary.

    Reporting periods:
        BEFORE: start_date through 2025-01-04
        AFTER:  2025-01-05 through end_date

    Note:
        This is a descriptive before/after comparison,
        not a causal impact assessment.
    """

    cbd_zone_ids = [
        4, 12, 13, 45, 48, 50, 68, 79,
        87, 88, 90, 100, 107, 113, 114,
        125, 137, 144, 148, 158, 161, 162,
        163, 164, 170, 186, 209, 211, 229,
        230, 231, 232, 233, 234, 246, 249, 261
    ]

    # Identify inbound CBD trips:
    # Pickup outside CBD and drop-off inside CBD.
    is_inbound_cbd = (
        F.col("pickup_location_id").isNotNull()
        & F.col("dropoff_location_id").isNotNull()
        & (~F.col("pickup_location_id").isin(cbd_zone_ids))
        & F.col("dropoff_location_id").isin(cbd_zone_ids)
    )

    # Define reporting boundaries using TIMESTAMP_NTZ.
    start_timestamp = (
        F.lit(f"{start_date} 00:00:00")
        .cast("timestamp_ntz")
    )

    end_timestamp = (
        F.date_add(F.lit(end_date).cast("date"), 1)
        .cast("timestamp_ntz")
    )

    effective_timestamp = (
        F.lit("2025-01-05 00:00:00")
        .cast("timestamp_ntz")
    )

    # Use drop-off timestamp as the CBD entry timestamp.
    inbound_trips = (
        trips_df
        .filter(is_inbound_cbd)
        .filter(
            (F.col("dropoff_datetime") >= start_timestamp)
            & (F.col("dropoff_datetime") < end_timestamp)
        )
        .withColumn(
            "period",
            F.when(
                F.col("dropoff_datetime") < effective_timestamp,
                F.lit("BEFORE")
            ).otherwise(F.lit("AFTER"))
        )
    )

    # Payment types assumed to represent completed payments.
    successful_payment = (
        F.col("payment_type").isin(0, 1, 2)
        & F.col("total_amount").isNotNull()
    )

    # Aggregate by period and service type.
    result_df = (
        inbound_trips
        .groupBy("period", "service_type")
        .agg(
            F.count("*").alias("trip_count"),

            F.sum(
                F.when(successful_payment, 1).otherwise(0)
            ).alias("paid_trip_count"),

            F.sum(
                F.when(
                    successful_payment,
                    F.col("total_amount")
                ).otherwise(F.lit(0))
            ).alias("collected_revenue"),

            F.avg("fare_amount").alias("average_fare"),

            F.sum(
                F.coalesce(
                    F.col("cbd_congestion_fee"),
                    F.lit(0)
                )
            ).alias("total_cbd_congestion_fee"),

            F.sum(
                F.when(
                    F.col("cbd_congestion_fee") > 0, 1
                ).otherwise(0)
            ).alias("trips_with_cbd_fee")
        )
        .withColumn(
            "average_fare",
            F.round("average_fare", 2)
        )
        .withColumn(
            "collected_revenue",
            F.round("collected_revenue", 2)
        )
        .withColumn(
            "total_cbd_congestion_fee",
            F.round("total_cbd_congestion_fee", 2)
        )
    )

    # Normalize period-level metrics by calendar days.
    # Assessment reporting window: 35 days before, 55 days after.
    result_df = (
        result_df
        .withColumn(
            "period_days",
            F.when(F.col("period") == "BEFORE", 35)
             .otherwise(55)
        )
        .withColumn(
            "average_trips_per_day",
            F.round(
                F.col("trip_count") / F.col("period_days"), 2
            )
        )
        .withColumn(
            "average_revenue_per_day",
            F.round(
                F.col("collected_revenue") / F.col("period_days"), 2
            )
        )
    )

    return result_df
