
from pyspark.sql import functions as F


def apply_data_quality_checks(df, boundary_days=1):
    """
    Apply Silver-layer data quality checks.

    Error-level checks:
        Records failing these checks are quarantined.

    Warning-level checks:
        Records are retained in Silver with warning flags.

    Args:
        df: Unified Yellow and Green trips DataFrame.
        boundary_days: Allowed tolerance around source month boundaries.

    Returns:
        valid_df: Records passing all error-level checks.
        quarantine_df: Records failing one or more error-level checks.

    Timestamp assumption:
        Source timestamps are timestamp_ntz values representing
        New York local wall-clock time.
    """

    # --------------------------------------------------
    # 1. Source month validation
    # --------------------------------------------------

    source_month_date = F.to_date(
        F.concat(F.col("source_month"), F.lit("-01"))
    )

    expected_start_date = F.date_sub(
        source_month_date, boundary_days
    )

    expected_end_date = F.date_add(
        F.add_months(source_month_date, 1),
        boundary_days
    )

    # --------------------------------------------------
    # 2. Timestamp validation
    # --------------------------------------------------

    timestamp_outside_source_window = (
        (F.to_date("pickup_datetime") < expected_start_date)
        | (F.to_date("pickup_datetime") >= expected_end_date)
        | (F.to_date("dropoff_datetime") < expected_start_date)
        | (F.to_date("dropoff_datetime") >= expected_end_date)
    )

    # --------------------------------------------------
    # 3. Error-level checks: quarantine
    # --------------------------------------------------

    failure_reasons = [
        F.when(
            F.col("pickup_datetime").isNull(),
            F.lit("MISSING_PICKUP_DATETIME")
        ),

        F.when(
            F.col("dropoff_datetime").isNull(),
            F.lit("MISSING_DROPOFF_DATETIME")
        ),

        F.when(
            F.col("dropoff_datetime") < F.col("pickup_datetime"),
            F.lit("DROPOFF_BEFORE_PICKUP")
        ),

        F.when(
            F.col("pickup_location_id").isNull(),
            F.lit("MISSING_PICKUP_LOCATION")
        ),

        F.when(
            F.col("dropoff_location_id").isNull(),
            F.lit("MISSING_DROPOFF_LOCATION")
        ),

        F.when(
            F.col("total_amount").isNull(),
            F.lit("MISSING_TOTAL_AMOUNT")
        ),

        F.when(
            F.col("source_month").isNull()
            | source_month_date.isNull(),
            F.lit("INVALID_SOURCE_MONTH")
        ),

        F.when(
            timestamp_outside_source_window,
            F.lit("TRIP_TIMESTAMP_OUTSIDE_SOURCE_PERIOD")
        )
    ]

    # --------------------------------------------------
    # 4. Warning-level checks: retain and flag
    # --------------------------------------------------

    effective_date = F.lit(
        "2025-01-05 00:00:00"
    ).cast("timestamp_ntz")

    warning_reasons = [
        F.when(
            F.col("fare_amount") < 0,
            F.lit("NEGATIVE_FARE_AMOUNT")
        ),

        F.when(
            F.col("total_amount") < 0,
            F.lit("NEGATIVE_TOTAL_AMOUNT")
        ),

        F.when(
            F.col("cbd_congestion_fee") < 0,
            F.lit("NEGATIVE_CBD_CONGESTION_FEE")
        ),

        F.when(
            (F.col("dropoff_datetime") < effective_date)
            & (F.col("cbd_congestion_fee") > 0),
            F.lit("CBD_FEE_BEFORE_EFFECTIVE_DATE")
        )
    ]

    # --------------------------------------------------
    # 5. Attach DQ results
    # --------------------------------------------------

    checked_df = (
        df
        .withColumn(
            "dq_failure_reason",
            F.concat_ws("; ", *failure_reasons)
        )
        .withColumn(
            "dq_warning_reason",
            F.concat_ws("; ", *warning_reasons)
        )
    )

    # --------------------------------------------------
    # 6. Split valid and quarantined records
    # --------------------------------------------------

    valid_df = (
        checked_df
        .filter(F.col("dq_failure_reason") == "")
        .drop("dq_failure_reason")
    )

    quarantine_df = checked_df.filter(
        F.col("dq_failure_reason") != ""
    )

    return valid_df, quarantine_df
