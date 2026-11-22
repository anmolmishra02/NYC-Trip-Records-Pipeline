# Gold Data Dictionary

Types below are expected Spark output types based on the implemented expressions. Verify physical types using `DESCRIBE TABLE` before submission because source casts can affect numeric precision.

## 1. `daily_revenue`
**Grain:** one row per `pickup_date`, `pickup_borough`, `service_type`.

| Column | Expected type | Business definition | Source lineage | Caveats |
|---|---|---|---|---|
| `pickup_date` | DATE | Calendar date of pickup | Silver `pickup_datetime` | NYC local wall-clock date |
| `pickup_borough` | STRING | Borough of pickup zone | `pickup_location_id` → zone dimension | Depends on zone lookup coverage |
| `service_type` | STRING | Yellow or Green service | Silver `service_type` | — |
| `trip_count` | BIGINT | All trips in group | Silver unified trips | Includes excluded payment categories |
| `collected_trip_count` | BIGINT | Trips included in collected revenue | Silver `payment_type` | Current implementation uses types 0, 1, 2 |
| `excluded_payment_trip_count` | BIGINT | Trips excluded from collected revenue | Silver `payment_type` | Not necessarily failed payments |
| `collected_revenue` | DOUBLE / DECIMAL | Sum of `total_amount` for included payments | Silver `total_amount` | Card tips already included; cash tips may be absent |

## 2. `airport_analysis`
**Grain:** one row per `month`, `airport`, `service_type`, `direction`.

| Column | Expected type | Business definition | Source lineage | Caveats |
|---|---|---|---|---|
| `month` | STRING (`yyyy-MM`) | Month of airport event | Event timestamp derived from pickup/drop-off | FROM uses pickup; TO uses drop-off |
| `airport` | STRING | JFK, LaGuardia or Newark | Airport zone ID joined to zone dimension | Depends on zone lookup names |
| `service_type` | STRING | Yellow or Green | Silver `service_type` | — |
| `direction` | STRING | `TO_AIRPORT` or `FROM_AIRPORT` | Drop-off or pickup airport endpoint | Same trip could count in both directions if both endpoints are airport zones |
| `trip_count` | BIGINT | Number of airport movements | Count of joined movements | Movements, not necessarily unique trips across directions |
| `average_fare` | DOUBLE / DECIMAL | Mean fare | Silver `fare_amount` | Null fares ignored |
| `credit_card_trip_count` | BIGINT | Credit-card trips | Silver `payment_type` | Current code uses type 1 |
| `observed_card_tip_count` | BIGINT | Card trips with non-null tip | `payment_type`, `tip_amount` | Denominator for observed tip rate |
| `tipped_card_trip_count` | BIGINT | Card trips with positive tip | `payment_type`, `tip_amount` | Zero/negative tips not counted as positive |
| `average_card_tip` | DOUBLE / DECIMAL | Mean card tip | Silver `tip_amount` for type 1 | Null values ignored |
| `average_positive_card_tip` | DOUBLE / DECIMAL | Mean positive card tip | Silver `tip_amount` for type 1 and tip > 0 | Excludes zero/negative tips |
| `card_tip_rate_pct` | DOUBLE | Positive observed card tips ÷ observed card tips × 100 | Derived counts | Not a cash-tip rate; null if denominator is zero |

## 3. `congestion_pricing_impact`
**Grain:** one row per `period`, `service_type`.

| Column | Expected type | Business definition | Source lineage | Caveats |
|---|---|---|---|---|
| `period` | STRING | `BEFORE` or `AFTER` Jan 5, 2025 | Silver `dropoff_datetime` | Default BEFORE Dec 1–Jan 4; AFTER Jan 5–Feb 28 |
| `service_type` | STRING | Yellow or Green | Silver `service_type` | — |
| `trip_count` | BIGINT | Inbound CBD trips | Pickup/drop-off zone IDs | CBD approximated by configured taxi-zone list |
| `paid_trip_count` | BIGINT | Trips included in collected revenue | `payment_type`, `total_amount` | Current implementation uses types 0, 1, 2 |
| `collected_revenue` | DOUBLE / DECIMAL | Sum of total amount for included payments | Silver `total_amount` | Inherits payment-code assumptions |
| `average_fare` | DOUBLE / DECIMAL | Mean fare for inbound trips | Silver `fare_amount` | Includes all inbound trips |
| `total_cbd_congestion_fee` | DOUBLE / DECIMAL | Sum of CBD fee; NULL treated as zero in this aggregate | Silver `cbd_congestion_fee` | Negative adjustments retained; December source field absent/NULL |
| `trips_with_cbd_fee` | BIGINT | Trips with positive CBD fee | Silver `cbd_congestion_fee` | Negative adjustments not counted |
| `period_days` | INT | Calendar days used for normalization | Fixed assessment window | 35 BEFORE and 55 AFTER; fixed-window assumption |
| `average_trips_per_day` | DOUBLE | Trip count divided by period days | Derived Gold metric | Descriptive daily average |
| `average_revenue_per_day` | DOUBLE | Collected revenue divided by period days | Derived Gold metric | Not seasonality-adjusted |

## Cross-table conventions
- Silver trip timestamps are NYC local wall-clock values (`TIMESTAMP_NTZ`).
- `service_type` is `yellow` or `green`.
- Gold is derived from Silver; the taxi-zone dimension supplies borough and zone descriptions.
- The CBD taxi-zone list approximates the charging zone; it is not an exact geospatial boundary.
