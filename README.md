# NYC Taxi Trip Records Pipeline

**Senior Data Engineer Take-Home Exercise**

## Overview
A Databricks pipeline using PySpark and Delta Lake to process NYC TLC Yellow and Green taxi trip records for December 2024–February 2025. It follows Bronze → Silver → Gold and produces daily revenue, airport movement/tipping, and congestion-pricing comparison datasets.

## Repository layout
```text
take_two_nyc_taxi/
├── src/bronze/ingestion.py
├── src/silver/transformations.py
├── src/silver/quality.py
├── src/gold/aggregations.py
├── notebooks/01_bronze_ingestion
├── notebooks/02_silver_transformation
├── notebooks/03_gold_analytics
├── README.md
├── DQ_SPEC.md
├── DATA_DICTIONARY.md
├── AI_DISCLOSURE.md
└── architecture.mmd
```

## How to run
1. Use a Databricks workspace with a compatible Spark and Delta runtime.
2. Make the six monthly Parquet files and `taxi_zone_lookup.csv` available in the configured input location.
3. Configure the input path, catalog, schemas and reporting dates using notebook configuration/widgets. Avoid developer-specific absolute paths.
4. Run notebooks in order: `01_bronze_ingestion` → `02_silver_transformation` → `03_gold_analytics`.
5. Confirm row reconciliation and inspect the three Gold Delta tables.

The implementation was developed in Databricks. Source data files are excluded from the submission; ingestion reads them from a configurable location.

## Layer design
**Bronze** retains source-shaped Yellow and Green records separately, adds `source_month`, `source_file` and `ingestion_timestamp`, and stores the taxi-zone lookup. Observed ingestion counts: 10,721,140 Yellow trips, 148,941 Green trips and 265 lookup rows.

**Silver** standardizes Yellow `tpep_*` and Green `lpep_*` fields into one trip model, retains source lineage, and applies row-level quality rules. Missing optional attributes are typed NULLs, not fabricated zeros. Trip timestamps are preserved as `TIMESTAMP_NTZ` (NYC local wall-clock values); ingestion timestamps remain timestamps representing ingestion time. Invalid rows are quarantined; non-blocking anomalies are retained with warning metadata.

Final Silver reconciliation: 10,869,523 valid rows + 558 quarantined rows = 10,870,081 ingested trip rows.

**Gold** produces:
- `daily_revenue`: pickup date × pickup borough × service type.
- `airport_analysis`: month × airport × service type × direction.
- `congestion_pricing_impact`: before/after period × service type.

Column definitions, types, lineage and caveats are in `DATA_DICTIONARY.md`.

## Data quality findings and treatment
Blocking row-level failures include missing pickup/drop-off timestamps, drop-off before pickup, missing pickup/drop-off location IDs, missing total amount, invalid source month, and timestamps outside the expected source-month window. A one-day boundary tolerance is used for cross-month trips. Failed rows are quarantined.

Warnings include negative fare, negative total amount, negative CBD congestion fee, and a positive CBD fee before January 5, 2025. Warnings do not remove rows or alter source values. Four pre-effective-date positive-fee rows were observed in Silver and retained with a warning. Negative fee adjustments are also retained. See `DQ_SPEC.md`.

Yellow and Green are treated as the same business event and standardized into a common schema. Service-specific fields absent from one source are nullable. `cbd_congestion_fee` is absent from December 2024 input and remains NULL for that month.

## Business assumptions and caveats
- Revenue uses `total_amount` for payment types 0, 1 and 2, as implemented. Credit-card tips are already included in `total_amount` and are not added again. Cash tips are not fully observable. Validate payment-type semantics against the TLC dictionary before production use.
- Airport direction is based on the airport endpoint: pickup timestamp for FROM airport and drop-off timestamp for TO airport. Credit-card tipping metrics use observed credit-card tip values; cash tipping is not captured.
- Airport zones are identified through the taxi-zone lookup.
- Congestion pricing is treated as effective from January 5, 2025. Inbound CBD is approximated by a configured list of Manhattan taxi zone IDs, not an exact geofence. Drop-off time determines the period.
- The comparison is descriptive, not causal; it does not control for seasonality, demand mix or other external factors.
- Source data anomalies are preserved and flagged rather than silently corrected.

## Logging, monitoring and production
Log source files, input/output counts, rejected and warning counts, schema changes, execution duration and table-write outcomes. Alert on failed ingestion, schema incompatibility, failed reconciliation, unusual quarantine rates, stale Gold outputs and unexpected volume changes.

For production, package modules and notebooks, deploy through CI/CD, and orchestrate stages with Databricks Workflows. Roll back by restoring prior validated Delta table versions and redeploying the last known-good code release. At 100× volume, use incremental ingestion, bounded processing windows, optimized file sizes, workload-aware partitioning/clustering, autoscaling and measured shuffle/skew tuning. Introduce streaming/CDC only if latency requirements justify it.

## Deliberate omissions
Exact CBD geospatial boundary matching, causal inference, full cash-tip analysis, production CI/CD implementation, alert integrations and automated deployment/rollback are not implemented in this prototype. These are production extensions, not claimed as completed features.

## AI usage
AI assistance was used for debugging, reviewing transformation and aggregation logic, SQL diagnostics and drafting documentation. See `AI_DISCLOSURE.md`.
