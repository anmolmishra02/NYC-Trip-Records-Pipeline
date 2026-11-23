# NYC Taxi Trip Records – Data Engineering Pipeline

## 1. Project Overview

An end-to-end data engineering pipeline built using **Databricks, PySpark and Delta Lake** to process NYC Yellow and Green Taxi trip records for December 2024, January 2025 and February 2025.

The solution follows the Medallion Architecture (Bronze, Silver and Gold) to ingest raw data, standardize source schemas, enforce data quality and generate business-ready analytics.

**Technology Stack:** Databricks, Apache Spark, PySpark, Delta Lake, Unity Catalog, Python, SQL, YAML and GitHub.

## 2. Project Structure

```text
take_two_nyc_taxi/
├── config/
│   ├── config.yaml
│   └── config_loader.py
├── setup/
│   ├── create_resources.sql
│   └── create_folders.py
├── src/
│   ├── bronze/
│   │   ├── ingestion.py
│   │   └── download_source_files.py
│   ├── silver/
│   │   ├── transformations.py
│   │   └── data_quality.py
│   └── gold/
│       └── aggregations.py
├── notebooks/
│   ├── bronze_ingestion
│   ├── silver_transformations
│   └── gold_analytics
├── output/
│   ├── screenshots/
│   ├── sql_queries/
│   │   ├── daily_revenue.sql
│   │   ├── airport_analysis.sql
│   │   └── congestion_pricing_impact.sql
│   └── results/
│       ├── daily_revenue/
│       │   └── daily_revenue.csv
│       ├── airport_analysis/
│       │   └── airport_analysis.csv
│       └── congestion_pricing/
│           └── congestion_pricing_impact.csv
├── README.md
├── DQ_SPEC.md
├── DATA_DICTIONARY.md
├── AI_DISCLOSURE.md
├── architecture.mmd
├── main.py
├── requirements.txt
└── .gitignore
```

## 3. Architecture and Layer Design

### Bronze – Raw Ingestion
- Reads monthly Yellow and Green Parquet files and the Taxi Zone Lookup CSV.
- Adds ingestion metadata: source month, source file and ingestion timestamp.
- Stores raw datasets as Delta tables.
- Maintains separate tables for Yellow, Green and Taxi Zone Lookup data.

### Silver – Standardization and Data Quality
- Maps Yellow and Green source columns into a common canonical schema.
- Handles differences in column names, data types and source availability.
- Combines standardized records using `unionByName`.
- Builds the Taxi Zone dimension.
- Applies validation rules and separates invalid records into a quarantine table.
- Publishes unified trips and supporting dimension tables.

### Gold – Business Analytics

Three analytics-ready Delta tables are generated:

1. **Daily Revenue:** Daily trip volume and collected revenue by pickup date, borough and service type.
2. **Airport Analysis:** Monthly trip volume, average fare and credit-card tipping behaviour for JFK, LaGuardia and Newark, by service type and direction.
3. **Congestion Pricing Impact:** Descriptive comparison of inbound Manhattan CBD taxi trips before and after January 5, 2025.

## 4. Configuration and Prerequisites

Project configuration is maintained in `config/config.yaml`, including:
- Catalog, schema and Volume names.
- Source folder names and reporting months.
- Bronze, Silver and Gold schemas and table names.
- Reporting date range.

**Prerequisites:**
- Databricks workspace with Unity Catalog access.
- Permissions to create schemas, tables and Volumes.
- GitHub repository connected through Databricks Git Folders.
- Required Yellow and Green monthly Parquet files and Taxi Zone Lookup CSV.
- PyYAML dependency (install using `%pip install PyYAML` if unavailable).

## 5. Setup and Execution

1. **Import repository:** Add the GitHub repository as a Databricks Git Folder and open `take_two_nyc_taxi`.

2. **Configure:** Review `config/config.yaml` and ensure the catalog, schemas, Volume and reporting dates match the target environment.

3. **Create resources:** Execute `setup/create_resources.sql` to create the required schemas and source Volume.

4. **Create source folders:** Execute `setup/create_folders.py` in a Databricks Python context.

5. **Upload source data:** Place the monthly Yellow and Green Parquet files in their respective `Yellow/` and `green/` directories and `taxi_zone_lookup.csv` at the root of the Volume.

Expected source location:

```text
/Volumes/workspace/nyc_taxi/source_data/
├── Yellow/
├── green/
└── taxi_zone_lookup.csv
```

6. **Run pipeline:** Execute `main.py` from the configured Databricks environment. It orchestrates the three notebooks sequentially:

```text
Bronze Ingestion → Silver Transformation → Gold Analytics
```

Each downstream stage runs only after successful completion of its upstream stage. Each notebook returns a success status to the orchestrator; failures are propagated and logged.

The individual notebooks can also be executed separately for development and troubleshooting.

## 6. Data Quality and Processing Results

Silver-layer validation checks identify missing timestamps and location identifiers, invalid trip durations, missing amounts, invalid source periods and other data issues. Invalid records are quarantined, while warning-level conditions are retained for analysis.

Observed processing results:

| Dataset | Records |
|---|---:|
| Yellow Bronze | 10,721,140 |
| Green Bronze | 148,941 |
| Combined Bronze | 10,870,081 |
| Silver Validated | 10,869,523 |
| Silver Quarantined | 558 |
| Taxi Zone Lookup | 265 |

Detailed checks, severity levels and failure actions are documented in `DQ_SPEC.md`.

## 7. Gold Results and Evidence

The generated Gold Delta tables are:

- `workspace.nyc_taxi_gold.daily_revenue`
- `workspace.nyc_taxi_gold.airport_analysis`
- `workspace.nyc_taxi_gold.congestion_pricing_impact`

Exported CSV results are included under `output/results/`.

| Gold output | Exported records |
|---|---:|
| Daily Revenue | 1,230 |
| Airport Analysis | 34 |
| Congestion Pricing Impact | See exported CSV |

Reusable SQL queries are available under `output/sql_queries/`. Pipeline execution and validation evidence are available under `output/screenshots/`.

## 8. Business Logic and Assumptions

- Revenue uses the trip `total_amount` field; collected revenue includes payment types 0, 1 and 2 when the amount is available.
- Daily revenue is grouped by pickup date, pickup borough and service type.
- Airport zones are identified using Taxi Zone Lookup data.
- Airport direction is determined by whether the pickup or drop-off location is an airport.
- Tipping behaviour is evaluated using observed credit-card tip values; positive tips are separately measured.
- Inbound CBD trips are identified using configured taxi zone IDs, with pickup outside and drop-off inside the defined CBD zones.
- January 5, 2025 is treated as the congestion pricing effective date.
- Drop-off timestamp is used as a proxy for CBD entry time.
- The before/after comparison is descriptive, not a causal impact assessment.
- Yellow and Green records are standardized into a common schema; unavailable source fields are represented as nullable fields.

Detailed Gold column definitions and metric descriptions are provided in `DATA_DICTIONARY.md`.

## 9. Operational Considerations and Scalability

**Logging and Monitoring**
- Pipeline execution and Spark errors are available through Databricks job runs.
- Data quality outcomes are monitored through valid and quarantined record counts.
- Notebook execution status is returned to the orchestrator.

**Deployment and Rollback**
- Source code and configuration are maintained in GitHub.
- Changes should be validated in development before deployment.
- Git commit history supports code rollback.
- Delta table history and versioning can support data recovery where applicable.

**Idempotency**
- The current implementation uses overwrite-based Delta writes for full refreshes.
- Re-running with the same source data and configuration is intended to reproduce the target datasets.
- Incremental ingestion and merge-based processing are recommended for production workloads.

**Scaling to 100x Data Volume**
- Introduce incremental ingestion and processing.
- Optimize file sizes, partition pruning and Delta clustering.
- Tune Spark shuffle partitions and compute resources.
- Parallelize independent ingestion workloads.
- Implement incremental Gold aggregations.
- Centralize monitoring, data quality reporting and automated alerting.

## 10. Scope and Supporting Documentation

Current limitations include the three-month processing window, full-refresh write strategy, and absence of production alert notifications. Advanced incremental processing and large-scale performance tuning are future enhancements.

Supporting documents:

- `DQ_SPEC.md` – Data quality checks, severity and failure handling.
- `DATA_DICTIONARY.md` – Gold table definitions and metrics.
- `AI_DISCLOSURE.md` – AI assistance disclosure.
- `architecture.mmd` – Pipeline architecture diagram.
