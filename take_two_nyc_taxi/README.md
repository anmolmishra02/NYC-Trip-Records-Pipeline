# NYC Taxi Trip Records -- Data Engineering Pipeline

## 1. Project Overview

This project implements an end-to-end data engineering pipeline using
**Databricks, PySpark and Delta Lake** to process NYC Yellow and Green
Taxi trip records for December 2024, January 2025 and February 2025.

The solution follows the Medallion Architecture (Bronze, Silver and
Gold) to ingest raw data, standardize schemas, apply data quality checks
and generate analytics-ready datasets.

### Technology Stack

-   Platform: Databricks
-   Processing: Apache Spark / PySpark
-   Storage: Delta Lake / Unity Catalog Volumes
-   Configuration: YAML
-   Programming: Python and SQL
-   Version Control: GitHub

## 2. Project Structure

``` text
take_two_nyc_taxi/
│
├── config/
│   ├── config.yaml
│   └── config_loader.py
│
├── setup/
│   ├── create_resources.sql
│   └── create_folders.py
│
├── src/
│   ├── bronze/
│   │   └── ingestion.py
│   ├── silver/
│   │   ├── transformations.py
│   │   └── data_quality.py
│   └── gold/
│       └── aggregations.py
│
├── notebooks/
│   ├── bronze_ingestion
│   ├── silver_transformations
│   └── gold_analytics
│
├── README.md
├── DQ_SPEC.md
├── DATA_DICTIONARY.md
├── AI_DISCLOSURE.md
├── architecture.mmd
├── requirements.txt
└── .gitignore
```

## 3. Architecture and Layer Design

### Bronze -- Raw Ingestion

-   Ingests monthly Yellow and Green Taxi Parquet files and the Taxi
    Zone Lookup CSV.
-   Preserves source-level data while adding ingestion metadata such as
    source month, source file and ingestion timestamp.
-   Stores datasets as Delta tables.
-   Maintains separate Bronze tables for Yellow, Green and Taxi Zone
    Lookup data.

### Silver -- Standardization and Data Quality

-   Standardizes Yellow and Green datasets into a common schema.
-   Handles differences in source column names, data types and available
    fields.
-   Combines standardized datasets using `unionByName`.
-   Builds a Taxi Zone dimension.
-   Applies data quality checks and separates invalid records into a
    quarantine table.
-   Publishes validated unified trips and supporting dimension tables.

### Gold -- Business Analytics

Produces three analytical datasets:

1.  **Daily Revenue Analysis:** Daily revenue grouped by pickup date,
    pickup borough and service type.
2.  **Airport Trip Analysis:** Monthly airport trip volume, average fare
    and tipping behaviour for JFK, LaGuardia and Newark, grouped by
    service type and trip direction.
3.  **Congestion Pricing Impact:** Comparison of inbound Manhattan CBD
    trip metrics before and after congestion pricing implementation.

## 4. Configuration

Project settings are maintained in `config/config.yaml`.

The configuration includes:

-   Catalog, schema and Volume names.
-   Source folder names.
-   Processing months.
-   Bronze, Silver and Gold schema names.
-   Delta table names.
-   Reporting start and end dates.

`config/config_loader.py` loads these settings and makes them available
to pipeline modules and notebooks.

## 5. Prerequisites

-   Databricks workspace with Unity Catalog access.
-   Permissions to create schemas, tables and Volumes.
-   GitHub access to the project repository.
-   Yellow and Green Taxi Parquet files for December 2024, January 2025
    and February 2025.
-   Taxi Zone Lookup CSV.

## 6. How to Run the Project

Follow these steps to execute the pipeline in a Databricks environment.

### Step 1: Import the repository

1.  Open your Databricks workspace.
2.  Navigate to Git folders.
3.  Select **Add Git Folder**.
4.  Provide the GitHub repository URL.
5.  Select the required branch and create the Git Folder.
6.  Open the `take_two_nyc_taxi` project directory.

### Step 2: Configure the project

Open `config/config.yaml` and update the following values according to
your Databricks environment:

-   `storage.catalog`
-   `storage.schema`
-   `storage.volume`
-   Bronze, Silver and Gold schema names.
-   Source folder names, if different.
-   Reporting dates, if required.

Ensure that the catalog and schema names are consistent with the
resource setup script.

### Step 3: Create Databricks resources

Open `setup/create_resources.sql` in a Databricks SQL editor and execute
it.

This creates the required project resources:

-   Project schema.
-   Bronze schema.
-   Silver schema.
-   Gold schema.
-   Source data Volume.

The provided SQL assumes the catalog is named `workspace`. Update the
SQL statements if using another catalog.

### Step 4: Create source folders

Execute `setup/create_folders.py` in a Databricks Python execution
context.

This creates the expected Yellow and Green folders inside the source
Volume.

The current script uses the default `workspace.nyc_taxi.source_data`
location. Update its values if your Volume location is different.

### Step 5: Upload source data

Upload the source Parquet and CSV files into the Unity Catalog Volume.

Expected folder structure:

``` text
/Volumes/<catalog>/nyc_taxi/source_data/
│
├── Yellow/
│   ├── yellow_tripdata_2024-12.parquet
│   ├── yellow_tripdata_2025-01.parquet
│   └── yellow_tripdata_2025-02.parquet
│
├── green/
│   ├── green_tripdata_2024-12.parquet
│   ├── green_tripdata_2025-01.parquet
│   └── green_tripdata_2025-02.parquet
│
└── taxi_zone_lookup.csv
```

Ensure that the filenames and folder names match the configuration and
ingestion code.

### Step 6: Install dependencies

Project dependencies are documented in `requirements.txt`.

PyYAML is required for loading the YAML configuration. Databricks
provides Spark and Delta Lake through its runtime, so separate
installations are generally unnecessary.

If PyYAML is not already available, install it using:

``` python
%pip install PyYAML
```

Restart Python if Databricks requests it.

### Step 7: Execute the pipeline notebooks

Run the notebooks in the following order.

**1. Bronze Ingestion**

`notebooks/bronze_ingestion`

-   Reads Yellow and Green Parquet files.
-   Reads Taxi Zone Lookup CSV.
-   Adds ingestion metadata.
-   Writes the Bronze Delta tables.

**2. Silver Transformation**

`notebooks/silver_transformations`

-   Reads Bronze tables.
-   Standardizes Yellow and Green schemas.
-   Unifies the datasets.
-   Executes data quality checks.
-   Writes validated trips, quarantined trips and the Taxi Zone
    dimension.

**3. Gold Analytics**

`notebooks/gold_analytics`

-   Reads unified Silver trips and the Taxi Zone dimension.
-   Builds daily revenue analysis.
-   Builds airport trip analysis.
-   Builds congestion pricing impact analysis.
-   Writes the three Gold Delta tables.

The Gold notebook accepts configurable reporting start and end dates
through Databricks widgets.

### Step 8: Validate the output

After successful execution, verify the following tables.

**Bronze**

-   `yellow_trips`
-   `green_trips`
-   `taxi_zone_lookup`

**Silver**

-   `unified_trips`
-   `dim_taxi_zones`
-   `quarantined_trips`

**Gold**

-   `daily_revenue`
-   `airport_analysis`
-   `congestion_pricing_impact`

All tables are stored in their respective configured Unity Catalog
schemas.

## 7. Pipeline Orchestration

The pipeline was orchestrated using a Databricks multi-task job.

Execution dependency:

``` text
Bronze Ingestion
       |
       v
Silver Transformation
       |
       v
Gold Analytics
```

Each task runs after successful completion of its upstream dependency.

Pipeline execution can be monitored through the Databricks job run
interface.

## 8. Data Quality and Validation

Data quality checks are applied in the Silver layer before publishing
validated records.

Invalid records are separated into a dedicated quarantine Delta table.

The detailed validation rules, severity levels and failure handling are
documented in `DQ_SPEC.md`.

Observed processing results:

  Dataset                   Records
  -------------------- ------------
  Yellow Bronze          10,721,140
  Green Bronze              148,941
  Combined Bronze        10,870,081
  Silver Validated       10,869,523
  Silver Quarantined            558
  Taxi Zone Lookup              265

## 9. Yellow and Green Schema Reconciliation

Yellow and Green Taxi datasets have differences in their source schemas
and naming conventions.

The Silver transformation layer addresses these differences by:

-   Mapping equivalent source columns to standardized names.
-   Casting fields into consistent data types.
-   Handling unavailable source columns using nullable fields.
-   Adding a service type identifier.
-   Combining standardized datasets using `unionByName`.

This provides a consistent analytical structure while preserving the
distinction between Yellow and Green services.

## 10. Business Logic and Assumptions

-   Revenue is aggregated using the trip `total_amount` field.
-   Daily revenue is grouped by pickup date, pickup borough and service
    type.
-   Airport analysis identifies JFK, LaGuardia and Newark using Taxi
    Zone Lookup information.
-   Airport trip direction is determined using pickup and drop-off
    airport zones.
-   Tipping analysis uses observed positive credit-card tip amounts.
-   Congestion pricing analysis identifies inbound Manhattan CBD trips
    using the configured CBD zone definition.
-   January 5, 2025 is used as the congestion pricing effective date.
-   Trip drop-off timestamp is used as the CBD entry-time proxy.
-   The congestion pricing comparison is descriptive and should not be
    interpreted as a causal impact evaluation.

Detailed metric definitions are documented in `DATA_DICTIONARY.md`.

## 11. Operational Considerations

### Logging and Monitoring

-   Notebook execution status and Spark errors are available through
    Databricks.
-   Pipeline stages can be monitored through Databricks job runs.
-   Data quality results can be monitored through valid and quarantined
    record counts.

### Deployment and Rollback

-   Source code and configuration are maintained in GitHub.
-   Changes should be tested in a development environment before
    deployment.
-   Git commit history supports code versioning and rollback.
-   Delta table versions can support data recovery where applicable.

### Idempotency

The current implementation uses overwrite-based Delta writes for full
pipeline refreshes. Re-running the pipeline with the same source data
and configuration is intended to reproduce the target datasets.

For production-scale incremental processing, ingestion tracking,
deduplication, merge-based writes and retry-safe processing should be
introduced.

### Scaling to 100x Data Volume

Recommended improvements include:

-   Incremental ingestion instead of full refreshes.
-   Partition pruning and optimized file sizing.
-   Delta Lake OPTIMIZE and appropriate clustering strategies.
-   Spark shuffle and executor resource tuning.
-   Parallel processing of independent source datasets.
-   Incremental Gold aggregations.
-   Centralized data quality monitoring and alerting.

## 12. Current Scope and Limitations

-   The implementation processes three monthly data periods.
-   Pipeline writes currently use full-refresh overwrite behaviour.
-   Production alerting and automated failure notifications are not
    implemented.
-   Congestion pricing analysis is descriptive rather than causal.
-   Large-scale incremental processing and performance tuning remain
    future enhancements.

## 13. Supporting Documentation

-   `DQ_SPEC.md` -- Data quality rules, severity and failure handling.
-   `DATA_DICTIONARY.md` -- Gold dataset definitions and metric
    descriptions.
-   `AI_DISCLOSURE.md` -- Disclosure of AI-assisted development.
-   `architecture.mmd` -- Pipeline architecture diagram.
