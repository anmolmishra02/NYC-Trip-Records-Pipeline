from pathlib import Path
import logging
import os
import runpy

import yaml

# Run this entry point in Databricks, where spark and dbutils are available.
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("nyc_taxi_pipeline")

cwd = Path.cwd()
PROJECT_ROOT = next(
    (
        path for path in (cwd, *cwd.parents)
        if (path / "config" / "config.yaml").is_file()
        and (path / "src").is_dir()
    ),
    None,
)
if PROJECT_ROOT is None:
    raise RuntimeError("Could not locate project root from the current directory.")
os.chdir(PROJECT_ROOT)
logger.info("Project root: %s", PROJECT_ROOT)


def validate_configuration():
    """Load config and check compatibility with the existing setup scripts."""
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    with config_path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    if not isinstance(config, dict):
        raise ValueError("config/config.yaml must contain a YAML mapping")

    # Current setup scripts use these literal names. Fail early if changed.
    expected = {
        "storage.catalog": "workspace",
        "storage.schema": "nyc_taxi",
        "storage.volume": "source_data",
        "source.yellow_folder": "Yellow",
        "source.green_folder": "green",
        "schemas.bronze": "nyc_taxi_bronze",
        "schemas.silver": "nyc_taxi_silver",
        "schemas.gold": "nyc_taxi_gold",
    }
    for key, value in expected.items():
        section, field = key.split(".")
        actual = config.get(section, {}).get(field)
        if actual != value:
            raise ValueError(
                f"Configuration mismatch: {key}={actual!r}; "
                f"current setup scripts require {value!r}"
            )
    logger.info("Configuration validated")
    return config


def create_resources():
    """Run the simple CREATE statements in the existing setup SQL file."""
    sql_path = PROJECT_ROOT / "setup" / "create_resources.sql"
    sql_text = sql_path.read_text(encoding="utf-8")
    # Existing SQL uses only single-line comments and semicolon-delimited DDL.
    sql_text = "\n".join(
        line for line in sql_text.splitlines()
        if not line.lstrip().startswith("--")
    )
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    if not statements:
        raise ValueError(f"No SQL statements found in {sql_path}")
    logger.info("Creating/verifying schemas and source Volume")
    for statement in statements:
        spark.sql(statement)
    logger.info("Databricks resources ready")


def create_source_folders():
    """Execute the existing setup script with Databricks dbutils available."""
    script_path = PROJECT_ROOT / "setup" / "create_folders.py"
    logger.info("Creating/verifying source folders")
    runpy.run_path(
        str(script_path),
        run_name="__main__",
        init_globals={"dbutils": dbutils},
    )
    logger.info("Source folders ready")


def run_stage(stage_name, notebook_name):
    notebook_path = str(PROJECT_ROOT / "notebooks" / notebook_name)
    logger.info("Starting %s stage", stage_name)
    try:
        result = dbutils.notebook.run(notebook_path, 3600)
        if result != "SUCCESS":
            raise RuntimeError(
                f"{stage_name} returned unexpected status: {result!r}"
            )
        logger.info("%s stage completed successfully", stage_name)
    except Exception:
        logger.exception("%s stage failed", stage_name)
        raise


def main():
    logger.info("Starting NYC Taxi pipeline")
    try:
        validate_configuration()
        create_resources()
        create_source_folders()
        run_stage("Bronze", "bronze_ingestion")
        run_stage("Silver", "silver_transformations")
        run_stage("Gold", "gold_analytics")
        logger.info("NYC Taxi pipeline completed successfully")
    except Exception:
        logger.exception("Pipeline failed; remaining dependent stages skipped")
        raise


if __name__ == "__main__":
    main()
