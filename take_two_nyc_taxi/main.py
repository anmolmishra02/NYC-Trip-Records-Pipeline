
from pathlib import Path
import os
import logging

# --------------------------------------------------
# Project root detection
# --------------------------------------------------

cwd = Path.cwd()

PROJECT_ROOT = next(
    (
        path for path in [cwd, *cwd.parents]
        if (path / "config" / "config.yaml").exists()
        and (path / "src").is_dir()
    ),
    None
)

if PROJECT_ROOT is None:
    raise RuntimeError("Could not locate project root.")

os.chdir(PROJECT_ROOT)

# --------------------------------------------------
# Logging configuration
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("nyc_taxi_pipeline")

logger.info("Project root: %s", PROJECT_ROOT)


# --------------------------------------------------
# Pipeline execution
# --------------------------------------------------

def run_stage(stage_name, notebook_name):

    notebook_path = str(PROJECT_ROOT / "notebooks" / notebook_name)

    logger.info("Starting %s stage", stage_name)

    try:
        result = dbutils.notebook.run(
            notebook_path,
            3600
        )

        if result != "SUCCESS":
            raise RuntimeError(
                f"{stage_name} returned unexpected status: {result}"
            )

        logger.info("%s stage completed successfully", stage_name)

    except Exception:
        logger.exception("%s stage failed", stage_name)
        raise


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():

    logger.info("Starting NYC Taxi pipeline")

    try:
        # Stage 1: Bronze
        run_stage("Bronze", "bronze_ingestion")

        # Dependency: Silver runs only after Bronze succeeds
        run_stage("Silver", "silver_transformations")

        # Dependency: Gold runs only after Silver succeeds
        run_stage("Gold", "gold_analytics")

        logger.info("NYC Taxi pipeline completed successfully")

    except Exception:
        logger.exception(
            "Pipeline execution failed. Remaining dependent stages skipped."
        )
        raise


if __name__ == "__main__":
    main()
