
from pathlib import Path
import os
import logging

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

logging.info(f"Project root: {PROJECT_ROOT}")

def main():
    logging.info("Starting NYC Taxi pipeline")

    # Execute notebooks in sequence
    dbutils.notebook.run(
        str(PROJECT_ROOT / "notebooks" / "bronze_ingestion"),
        0
    )

    dbutils.notebook.run(
        str(PROJECT_ROOT / "notebooks" / "silver_transformations"),
        0
    )

    dbutils.notebook.run(
        str(PROJECT_ROOT / "notebooks" / "gold_analytics"),
        0
    )

    logging.info("NYC Taxi pipeline completed successfully")

if __name__ == "__main__":
    main()
