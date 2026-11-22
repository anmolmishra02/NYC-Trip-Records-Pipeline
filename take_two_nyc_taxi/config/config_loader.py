import yaml
from pathlib import Path


def load_config():
    project_root = Path(__file__).resolve().parents[1]
    config_path = project_root / "config" / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}"
        )

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError("Invalid or empty configuration file.")

    return config
