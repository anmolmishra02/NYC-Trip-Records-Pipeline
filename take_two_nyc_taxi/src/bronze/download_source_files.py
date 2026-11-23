from pathlib import Path
from urllib.request import Request, urlopen
from shutil import copyfileobj

from config.config_loader import load_config


def download_file(url, destination):
    """Download a source file unless a non-empty copy already exists."""

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and destination.stat().st_size > 0:
        print(f"Already exists, skipping: {destination.name}")
        return

    temp_file = destination.with_suffix(destination.suffix + ".part")

    try:
        print(f"Downloading: {destination.name}")

        request = Request(
            url,
            headers={"User-Agent": "NYC-Taxi-Data-Pipeline/1.0"}
        )

        with urlopen(request, timeout=120) as response:
            with temp_file.open("wb") as output:
                copyfileobj(response, output)

        if not temp_file.exists() or temp_file.stat().st_size == 0:
            raise RuntimeError(
                f"Download produced an empty file: {destination.name}"
            )

        # Rename only after the download completes successfully.
        temp_file.replace(destination)
        print(
            f"Downloaded: {destination.name} "
            f"({destination.stat().st_size:,} bytes)"
        )

    except Exception:
        if temp_file.exists():
            temp_file.unlink()
        raise


def download_source_files():
    """Download configured Yellow, Green and Taxi Zone source files."""

    config = load_config()

    storage = config["storage"]
    source = config["source"]
    months = config["months"]

    base_path = Path(
        f"/Volumes/{storage['catalog']}/"
        f"{storage['schema']}/"
        f"{storage['volume']}"
    )

    yellow_path = base_path / source["yellow_folder"]
    green_path = base_path / source["green_folder"]

    # Download the configured monthly trip files.
    for month in months:
        for service_type, folder in (
            ("yellow", yellow_path),
            ("green", green_path),
        ):
            file_name = f"{service_type}_tripdata_{month}.parquet"
            url = (
                "https://d37ci6vzurychx.cloudfront.net/"
                f"trip-data/{file_name}"
            )

            download_file(url, folder / file_name)

    # Download the Taxi Zone Lookup file.
    lookup_file = source["zone_lookup_file"]
    lookup_url = (
        "https://d37ci6vzurychx.cloudfront.net/"
        "misc/taxi_zone_lookup.csv"
    )

    download_file(lookup_url, base_path / lookup_file)

    print("\nAll configured source files are available in the Volume.")
