import hashlib
from pathlib import Path

from nyc_open_data_utils import (
    count_rows,
    fetch_dataset_metadata,
    fetch_url,
    output_paths,
    read_json,
    write_json,
)


# Store the settings used by this source refresh core.
DEFAULT_LIMIT = 5000
NYC_OPEN_DATA_BASE_URL = "https://data.cityofnewyork.us"
CSV_RESOURCE_PATH = "/resource/{resource_id}.csv?$limit={limit}"
METADATA_TIMEOUT_SECONDS = 20
CSV_TIMEOUT_SECONDS = 60

# Store the path keys used by this source refresh core.
SOURCE_METADATA_PATH_KEY = "source_metadata"
RAW_CSV_PATH_KEY = "raw_csv"


# Return the CSV row limit for one refresh.
def infer_csv_limit(path, default_limit=DEFAULT_LIMIT):
    return count_rows(path) or default_limit


# Download the CSV for one dataset.
def fetch_dataset_csv(resource_id, limit, timeout=CSV_TIMEOUT_SECONDS):
    url = NYC_OPEN_DATA_BASE_URL + CSV_RESOURCE_PATH.format(resource_id=resource_id, limit=limit)
    return fetch_url(url, timeout)


# Return file stats for one local file.
def file_stats(path):
    if not path.exists():
        return None

    raw = path.read_bytes()
    return {
        "size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "row_count": count_rows(path),
    }


# Refresh the local source files for one dataset.
def refresh_source_assets(
    dataset,
    report_item,
    paths,
    default_limit=DEFAULT_LIMIT,
):
    source_metadata_path = paths[SOURCE_METADATA_PATH_KEY]
    raw_csv_path = paths[RAW_CSV_PATH_KEY]
    before_metadata = read_json(source_metadata_path)
    before_csv = file_stats(raw_csv_path)
    remote_metadata = fetch_dataset_metadata(
        dataset["resource_id"],
        timeout=METADATA_TIMEOUT_SECONDS,
    )
    write_json(source_metadata_path, remote_metadata)

    csv_limit = None
    if report_item["action"] == "refresh_raw_data":
        csv_limit = infer_csv_limit(raw_csv_path, default_limit)
        csv_bytes = fetch_dataset_csv(remote_metadata["_resolved_view_id"], csv_limit)
        raw_csv_path.write_bytes(csv_bytes)

    current_csv = file_stats(raw_csv_path)

    return {
        "paths": paths,
        "before_metadata": before_metadata,
        "before_csv": before_csv,
        "remote_metadata": remote_metadata,
        "csv_limit": csv_limit,
        "current_csv": current_csv,
    }


# Split the report into pending and errored datasets.
def split_refresh_targets(report):
    pending = []
    errored = []

    for item in report.get("datasets", []):
        status = item.get("status")
        if status == "error":
            errored.append(item["dataset_name"])
        elif item.get("needs_refresh"):
            pending.append(item)

    return pending, errored


# Refresh the source files for all changed datasets.
def refresh_changed_datasets(
    datasets,
    report,
    paths_builder=output_paths,
    default_limit=DEFAULT_LIMIT,
):
    dataset_lookup = {item["name"]: item for item in datasets}
    pending, _ = split_refresh_targets(report)
    refreshed = []

    for item in pending:
        dataset_name = item["dataset_name"]
        dataset = dataset_lookup[dataset_name]
        paths = paths_builder(dataset["name"])
        source_refresh = refresh_source_assets(
            dataset=dataset,
            report_item=item,
            paths=paths,
            default_limit=default_limit,
        )
        refreshed.append(
            {
                "dataset": dataset,
                "report_item": item,
                **source_refresh,
            }
        )

    return refreshed
