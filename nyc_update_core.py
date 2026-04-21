import csv
from pathlib import Path

from nyc_open_data_utils import (
    fetch_dataset_metadata,
    format_epoch,
    output_paths,
    read_json,
    utc_now,
    write_json,
)


# Store the config keys used by this update checker.
DATASET_NAME_KEY = "name"
DATASET_RESOURCE_ID_KEY = "resource_id"
SOURCE_METADATA_KEY = "source_metadata"
NEW_DATASET_RAW_FILE_KEY = "raw_csv"

# Store the source metadata fields used for schema comparison.
SOURCE_COLUMN_FIELDS = (
    "fieldName",
    "name",
    "dataTypeName",
    "position",
    "description",
)

# Store the source fields used for raw data checks.
RAW_DATA_CHANGE_FIELDS = ("rows_updated_at", "columns")

# Store the source fields used for metadata checks.
METADATA_CHANGE_FIELDS = (
    "title",
    "description",
    "category",
    "tags",
    "view_last_modified",
)

# Return the source fields used by the update checker.
def summarize_source_metadata(metadata):
    metadata = metadata or {}
    columns = []

    for column in metadata.get("columns", []):
        column_summary = {}
        for key in SOURCE_COLUMN_FIELDS:
            column_summary[key] = column.get(key)
        columns.append(column_summary)

    return {
        "title": metadata.get("name"),
        "description": metadata.get("description"),
        "category": metadata.get("category"),
        "tags": metadata.get("tags"),
        "rows_updated_at": metadata.get("rowsUpdatedAt"),
        "view_last_modified": metadata.get("viewLastModified"),
        "column_count": len(metadata.get("columns", [])),
        "columns": columns,
    }


# Return the missing local files for one dataset.
def find_missing_local_files(paths):
    missing = []

    for name, path in paths.items():
        if name == SOURCE_METADATA_KEY:
            continue
        if not path.exists():
            missing.append(name)

    return missing


# Return True when this dataset is new.
def is_new_dataset_case(local_metadata_exists, missing_local_files):
    return not local_metadata_exists and NEW_DATASET_RAW_FILE_KEY in missing_local_files


# Return the update decision for one dataset.
def decide_refresh(
    local,
    remote,
    local_metadata_exists,
    missing_local_files,
):
    raw_reasons = []
    metadata_reasons = []

    if is_new_dataset_case(local_metadata_exists, missing_local_files):
        raw_reasons.append("new_dataset")
    elif not local_metadata_exists:
        raw_reasons.append("local_metadata_missing")
    for name in missing_local_files:
        raw_reasons.append(f"missing_{name}")

    for field in RAW_DATA_CHANGE_FIELDS:
        if local.get(field) != remote.get(field):
            raw_reasons.append(field)

    for field in METADATA_CHANGE_FIELDS:
        if local.get(field) != remote.get(field):
            metadata_reasons.append(field)

    if raw_reasons:
        return {
            "status": "raw_data_changed",
            "action": "refresh_raw_data",
            "needs_refresh": True,
            "changes_vs_local": raw_reasons,
        }

    if metadata_reasons:
        return {
            "status": "metadata_changed",
            "action": "refresh_metadata",
            "needs_refresh": True,
            "changes_vs_local": metadata_reasons,
        }

    return {
        "status": "unchanged",
        "action": "no_action",
        "needs_refresh": False,
        "changes_vs_local": [],
    }


# Return the status counts for one report.
def summarize_results(results):
    counts = {}

    for item in results:
        counts[item["status"]] = counts.get(item["status"], 0) + 1

    return counts


# Add readable time values to each report row.
def add_readable_times(results):
    time_fields = [
        ("local_rows_updated_at", "local_rows_updated_at_readable"),
        ("remote_rows_updated_at", "remote_rows_updated_at_readable"),
        ("local_view_last_modified", "local_view_last_modified_readable"),
        ("remote_view_last_modified", "remote_view_last_modified_readable"),
    ]
    updated = []

    for item in results:
        row = dict(item)
        for source_key, readable_key in time_fields:
            row[readable_key] = format_epoch(row.get(source_key))
        updated.append(row)

    return updated


# Check one dataset against local and remote metadata.
def check_dataset(
    dataset,
    local_metadata,
    local_metadata_exists,
    missing_local_files,
    metadata_fetcher=fetch_dataset_metadata,
):
    dataset_name = dataset[DATASET_NAME_KEY]
    local = summarize_source_metadata(local_metadata)
    new_dataset = is_new_dataset_case(local_metadata_exists, missing_local_files)
    remote_raw = {}
    remote = {}
    error = None

    try:
        remote_raw = metadata_fetcher(dataset[DATASET_RESOURCE_ID_KEY])
        remote = summarize_source_metadata(remote_raw)
    except Exception as exc:
        error = str(exc)

    if error:
        decision = {
            "status": "error",
            "action": "retry_check",
            "needs_refresh": False,
            "changes_vs_local": [],
        }
    else:
        decision = decide_refresh(
            local,
            remote,
            local_metadata_exists,
            missing_local_files,
        )

    return {
        "dataset_name": dataset_name,
        "resource_id": dataset[DATASET_RESOURCE_ID_KEY],
        "status": decision["status"],
        "action": decision["action"],
        "needs_refresh": decision["needs_refresh"],
        "changes_vs_local": decision["changes_vs_local"],
        "missing_local_files": missing_local_files,
        "is_new_dataset": new_dataset,
        "error": error,
        "local_rows_updated_at": local.get("rows_updated_at"),
        "remote_rows_updated_at": remote.get("rows_updated_at"),
        "local_view_last_modified": local.get("view_last_modified"),
        "remote_view_last_modified": remote.get("view_last_modified"),
        "local_column_count": local.get("column_count"),
        "remote_column_count": remote.get("column_count"),
        "remote_metadata": remote_raw,
    }


# Return the update result for one dataset.
def build_dataset_result(dataset, paths_builder, metadata_fetcher):
    paths = paths_builder(dataset[DATASET_NAME_KEY])
    local_metadata_path = paths[SOURCE_METADATA_KEY]
    local_metadata = read_json(local_metadata_path)
    local_metadata_exists = local_metadata_path.exists()
    missing_local_files = find_missing_local_files(paths)

    return check_dataset(
        dataset,
        local_metadata,
        local_metadata_exists,
        missing_local_files,
        metadata_fetcher,
    )


# Build the Markdown version of the report.
def build_markdown_report(checked_at, results):
    lines = [
        "# NYC Open Data Update Check",
        "",
        f"- Checked at: `{checked_at}`",
        f"- Datasets: `{len(results)}`",
        "",
        "| Dataset | Status | Action | Needs Refresh | New Dataset | Changes vs Local |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            f"| {item['dataset_name']} | {item['status']} | {item['action']} | "
            f"{item['needs_refresh']} | "
            f"{item['is_new_dataset']} | "
            f"{', '.join(item['changes_vs_local']) or 'none'} |"
        )
    return "\n".join(lines) + "\n"


# Write the CSV version of the report.
def write_report_csv(path, results):
    report_fields = [
        "dataset_name",
        "resource_id",
        "status",
        "action",
        "needs_refresh",
        "is_new_dataset",
        "changes_vs_local",
        "local_rows_updated_at",
        "remote_rows_updated_at",
        "local_view_last_modified",
        "remote_view_last_modified",
        "local_rows_updated_at_readable",
        "remote_rows_updated_at_readable",
        "local_view_last_modified_readable",
        "remote_view_last_modified_readable",
        "local_column_count",
        "remote_column_count",
        "error",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=report_fields)
        writer.writeheader()

        for item in results:
            row = dict(item)
            row["changes_vs_local"] = ",".join(item["changes_vs_local"])
            writer.writerow({field: row.get(field) for field in report_fields})


# Run the full update check for all datasets.
def run_update_check(
    datasets,
    report_json_path=None,
    report_csv_path=None,
    report_md_path=None,
    paths_builder=output_paths,
    metadata_fetcher=fetch_dataset_metadata,
):
    results = []

    for dataset in datasets:
        results.append(
            build_dataset_result(dataset, paths_builder, metadata_fetcher)
        )

    results.sort(key=lambda item: item["dataset_name"])
    results = add_readable_times(results)
    checked_at = utc_now()
    payload = {
        "checked_at": checked_at,
        "summary": summarize_results(results),
        "datasets": results,
    }

    if report_json_path is not None:
        write_json(report_json_path, payload)
    if report_csv_path is not None:
        write_report_csv(report_csv_path, results)
    if report_md_path is not None:
        report_md_path.write_text(
            build_markdown_report(checked_at, results),
            encoding="utf-8",
        )

    return payload
