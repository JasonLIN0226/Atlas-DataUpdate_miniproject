import csv
from datetime import datetime


TIME_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
]


# Build temporal metadata from one dataset and its Atlas metadata.
def build_temporal_metadata(data_path, atlas_metadata):
    temporal_columns = find_temporal_columns(atlas_metadata)
    if not temporal_columns:
        return empty_temporal_metadata()

    column_ranges = []
    month_coverage, month_row_counts = scan_temporal_row_counts(data_path, temporal_columns)
    for name in temporal_columns:
        column_range = scan_temporal_column(data_path, name)
        if column_range:
            column_ranges.append(column_range)

    if not column_ranges:
        return empty_temporal_metadata(temporal_columns)

    starts = [item["start"] for item in column_ranges]
    ends = [item["end"] for item in column_ranges]
    return {
        "has_temporal_data": True,
        "temporal_columns": [item["name"] for item in column_ranges],
        "temporal_start": min(starts),
        "temporal_end": max(ends),
        "column_ranges": column_ranges,
        "month_coverage": month_coverage,
        "month_row_counts": month_row_counts,
    }


# Return empty temporal metadata.
def empty_temporal_metadata(temporal_columns=None):
    return {
        "has_temporal_data": False,
        "temporal_columns": temporal_columns or [],
        "temporal_start": None,
        "temporal_end": None,
        "column_ranges": [],
        "month_coverage": {},
        "month_row_counts": {},
    }


# Find temporal columns from Atlas metadata.
def find_temporal_columns(atlas_metadata):
    columns = []
    for column in atlas_metadata.get("columns", []):
        if is_temporal_column(column):
            columns.append(column["name"])
    return columns


# Check whether one Atlas column is temporal.
def is_temporal_column(column):
    for value in column.get("semantic_types", []):
        text = str(value).lower()
        if "date" in text or "time" in text:
            return True

    structural_type = str(column.get("structural_type") or "").lower()
    return "date" in structural_type or "time" in structural_type


# Scan one temporal column from the raw CSV.
def scan_temporal_column(data_path, column_name):
    first_value = None
    last_value = None
    month_coverage = {}
    month_row_counts = {}

    with open(data_path, "r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        if column_name not in (reader.fieldnames or []):
            return None

        for row in reader:
            raw_value = (row.get(column_name) or "").strip()
            parsed = parse_datetime_value(raw_value)
            if parsed is None:
                continue

            if first_value is None or parsed < first_value:
                first_value = parsed
            if last_value is None or parsed > last_value:
                last_value = parsed
            year = str(parsed.year)
            month_coverage.setdefault(year, set()).add(parsed.month)
            month_row_counts.setdefault(year, {})
            month_row_counts[year][parsed.month] = month_row_counts[year].get(parsed.month, 0) + 1

    if first_value is None or last_value is None:
        return None

    return {
        "name": column_name,
        "start": first_value.isoformat(),
        "end": last_value.isoformat(),
        "month_coverage": normalize_month_coverage(month_coverage),
        "month_row_counts": normalize_month_row_counts(month_row_counts),
    }


# Scan the dataset once to count rows for covered months.
def scan_temporal_row_counts(data_path, temporal_columns):
    month_coverage = {}
    month_row_counts = {}

    with open(data_path, "r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        available_columns = [name for name in temporal_columns if name in fieldnames]
        if not available_columns:
            return {}, {}

        for row in reader:
            row_months = set()
            for name in available_columns:
                raw_value = (row.get(name) or "").strip()
                parsed = parse_datetime_value(raw_value)
                if parsed is None:
                    continue
                row_months.add((str(parsed.year), parsed.month))

            for year, month in row_months:
                month_coverage.setdefault(year, set()).add(month)
                month_row_counts.setdefault(year, {})
                month_row_counts[year][month] = month_row_counts[year].get(month, 0) + 1

    return normalize_month_coverage(month_coverage), normalize_month_row_counts(month_row_counts)


# Sort month coverage into a stable metadata format.
def normalize_month_coverage(month_coverage):
    normalized = {}
    for year in sorted(month_coverage, key=int):
        months = sorted(int(month) for month in month_coverage[year])
        if months:
            normalized[str(year)] = months
    return normalized


# Sort month row counts into a stable metadata format.
def normalize_month_row_counts(month_row_counts):
    normalized = {}
    for year in sorted(month_row_counts, key=int):
        counts = {}
        for month in sorted(month_row_counts[year], key=int):
            counts[str(int(month))] = int(month_row_counts[year][month])
        if counts:
            normalized[str(year)] = counts
    return normalized


# Parse one date or datetime string.
def parse_datetime_value(value):
    if not value:
        return None

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return None
