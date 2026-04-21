# Atlas Profiler Evaluation Report

## Purpose

This report gives a quick summary of what Atlas does well and what Atlas does badly on the datasets in this project.

It uses:

- plain Atlas output
- Atlas plus wrapper output

Datasets used:

- `bike_routes`
- `child_care`
- `commonplace`
- `farmers_markets`
- `hydrants`
- `libraries`
- `nyc311`
- `ped_counts`
- `play_areas`
- `street_trees`
- `wifi_hotspots`

---

## Part 1. Atlas By Itself

### What Atlas Does Well

#### 1. Geometry columns

Atlas is strong on direct geometry fields.

Examples:

- `ped_counts.the_geom -> point`
- `bike_routes.the_geom -> multi-line`
- `play_areas.multipolygon -> multi-polygon`
- `hydrants.the_geom -> point`
- `libraries.the_geom -> point`

#### 2. Address fields

Atlas is strong on explicit address text.

Examples:

- `nyc311.incident_address -> address`
- `nyc311.street_name -> address`
- `nyc311.cross_street_1 -> address`
- `nyc311.cross_street_2 -> address`
- `libraries.streetname -> address`
- `child_care.address -> address`

#### 3. Standard longitude fields

Atlas is generally strong on direct longitude columns.

Examples:

- `child_care.longitude -> longitude`
- `farmers_markets.longitude -> longitude`
- `hydrants.longitude -> longitude`
- `libraries.longitude -> longitude`
- `wifi_hotspots.longitude -> longitude`

### What Atlas Does Badly

#### 1. Missed admin fields

Atlas often misses admin-like columns.

Examples:

- `nyc311.community_board -> None`
- `nyc311.council_district -> None`
- `nyc311.police_precinct -> None`
- `libraries.city -> None`
- `street_trees.boroname -> None`
- `street_trees.nta -> None`
- `street_trees.nta_name -> None`
- `wifi_hotspots.borocd -> None`
- `wifi_hotspots.boroname -> None`

#### 2. Missed NYC identifiers

Atlas often misses `bin`, `bbl`, and ZIP style fields.

Examples:

- `nyc311.bbl -> None`
- `street_trees.bbl -> None`
- `libraries.bin -> None`
- `child_care.bbl -> None`
- `child_care.zipcode -> None`
- `play_areas.zipcode -> None`

#### 3. Wrong coordinate assignments

Atlas sometimes confuses identifiers or codes with coordinates.

Examples:

- `commonplace.bin -> x_coord`
- `child_care.bin -> x_coord`
- `street_trees.boro_ct -> x_coord`
- `commonplace.primaryaddresspointid -> x_coord`

#### 4. Missed projected coordinates

Atlas misses some projected coordinate columns.

Examples:

- `nyc311.y_coordinate_state_plane -> None`
- `street_trees.y_sp -> None`
- `libraries.x -> None`
- `libraries.y -> None`
- `wifi_hotspots.y -> None`

#### 5. False admin labels

Atlas sometimes assigns admin labels where it should not.

Examples:

- `commonplace.facility_domains -> borough_code`
- `commonplace.modified_by -> borough_code`
- `bike_routes.gwsystem -> borough`

### Atlas Summary

Plain Atlas total:

- `61` geo-labeled columns across 11 datasets

Best areas:

- geometry
- address
- standard longitude fields

Weak areas:

- admin-like fields
- `bin`
- `bbl`
- ZIP fields
- projected coordinates
- false admin labels on coded fields

---

## Part 2. Atlas Plus Wrapper

### What The Wrapper Adds

The wrapper mainly adds five kinds of fixes:

1. fix `bin`
2. fix `bbl`
3. fix ZIP fields
4. fix coordinate mistakes
5. recover missed admin and city fields

It also clears some bad Atlas labels.

### Main Wrapper Fixes

Examples:

- `commonplace.bin: x_coord -> bin`
- `child_care.bin: x_coord -> bin`
- `nyc311.bbl: None -> bbl`
- `street_trees.bbl: None -> bbl`
- `nyc311.incident_zip: None -> zip5`
- `play_areas.zipcode: None -> zip5`
- `nyc311.y_coordinate_state_plane: None -> y_coord`
- `libraries.x: None -> x_coord`
- `libraries.y: None -> y_coord`
- `libraries.city: None -> city`
- `wifi_hotspots.borocd: None -> borough_code`
- `street_trees.state: None -> state`

### Main Wrapper Cleanups

Examples:

- `bike_routes.gwsystem: borough -> None`
- `commonplace.facility_domains: borough_code -> None`
- `commonplace.modified_by: borough_code -> None`
- `commonplace.primaryaddresspointid: x_coord -> None`
- `wifi_hotspots.location_lat_long: longitude -> None`

### Result Comparison

| Output | Geo-labeled columns |
| --- | ---: |
| Plain Atlas | 61 |
| Atlas + Wrapper | 96 |

Net gain:

- `+35` geo-labeled columns

Wrapper changes:

- `42` added labels
- `7` cleared labels
- `3` corrected labels

### Dataset Level Comparison

| Dataset | Plain Atlas | Atlas + Wrapper |
| --- | ---: | ---: |
| `bike_routes` | 3 | 3 |
| `child_care` | 6 | 9 |
| `commonplace` | 6 | 3 |
| `farmers_markets` | 4 | 5 |
| `hydrants` | 3 | 4 |
| `libraries` | 6 | 11 |
| `nyc311` | 13 | 21 |
| `ped_counts` | 5 | 5 |
| `play_areas` | 3 | 7 |
| `street_trees` | 5 | 14 |
| `wifi_hotspots` | 7 | 14 |

---

## Final Summary

Atlas already does well on:

- geometry
- address
- direct longitude fields

Atlas still does badly on:

- admin-like fields
- `bin`
- `bbl`
- ZIP fields
- projected coordinates
- false admin labels on coded fields

The wrapper improves the final result by:

- adding missed fields
- correcting wrong labels
- removing bad labels
