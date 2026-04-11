`starlink_shell_map` builds a static shell-by-shell Starlink map from the local Group 1 and Group 4 GP history files.

It generates:

- `data/starlink_shell_map/index.html`
  A browser-viewable map with:
  - Group 1 / Group 4 satellite points
  - a per-cell visible-satellite heatmap
  - a diff mode for `Group 1 - Group 4`
  - animated motion using precomputed SGP4 samples and browser-side shortest-path interpolation
- `data/starlink_shell_map/data.js`
  The precomputed payload used by the HTML app
- `data/starlink_shell_map/starlink_shell_map.sqlite`
  A relational export of shells, groups, satellites, frames, positions, and per-cell visibility
- `data/starlink_shell_map/shell_summary.csv`
  A compact shell coverage summary

## Default behavior

- Uses `data/starlink_group_catalog.csv`
- Uses `starlink-group-1/` for Group 1 history
- Uses `starlink-group-4/` for Group 4 history
- Detects the latest common shell epoch and rounds it down to the sample grid
- Builds a 24-hour window centered on that timestamp
- Uses a 5-minute sampling interval by default
- Uses a 5-degree ground cell grid by default
- Drops satellites that are already decayed or whose latest TLE is older than 14 days at the selected center time

## Shell mapping

- Group 1 is mapped from catalog rows whose `group_family` is `phase1`
- Group 4 is mapped from catalog rows whose `group_family` is `group4`
- Group 1 uses a single local GP history source under `starlink-group-1/`
- Group 4 uses one GP history CSV per locally available subgroup directory under `starlink-group-4/`
- If a Group 4 subgroup is missing locally, it is still counted in `expected_group_count` but not in `available_group_count`

This means the app can render partial local coverage while still making the coverage gap explicit in the summary outputs.

## Implementation flow

The generator does the following:

1. Load `data/starlink_group_catalog.csv`
2. Build two shell datasets:
   - Group 1 from `phase1`
   - Group 4 from `group4`
3. Discover local GP history files
4. Scan each shell's history files to find the latest local epoch
5. Select the latest common shell epoch
6. Choose the latest TLE at or before the selected center time for each NORAD ID
7. Filter out:
   - satellites with `DECAY_DATE <= center_utc`
   - satellites whose selected TLE epoch is more than 14 days older than `center_utc`
8. Run SGP4 in Rust for every selected satellite at every sample timestamp
9. Convert TEME to ECEF using the local EOP file
10. Convert ECEF to geodetic latitude, longitude, and altitude
11. Count visible satellites per ground cell from each shell
12. Write:
   - `shell_summary.csv`
   - `starlink_shell_map.sqlite`
   - `data.js`
   - `index.html`

The browser does not run SGP4. All propagation is done ahead of time in Rust.

## Motion model

The generated `data.js` stores sampled positions only at the configured sample times.

- Rust computes the sample points with SGP4
- The browser interpolates between adjacent sample points
- The moving point is interpolated along the shortest path on the Earth surface between the two sampled geodetic positions
- The drawn segment between two sampled points is also rendered as a shortest-path curve instead of a straight line in `lat/lon`
- Altitude is interpolated linearly between the two adjacent samples

This keeps the browser light while avoiding per-frame SGP4 evaluation.

## Visibility model

Visibility is computed per ground cell center.

- Each cell is a latitude/longitude rectangle
- The cell center is converted into a unit vector
- Each propagated satellite position is in ECEF
- A satellite is counted as visible for that cell when the line of sight is above the local horizon

The current test is a simple spherical Earth horizon test:

- visible when `r_sat . u_cell > R_earth`

where:

- `r_sat` is the satellite ECEF position vector
- `u_cell` is the unit vector from Earth center toward the cell center
- `R_earth` is the Earth radius used by the app

This is intentionally simple and fast. It is appropriate for relative shell comparison, but it is not a high-fidelity observer model with terrain, atmosphere, or elevation masks.

## Relational database

The generated SQLite file is:

- `data/starlink_shell_map/starlink_shell_map.sqlite`

Current schema:

```sql
CREATE TABLE metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE shells (
  shell_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  color TEXT NOT NULL,
  expected_group_count INTEGER NOT NULL,
  available_group_count INTEGER NOT NULL,
  expected_satellite_count INTEGER NOT NULL,
  available_satellite_count INTEGER NOT NULL,
  latest_epoch_utc TEXT
);

CREATE TABLE groups (
  group_slug TEXT PRIMARY KEY,
  shell_id TEXT NOT NULL,
  group_name TEXT NOT NULL,
  launch_date TEXT NOT NULL,
  satellite_count INTEGER NOT NULL,
  history_path TEXT
);

CREATE TABLE satellites (
  norad_cat_id TEXT PRIMARY KEY,
  shell_id TEXT NOT NULL,
  group_slug TEXT NOT NULL,
  satname TEXT NOT NULL,
  object_name TEXT NOT NULL,
  object_id TEXT NOT NULL,
  launch_date TEXT NOT NULL,
  tle_epoch_utc TEXT NOT NULL,
  tle_creation_date_utc TEXT NOT NULL,
  decay_date_utc TEXT NOT NULL,
  tle_line1 TEXT NOT NULL,
  tle_line2 TEXT NOT NULL
);

CREATE TABLE frames (
  frame_index INTEGER PRIMARY KEY,
  frame_utc TEXT NOT NULL
);

CREATE TABLE frame_samples (
  frame_index INTEGER NOT NULL,
  norad_cat_id TEXT NOT NULL,
  shell_id TEXT NOT NULL,
  lat_deg REAL NOT NULL,
  lon_deg REAL NOT NULL,
  altitude_km REAL NOT NULL,
  x_km REAL NOT NULL,
  y_km REAL NOT NULL,
  z_km REAL NOT NULL,
  PRIMARY KEY (frame_index, norad_cat_id)
);

CREATE TABLE cell_visibility (
  frame_index INTEGER NOT NULL,
  cell_index INTEGER NOT NULL,
  lat_min_deg REAL NOT NULL,
  lat_max_deg REAL NOT NULL,
  lon_min_deg REAL NOT NULL,
  lon_max_deg REAL NOT NULL,
  group1_visible INTEGER NOT NULL,
  group4_visible INTEGER NOT NULL,
  diff_visible INTEGER NOT NULL,
  PRIMARY KEY (frame_index, cell_index)
);

CREATE INDEX idx_frame_samples_shell
  ON frame_samples (shell_id, frame_index);

CREATE INDEX idx_cell_visibility_frame
  ON cell_visibility (frame_index);
```

### Table roles

- `metadata`
  Run-level metadata such as generation time, chosen `center_utc`, and `latest_common_utc`
- `shells`
  One row per rendered shell, including expected vs locally available coverage
- `groups`
  One row per launch group included in the shell definition
- `satellites`
  One row per NORAD satellite kept after filtering and TLE selection
- `frames`
  One row per animation frame
- `frame_samples`
  One row per `(frame, satellite)` sample
- `cell_visibility`
  One row per `(frame, cell)` visibility aggregate

### Logical relationships

- `groups.shell_id -> shells.shell_id`
- `satellites.shell_id -> shells.shell_id`
- `satellites.group_slug -> groups.group_slug`
- `frame_samples.frame_index -> frames.frame_index`
- `frame_samples.norad_cat_id -> satellites.norad_cat_id`
- `cell_visibility.frame_index -> frames.frame_index`

These relations are not currently declared as SQLite foreign-key constraints. They are maintained by the generator logic.

### Why this schema

This layout separates three concerns cleanly:

- shell/group metadata
- per-satellite selected TLE state
- time-varying propagated state and time-varying cell aggregates

It supports:

- shell-level summaries
- launch-group filtering
- replaying one frame or one satellite from SQL
- plotting shell visibility heatmaps directly from SQL
- comparing shell coverage over time without reparsing CSV or rerunning SGP4

### Example queries

List shells and local coverage:

```bash
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite \
  'select shell_id, expected_group_count, available_group_count, expected_satellite_count, available_satellite_count from shells;'
```

List the selected satellite count per shell:

```bash
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite \
  'select shell_id, count(*) from satellites group by shell_id;'
```

Inspect frame timestamps:

```bash
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite \
  'select frame_index, frame_utc from frames limit 10;'
```

Inspect one satellite trajectory:

```bash
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite \
  "select frame_index, lat_deg, lon_deg, altitude_km
   from frame_samples
   where norad_cat_id = '44713'
   order by frame_index;"
```

Inspect one frame's visibility cells:

```bash
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite \
  'select cell_index, group1_visible, group4_visible, diff_visible
   from cell_visibility
   where frame_index = 0
   order by cell_index
   limit 20;'
```

Find the cells where Group 1 exceeds Group 4 the most at a frame:

```bash
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite \
  'select lat_min_deg, lat_max_deg, lon_min_deg, lon_max_deg, diff_visible
   from cell_visibility
   where frame_index = 0
   order by diff_visible desc
   limit 20;'
```

## Current local output example

On the current local workspace, the generated database contains:

- metadata:
  - `generated_utc`
  - `center_utc`
  - `latest_common_utc`
- shell summary:
  - `group1`: expected groups `28`, available groups `28`, expected satellites `1665`, available satellites `1375`
  - `group4`: expected groups `32`, available groups `6`, expected satellites `1605`, available satellites `293`

This reflects the local filesystem state, not the complete Starlink constellation.

## Important assumptions and limitations

- Group 1 is treated as `phase1`, not as a single official Starlink shell designation from Space-Track
- Group 4 coverage is only as complete as the subgroup directories currently present locally
- The selected TLE for each NORAD ID is the latest one at or before the chosen center time
- The current observer model is cell-center visibility only
- The current database stores sampled propagated positions, not raw GP history
- The browser interpolation is for visualization only; authoritative positions are the stored sampled frames

## Examples

Run with defaults:

```bash
cargo run --bin starlink_shell_map
```

Run with explicit parameters:

```bash
cargo run --bin starlink_shell_map -- \
  --center-utc 2024-08-02T22:00:00Z \
  --hours 12 \
  --step-minutes 5 \
  --cell-degrees 5
```

Open the generated app:

```bash
open data/starlink_shell_map/index.html
```

Inspect the SQLite output:

```bash
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite '.tables'
sqlite3 data/starlink_shell_map/starlink_shell_map.sqlite '.schema'
```
