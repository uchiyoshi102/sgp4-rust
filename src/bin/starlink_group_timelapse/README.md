`starlink_group_timelapse` summarizes per-group `GP_HISTORY` output and writes:

- `data/starlink_group_satellite_windows.csv`
- `data/starlink_group_stats.csv`
- `data/starlink_group_timelapse.html`

The HTML viewer supports:

- time-slider playback
- family/group filters
- active-satellite counts by group
- decay order inspection
- per-group satellite tables

Example:

```bash
cargo run --bin starlink_group_timelapse
```
