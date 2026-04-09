`starlink_group_gp_history` downloads `GP_HISTORY` per Starlink group using the catalog written by `starlink_group_catalog`.

Default behavior:

- Reads `data/starlink_group_catalog.csv`
- Writes group outputs under `starlink-groups/<group_slug>/`
- Uses full history by default, so the requests are not time-limited
- Writes a stable merged file to `starlink-groups/<group_slug>/starlink_gp_history.csv`
- Supports `--resume` to skip groups that already have `starlink_gp_history.csv`
- Tries ZIP first, then falls back to batch CSV
- If a batch response is invalid, it recursively splits that batch into smaller requests

Examples:

```bash
cargo run --bin starlink_group_gp_history -- --dry-run
cargo run --bin starlink_group_gp_history -- --group phase1-launch-01 --dry-run
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
  cargo run --bin starlink_group_gp_history
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
  cargo run --bin starlink_group_gp_history -- --resume
```
