`starlink_group_1_gp_history` downloads GP history from Space-Track for the NORAD catalog IDs listed in `data/starlink_satcat.csv`.

Output layout:

- It first tries a single `format/zip` download
- If the ZIP URL is too long or the ZIP path does not return a ZIP file, it falls back to batch CSV downloads
- Invalid batch responses are split into smaller recursive requests automatically
- Full history is the default, so the merged CSV goes into `starlink-group-1/starlink_gp_history_full_history.csv`
- A stable latest copy is also written to `starlink-group-1/starlink_gp_history.csv`
- The URL manifest goes into `starlink-group-1/gp_history_urls_full_history.csv`

The implementation follows the same Space-Track login/session flow as the provided Python example, using `curl` from Rust. ZIP extraction uses the system `unzip` command. Batch CSV download tries multiple GP_HISTORY URL variants and treats `""` as an empty batch instead of an immediate failure.

Examples:

```bash
cargo run --bin starlink_group_1_gp_history -- --dry-run
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
  cargo run --bin starlink_group_1_gp_history
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
  cargo run --bin starlink_group_1_gp_history -- --chunk-size 20
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
  cargo run --bin starlink_group_1_gp_history -- --start-date 2021-07-15 --end-date 2024-08-02
```
