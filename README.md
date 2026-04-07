# sgp4-rust

Starlink utilities in this repository now include:

- `starlink_space_track_catalog`: legacy phase1 catalog fetcher
- `starlink_group_catalog`: manifest-driven group catalog fetcher
- `starlink_group_gp_history`: full-history GP/TLE downloader per group
- `starlink_group_timelapse`: launch/decay summary and HTML viewer with group filters

Typical flow:

```bash
cargo run --bin starlink_group_catalog
cargo run --bin starlink_group_gp_history
cargo run --bin starlink_group_timelapse
```

The default manifest in `data/starlink_group_manifest.csv` now covers the phase 1 launches plus the Wikipedia launch groups through `Group 5-15`.
