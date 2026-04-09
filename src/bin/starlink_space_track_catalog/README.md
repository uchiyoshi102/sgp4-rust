`starlink_space_track_catalog` uses the Wikipedia launch table and Space-Track SATCAT data to list `NORAD_CAT_ID` and `SATNAME` for rows whose `SATNAME` contains `STARLINK`.

The fixed launch-date set comes from Wikipedia `List of Starlink and Starshield launches`, specifically No.2 `Launch 1` through No.29 `Launch 28`, normalized to `YYYY-MM-DD`.

Examples:

```bash
cargo run --bin starlink_space_track_catalog -- --print-launches
cargo run --bin starlink_space_track_catalog -- --dry-run
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
  cargo run --bin starlink_space_track_catalog
```

By default, the command writes `data/starlink_satcat.csv`.

If `SPACE_TRACK_IDENTITY` and `SPACE_TRACK_PASSWORD` are not set, the command prompts for them.

