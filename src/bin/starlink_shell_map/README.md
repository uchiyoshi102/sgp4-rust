`starlink_shell_map` now builds a Starlink shell coverage app from Space-Track current TLEs:

- `data/starlink_space_track_current.tle`

It no longer needs a hand-built launch/group CSV. Instead it:

- groups satellites into shell layers by current inclination and mean altitude
- computes per-cell visible-satellite counts for every shell
- exposes overlap depth, exclusive coverage, and a decay proxy
- lets the browser remove one shell and highlight where other shells still cover the same cells

Default flow:

```bash
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
cargo run --bin starlink_space_track_tles
cargo run --bin starlink_shell_map
```

Outputs:

- `data/starlink_shell_map/data.js`
- `data/starlink_shell_map/shell_summary.csv`
- `data/starlink_shell_map/index.html`

The checked-in `index.html` is now the UI template. The Rust generator writes `data.js` and `shell_summary.csv`.
