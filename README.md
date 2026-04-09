# sgp4-rust

This repository contains the original `sgp4` code plus Starlink-oriented utilities for:

- `starlink_space_track_catalog`: legacy phase1 catalog fetcher
- `starlink_group_catalog`: manifest-driven group catalog fetcher
- `starlink_group_gp_history`: full-history GP/TLE downloader per group
- `starlink_group_timelapse`: launch/decay summary and HTML viewer with group filters
- `starlink_shell_map`: Group 1 / Group 4 shell map, visibility heatmap, and SQLite export

## Starlink group pipeline

The current Starlink workflow is:

```bash
cargo run --bin starlink_group_catalog
cargo run --bin starlink_group_gp_history
cargo run --bin starlink_group_timelapse
cargo run --bin starlink_shell_map
```

This produces:

- `data/starlink_group_catalog.csv`
- `starlink-groups/<group_slug>/starlink_gp_history.csv`
- `data/starlink_group_timelapse.html`
- `data/starlink_shell_map/index.html`
- `data/starlink_shell_map/starlink_shell_map.sqlite`

## Important files

- [`data/starlink_group_manifest.csv`](/Users/yoshikiuchida/github/sgp4-rust/data/starlink_group_manifest.csv)
  Maps `launch_date -> group_name`.
  Space-Track does not return Wikipedia-style group labels such as `Group 5-6`, so this file is the classification table.
- [`data/starlink_group_catalog.csv`](/Users/yoshikiuchida/github/sgp4-rust/data/starlink_group_catalog.csv)
  Generated from Space-Track `SATCAT`.
  This is the actual per-group satellite list with `norad_cat_id` and `satname`.
- `starlink-groups/<group_slug>/starlink_gp_history.csv`
  Generated from Space-Track `GP_HISTORY`.
  This is the merged TLE history used by the timelapse step.

The default manifest covers the phase 1 launches plus the Wikipedia launch groups through `Group 5-15`.

## Commands

Build the per-group Starlink catalog:

```bash
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
cargo run --bin starlink_group_catalog
```

Preview the manifest/query without logging in:

```bash
cargo run --bin starlink_group_catalog -- --dry-run
```

Download TLE history for all groups:

```bash
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
cargo run --bin starlink_group_gp_history
```

Download one group first as a smoke test:

```bash
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
cargo run --bin starlink_group_gp_history -- --group group-5-6
```

Resume after an interrupted run:

```bash
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
cargo run --bin starlink_group_gp_history -- --resume
```

Generate the launch/decay viewer after TLE download:

```bash
cargo run --bin starlink_group_timelapse
```

Generate the shell map app and relational export from the local Group 1 / Group 4 histories:

```bash
cargo run --bin starlink_shell_map
```

## GP history notes

- The downloader tries a single ZIP request first.
- If Space-Track does not return a ZIP, it automatically falls back to batch CSV downloads.
- If a batch response is not valid `GP_HISTORY` CSV, the downloader now splits that batch into smaller chunks and retries automatically.
- `starlink_gp_history_full_history.csv` is the explicit output for a full-history run.
- `starlink_gp_history.csv` is the stable latest filename used by later steps.

## Output layout

Per group, output is written under `starlink-groups/<group_slug>/`.

Typical files are:

- `catalog.csv`
- `gp_history_urls_full_history.csv`
- `starlink_gp_history_full_history.csv`
- `starlink_gp_history.csv`
- `batches/`

## Git

Large generated files are ignored by `.gitignore`, including:

- `starlink-groups/`
- `starlink-group-1/`
- `target/`
- generated CSV/HTML exports under `data/`
