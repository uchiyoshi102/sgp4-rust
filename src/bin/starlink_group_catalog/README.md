`starlink_group_catalog` maps a manifest of Starlink groups to Space-Track `SATCAT` rows and writes a catalog with per-group satellite membership.

Default inputs and outputs:

- Manifest: `data/starlink_group_manifest.csv`
- Combined catalog: `data/starlink_group_catalog.csv`
- Per-group catalogs: `starlink-groups/<group_slug>/catalog.csv`

Example:

```bash
cargo run --bin starlink_group_catalog -- --print-groups
cargo run --bin starlink_group_catalog -- --dry-run
SPACE_TRACK_IDENTITY='you@example.com' SPACE_TRACK_PASSWORD='secret' \
  cargo run --bin starlink_group_catalog
```
