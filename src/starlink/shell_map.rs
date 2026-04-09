use crate::sgp4::sgp4::{gstime, jday};
use crate::sgp4::tle::TLE;
use crate::starlink::csv::{build_header_map, csv_escape, get_field, parse_csv_line, require_column};
use crate::starlink::manifest::{load_catalog_rows, CatalogRow};
use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Timelike, Utc};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const GROUP1_SHELL_ID: &str = "group1";
const GROUP4_SHELL_ID: &str = "group4";
const GROUP1_DISPLAY_NAME: &str = "Group 1";
const GROUP4_DISPLAY_NAME: &str = "Group 4";
const GROUP1_COLOR: &str = "#66d9ff";
const GROUP4_COLOR: &str = "#ffb35c";
const EARTH_RADIUS_KM: f64 = 6378.137;
const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / (180.0 * 3600.0);
const MJD_OFFSET: f64 = 2_400_000.5;
const WGS84_A_KM: f64 = 6378.137;
const WGS84_F: f64 = 1.0 / 298.257223563;
const MAX_TLE_AGE_DAYS: i64 = 14;

#[derive(Debug)]
struct Config {
    catalog: PathBuf,
    group1_history: PathBuf,
    group4_history_root: PathBuf,
    eop: PathBuf,
    output_dir: PathBuf,
    center_utc: Option<DateTime<Utc>>,
    hours: i64,
    step_minutes: i64,
    cell_degrees: i32,
}

#[derive(Clone, Debug)]
struct HistoryInput {
    path: PathBuf,
    norad_ids: HashSet<String>,
}

#[derive(Clone, Debug)]
struct ShellInfo {
    shell_id: String,
    display_name: String,
    color: String,
    expected_group_count: usize,
    available_group_count: usize,
    expected_satellite_count: usize,
    available_satellite_count: usize,
    latest_epoch_text: Option<String>,
    missing_groups: Vec<String>,
}

#[derive(Clone, Debug)]
struct ShellDataset {
    shell_id: String,
    display_name: String,
    color: String,
    groups: Vec<GroupInfo>,
    inputs: Vec<HistoryInput>,
    missing_groups: Vec<String>,
}

#[derive(Clone, Debug)]
struct GroupInfo {
    group_slug: String,
    group_name: String,
    launch_date: String,
    satellite_count: usize,
    history_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct LatestTleRecord {
    shell_id: String,
    display_name: String,
    color: String,
    group_slug: String,
    group_name: String,
    launch_date: String,
    norad_cat_id: String,
    satname: String,
    object_name: String,
    object_id: String,
    epoch_text: String,
    creation_date_text: String,
    decay_date_text: String,
    tle_line1: String,
    tle_line2: String,
}

#[derive(Clone, Debug)]
struct SatelliteTrack {
    record: LatestTleRecord,
    samples: Vec<PositionSample>,
}

#[derive(Clone, Copy, Debug)]
struct PositionSample {
    lat_deg: f64,
    lon_deg: f64,
    altitude_km: f64,
    x_km: f64,
    y_km: f64,
    z_km: f64,
}

#[derive(Clone, Copy, Debug)]
struct Cell {
    index: usize,
    lat_min_deg: f64,
    lat_max_deg: f64,
    lon_min_deg: f64,
    lon_max_deg: f64,
    unit_x: f64,
    unit_y: f64,
    unit_z: f64,
}

#[derive(Clone, Debug)]
struct FrameCellVisibility {
    group1_counts: Vec<u16>,
    group4_counts: Vec<u16>,
}

#[derive(Clone, Copy, Debug)]
struct EopRecord {
    mjd_utc: f64,
    xp_arcsec: f64,
    yp_arcsec: f64,
    ut1_utc_seconds: f64,
    lod_seconds: f64,
}

#[derive(Clone, Copy, Debug)]
struct EopSample {
    xp_rad: f64,
    yp_rad: f64,
    ut1_utc_seconds: f64,
    lod_seconds: f64,
}

pub fn run_from_args() -> io::Result<()> {
    let config = parse_args()?;
    run(config)
}

fn run(config: Config) -> io::Result<()> {
    if config.hours <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--hours must be greater than 0",
        ));
    }
    if config.step_minutes <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--step-minutes must be greater than 0",
        ));
    }
    if config.cell_degrees <= 0 || 180 % config.cell_degrees != 0 || 360 % config.cell_degrees != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--cell-degrees must divide both 180 and 360",
        ));
    }

    fs::create_dir_all(&config.output_dir)?;

    eprintln!("Loading catalog from {}", config.catalog.display());
    let catalog_rows = load_catalog_rows(&config.catalog)?;
    let datasets = build_shell_datasets(
        &catalog_rows,
        &config.group1_history,
        &config.group4_history_root,
    )?;

    let shell_max_epochs = scan_shell_max_epochs(&datasets)?;
    let latest_common_utc = determine_latest_common_utc(&shell_max_epochs)?;
    let center_utc = config
        .center_utc
        .unwrap_or_else(|| round_down_to_step(latest_common_utc, config.step_minutes));
    let start_utc = center_utc - Duration::minutes(config.hours * 30);
    let end_utc = center_utc + Duration::minutes(config.hours * 30);
    let frame_times = build_frame_times(start_utc, end_utc, config.step_minutes)?;

    eprintln!(
        "Selecting latest TLEs at or before {}",
        center_utc.to_rfc3339()
    );
    let latest_records = select_latest_tles(&datasets, &catalog_rows, &center_utc)?;
    let latest_records = filter_active_records(latest_records, center_utc);
    if latest_records.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no TLEs were found for the requested shells",
        ));
    }

    let cells = build_cells(config.cell_degrees);
    let eop_records = load_eop_records(&config.eop)?;
    let (tracks, visibility) = compute_tracks_and_visibility(
        &latest_records,
        &frame_times,
        &cells,
        &eop_records,
    )?;
    let shell_info = build_shell_info(&datasets, &shell_max_epochs, &tracks);

    let summary_csv_path = config.output_dir.join("shell_summary.csv");
    let db_path = config.output_dir.join("starlink_shell_map.sqlite");
    let data_js_path = config.output_dir.join("data.js");
    let html_path = config.output_dir.join("index.html");

    write_summary_csv(&summary_csv_path, &shell_info)?;
    write_sqlite_database(
        &db_path,
        &shell_info,
        &datasets,
        &tracks,
        &frame_times,
        &cells,
        &visibility,
        center_utc,
        latest_common_utc,
    )?;
    write_data_js(
        &data_js_path,
        &shell_info,
        &tracks,
        &frame_times,
        &cells,
        &visibility,
        center_utc,
        latest_common_utc,
        config.step_minutes,
    )?;
    write_html(&html_path, &shell_info, center_utc, config.step_minutes, config.cell_degrees)?;

    eprintln!("Wrote {}", summary_csv_path.display());
    eprintln!("Wrote {}", db_path.display());
    eprintln!("Wrote {}", data_js_path.display());
    eprintln!("Wrote {}", html_path.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut catalog = root.join("data/starlink_group_catalog.csv");
    let mut group1_history = root.join("starlink-group-1");
    let mut group4_history_root = root.join("starlink-group-4");
    let mut eop = root.join("eop/eopc04_20u24.1962-now.csv");
    let mut output_dir = root.join("data/starlink_shell_map");
    let mut center_utc = None;
    let mut hours = 24i64;
    let mut step_minutes = 5i64;
    let mut cell_degrees = 5i32;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--catalog" => catalog = PathBuf::from(next_arg(&mut args, "--catalog")?),
            "--group1-history" => {
                group1_history = PathBuf::from(next_arg(&mut args, "--group1-history")?)
            }
            "--group4-history-root" => {
                group4_history_root = PathBuf::from(next_arg(&mut args, "--group4-history-root")?)
            }
            "--eop" => eop = PathBuf::from(next_arg(&mut args, "--eop")?),
            "--output-dir" => output_dir = PathBuf::from(next_arg(&mut args, "--output-dir")?),
            "--center-utc" => {
                center_utc = Some(parse_rfc3339_utc(&next_arg(&mut args, "--center-utc")?)?)
            }
            "--hours" => {
                hours = next_arg(&mut args, "--hours")?.parse::<i64>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid --hours: {}", error),
                    )
                })?;
            }
            "--step-minutes" => {
                step_minutes = next_arg(&mut args, "--step-minutes")?
                    .parse::<i64>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --step-minutes: {}", error),
                        )
                    })?;
            }
            "--cell-degrees" => {
                cell_degrees = next_arg(&mut args, "--cell-degrees")?
                    .parse::<i32>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --cell-degrees: {}", error),
                        )
                    })?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                ))
            }
        }
    }

    Ok(Config {
        catalog,
        group1_history,
        group4_history_root,
        eop,
        output_dir,
        center_utc,
        hours,
        step_minutes,
        cell_degrees,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_shell_map -- [options]");
    println!("Options:");
    println!("  --catalog PATH");
    println!("  --group1-history PATH");
    println!("  --group4-history-root PATH");
    println!("  --eop PATH");
    println!("  --output-dir PATH");
    println!("  --center-utc RFC3339");
    println!("  --hours N");
    println!("  --step-minutes N");
    println!("  --cell-degrees N");
    println!();
    println!("Defaults:");
    println!("  catalog: data/starlink_group_catalog.csv");
    println!("  group1-history: starlink-group-1");
    println!("  group4-history-root: starlink-group-4");
    println!("  eop: eop/eopc04_20u24.1962-now.csv");
    println!("  output-dir: data/starlink_shell_map");
    println!("  center-utc: latest common shell epoch rounded down to step");
    println!("  hours: 24");
    println!("  step-minutes: 5");
    println!("  cell-degrees: 5");
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing value for {}", flag),
        )
    })
}

fn build_shell_datasets(
    catalog_rows: &[CatalogRow],
    group1_history: &Path,
    group4_history_root: &Path,
) -> io::Result<Vec<ShellDataset>> {
    let group1_history_file = discover_group1_history_file(group1_history)?;
    let group4_history_map = discover_group4_history_files(group4_history_root)?;

    let mut phase1_rows = Vec::new();
    let mut group4_rows = Vec::new();
    for row in catalog_rows {
        match row.group_family.as_str() {
            "phase1" => phase1_rows.push(row.clone()),
            "group4" => group4_rows.push(row.clone()),
            _ => {}
        }
    }

    if phase1_rows.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "catalog did not contain any phase1 rows",
        ));
    }

    let mut phase1_ids = HashSet::new();
    let mut phase1_groups = BTreeMap::<String, GroupInfo>::new();
    for row in &phase1_rows {
        phase1_ids.insert(row.norad_cat_id.clone());
        phase1_groups.entry(row.group_slug.clone()).or_insert(GroupInfo {
            group_slug: row.group_slug.clone(),
            group_name: row.group_name.clone(),
            launch_date: row.launch_date.clone(),
            satellite_count: 0,
            history_path: Some(group1_history_file.clone()),
        });
        if let Some(group) = phase1_groups.get_mut(&row.group_slug) {
            group.satellite_count += 1;
        }
    }

    let mut group4_groups = BTreeMap::<String, GroupInfo>::new();
    let mut group4_inputs = Vec::new();
    let mut missing_group4 = Vec::new();
    let mut group4_rows_by_slug = BTreeMap::<String, Vec<CatalogRow>>::new();
    for row in group4_rows {
        group4_rows_by_slug
            .entry(row.group_slug.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    for (group_slug, rows) in group4_rows_by_slug {
        let history_path = group4_history_map.get(&group_slug).cloned();
        group4_groups.insert(
            group_slug.clone(),
            GroupInfo {
                group_slug: group_slug.clone(),
                group_name: rows[0].group_name.clone(),
                launch_date: rows[0].launch_date.clone(),
                satellite_count: rows.len(),
                history_path: history_path.clone(),
            },
        );
        if let Some(path) = history_path {
            let mut ids = HashSet::new();
            for row in &rows {
                ids.insert(row.norad_cat_id.clone());
            }
            group4_inputs.push(HistoryInput {
                path,
                norad_ids: ids,
            });
        } else {
            missing_group4.push(group_slug);
        }
    }

    Ok(vec![
        ShellDataset {
            shell_id: GROUP1_SHELL_ID.to_string(),
            display_name: GROUP1_DISPLAY_NAME.to_string(),
            color: GROUP1_COLOR.to_string(),
            groups: phase1_groups.into_values().collect(),
            inputs: vec![HistoryInput {
                path: group1_history_file,
                norad_ids: phase1_ids,
            }],
            missing_groups: Vec::new(),
        },
        ShellDataset {
            shell_id: GROUP4_SHELL_ID.to_string(),
            display_name: GROUP4_DISPLAY_NAME.to_string(),
            color: GROUP4_COLOR.to_string(),
            groups: group4_groups.into_values().collect(),
            inputs: group4_inputs,
            missing_groups: missing_group4,
        },
    ])
}

fn discover_group1_history_file(root: &Path) -> io::Result<PathBuf> {
    if root.is_file() {
        return Ok(root.to_path_buf());
    }
    let preferred = [
        "starlink_gp_history.csv",
        "starlink_gp_history_full_history.csv",
    ];
    for name in preferred {
        let path = root.join(name);
        if path.exists() {
            return Ok(path);
        }
    }

    let mut candidates = fs::read_dir(root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .map(|value| value.starts_with("starlink_gp_history_") && value.ends_with(".csv"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.pop().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "could not find a starlink GP history CSV under {}",
                root.display()
            ),
        )
    })
}

fn discover_group4_history_files(root: &Path) -> io::Result<BTreeMap<String, PathBuf>> {
    let mut map = BTreeMap::new();
    if !root.exists() {
        return Ok(map);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let group_slug = entry.file_name().to_string_lossy().to_string();
        let dir = entry.path();
        let candidates = [
            dir.join("starlink_gp_history.csv"),
            dir.join("starlink_gp_history_full_history.csv"),
        ];
        for candidate in candidates {
            if candidate.exists() {
                map.insert(group_slug.clone(), candidate);
                break;
            }
        }
    }
    Ok(map)
}

fn scan_shell_max_epochs(datasets: &[ShellDataset]) -> io::Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for dataset in datasets {
        let mut shell_max = None::<String>;
        for input in &dataset.inputs {
            eprintln!("Scanning latest epoch in {}", input.path.display());
            let file_max = scan_history_max_epoch(&input.path, &input.norad_ids)?;
            if let Some(epoch) = file_max {
                if shell_max.as_ref().map(|current| &epoch > current).unwrap_or(true) {
                    shell_max = Some(epoch);
                }
            }
        }
        if let Some(max_epoch) = shell_max {
            result.insert(dataset.shell_id.clone(), max_epoch);
        }
    }
    Ok(result)
}

fn scan_history_max_epoch(path: &Path, norad_ids: &HashSet<String>) -> io::Result<Option<String>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "history CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(&header));
    let norad_index = require_column(&header_map, "NORAD_CAT_ID")?;
    let epoch_index = require_column(&header_map, "EPOCH")?;

    let mut latest = None::<String>;
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(&line);
        let norad = get_field(&fields, norad_index, "NORAD_CAT_ID")?;
        if !norad_ids.contains(&norad) {
            continue;
        }
        let epoch = get_field(&fields, epoch_index, "EPOCH")?;
        if epoch.is_empty() {
            continue;
        }
        if latest.as_ref().map(|current| &epoch > current).unwrap_or(true) {
            latest = Some(epoch);
        }
    }
    Ok(latest)
}

fn determine_latest_common_utc(shell_max_epochs: &BTreeMap<String, String>) -> io::Result<DateTime<Utc>> {
    let group1 = shell_max_epochs.get(GROUP1_SHELL_ID).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "no Group 1 history rows were available",
        )
    })?;
    let group4 = shell_max_epochs.get(GROUP4_SHELL_ID).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "no Group 4 history rows were available",
        )
    })?;
    let anchor = if group1 <= group4 { group1 } else { group4 };
    parse_iso_utc(anchor)
}

fn select_latest_tles(
    datasets: &[ShellDataset],
    catalog_rows: &[CatalogRow],
    center_utc: &DateTime<Utc>,
) -> io::Result<Vec<LatestTleRecord>> {
    let anchor_text = format_iso_utc(center_utc);
    let mut catalog_by_norad = HashMap::<String, CatalogRow>::new();
    let mut shell_by_norad = HashMap::<String, (String, String, String)>::new();
    for row in catalog_rows {
        if let Some((shell_id, display_name, color)) = shell_identity(&row.group_family) {
            catalog_by_norad.insert(row.norad_cat_id.clone(), row.clone());
            shell_by_norad.insert(
                row.norad_cat_id.clone(),
                (shell_id.to_string(), display_name.to_string(), color.to_string()),
            );
        }
    }

    let mut found = BTreeMap::<String, LatestTleRecord>::new();
    for dataset in datasets {
        for input in &dataset.inputs {
            eprintln!("Selecting TLEs from {}", input.path.display());
            select_latest_tles_from_history(
                &input.path,
                &input.norad_ids,
                &anchor_text,
                &catalog_by_norad,
                &shell_by_norad,
                &mut found,
            )?;
        }
    }

    Ok(found.into_values().collect())
}

fn select_latest_tles_from_history(
    path: &Path,
    norad_ids: &HashSet<String>,
    anchor_text: &str,
    catalog_by_norad: &HashMap<String, CatalogRow>,
    shell_by_norad: &HashMap<String, (String, String, String)>,
    found: &mut BTreeMap<String, LatestTleRecord>,
) -> io::Result<()> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "history CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(&header));
    let norad_index = require_column(&header_map, "NORAD_CAT_ID")?;
    let object_name_index = require_column(&header_map, "OBJECT_NAME")?;
    let object_id_index = require_column(&header_map, "OBJECT_ID")?;
    let epoch_index = require_column(&header_map, "EPOCH")?;
    let creation_index = require_column(&header_map, "CREATION_DATE")?;
    let decay_index = require_column(&header_map, "DECAY_DATE")?;
    let line1_index = require_column(&header_map, "TLE_LINE1")?;
    let line2_index = require_column(&header_map, "TLE_LINE2")?;

    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(&line);
        let norad = get_field(&fields, norad_index, "NORAD_CAT_ID")?;
        if !norad_ids.contains(&norad) {
            continue;
        }
        let epoch_text = get_field(&fields, epoch_index, "EPOCH")?;
        if epoch_text.is_empty() || epoch_text.as_str() > anchor_text {
            continue;
        }
        let creation_date_text = get_field(&fields, creation_index, "CREATION_DATE")?;
        let better = match found.get(&norad) {
            Some(existing) => {
                epoch_text > existing.epoch_text
                    || (epoch_text == existing.epoch_text
                        && creation_date_text > existing.creation_date_text)
            }
            None => true,
        };
        if !better {
            continue;
        }

        let catalog = catalog_by_norad.get(&norad).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("NORAD {} was missing from the catalog", norad),
            )
        })?;
        let (shell_id, display_name, color) = shell_by_norad.get(&norad).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("NORAD {} did not map to Group 1 or Group 4", norad),
            )
        })?;
        let object_name = get_field(&fields, object_name_index, "OBJECT_NAME")?;
        let object_id = get_field(&fields, object_id_index, "OBJECT_ID")?;
        let decay_date_text = get_field(&fields, decay_index, "DECAY_DATE")?;
        let line1 = get_field(&fields, line1_index, "TLE_LINE1")?;
        let line2 = get_field(&fields, line2_index, "TLE_LINE2")?;
        found.insert(
            norad.clone(),
            LatestTleRecord {
                shell_id: shell_id.clone(),
                display_name: display_name.clone(),
                color: color.clone(),
                group_slug: catalog.group_slug.clone(),
                group_name: catalog.group_name.clone(),
                launch_date: catalog.launch_date.clone(),
                norad_cat_id: norad,
                satname: catalog.satname.clone(),
                object_name,
                object_id,
                epoch_text,
                creation_date_text,
                decay_date_text,
                tle_line1: line1,
                tle_line2: line2,
            },
        );
    }
    Ok(())
}

fn build_frame_times(
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    step_minutes: i64,
) -> io::Result<Vec<DateTime<Utc>>> {
    if end_utc < start_utc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "end UTC must be after start UTC",
        ));
    }
    let mut frames = Vec::new();
    let mut current = start_utc;
    while current <= end_utc {
        frames.push(current);
        current += Duration::minutes(step_minutes);
    }
    Ok(frames)
}

fn build_cells(cell_degrees: i32) -> Vec<Cell> {
    let mut cells = Vec::new();
    let lat_steps = 180 / cell_degrees;
    let lon_steps = 360 / cell_degrees;
    for lat_index in 0..lat_steps {
        let lat_min_deg = -90.0 + (lat_index * cell_degrees) as f64;
        let lat_max_deg = lat_min_deg + cell_degrees as f64;
        let lat_center_deg = (lat_min_deg + lat_max_deg) * 0.5;
        let lat_rad = lat_center_deg.to_radians();
        let cos_lat = lat_rad.cos();
        let sin_lat = lat_rad.sin();
        for lon_index in 0..lon_steps {
            let lon_min_deg = -180.0 + (lon_index * cell_degrees) as f64;
            let lon_max_deg = lon_min_deg + cell_degrees as f64;
            let lon_center_deg = (lon_min_deg + lon_max_deg) * 0.5;
            let lon_rad = lon_center_deg.to_radians();
            cells.push(Cell {
                index: cells.len(),
                lat_min_deg,
                lat_max_deg,
                lon_min_deg,
                lon_max_deg,
                unit_x: cos_lat * lon_rad.cos(),
                unit_y: cos_lat * lon_rad.sin(),
                unit_z: sin_lat,
            });
        }
    }
    cells
}

fn compute_tracks_and_visibility(
    records: &[LatestTleRecord],
    frame_times: &[DateTime<Utc>],
    cells: &[Cell],
    eop_records: &[EopRecord],
) -> io::Result<(Vec<SatelliteTrack>, Vec<FrameCellVisibility>)> {
    let mut tracks = Vec::new();
    let mut visibility = frame_times
        .iter()
        .map(|_| FrameCellVisibility {
            group1_counts: vec![0u16; cells.len()],
            group4_counts: vec![0u16; cells.len()],
        })
        .collect::<Vec<_>>();

    for (index, record) in records.iter().enumerate() {
        if index % 100 == 0 {
            eprintln!("Propagating satellite {} / {}", index + 1, records.len());
        }
        let mut tle = TLE::new();
        tle.name = record.satname.clone();
        tle.parse_lines(&record.tle_line1, &record.tle_line2);
        let mut samples = Vec::with_capacity(frame_times.len());
        let mut failed = false;
        for (frame_index, frame_utc) in frame_times.iter().enumerate() {
            let mins_after_epoch = duration_to_minutes(*frame_utc - tle.epoch)?;
            let (r_teme, v_teme) = tle.get_rv(mins_after_epoch);
            if tle.sgp4_error != 0 {
                eprintln!(
                    "Skipping NORAD {} due to SGP4 error {} at {}",
                    record.norad_cat_id,
                    tle.sgp4_error,
                    frame_utc.to_rfc3339()
                );
                failed = true;
                break;
            }
            let mjd_utc = datetime_to_jd(frame_utc) - MJD_OFFSET;
            let eop = interpolate_eop(eop_records, mjd_utc)?;
            let jd_utc = datetime_to_jd(frame_utc);
            let jdut1 = jd_utc + eop.ut1_utc_seconds / 86400.0;
            let (r_ecef, _) = teme_to_ecef(
                r_teme,
                v_teme,
                jdut1,
                eop.lod_seconds,
                eop.xp_rad,
                eop.yp_rad,
            );
            let (lat_deg, lon_deg, altitude_km) = ecef_to_geodetic(r_ecef);
            samples.push(PositionSample {
                lat_deg,
                lon_deg,
                altitude_km,
                x_km: r_ecef[0],
                y_km: r_ecef[1],
                z_km: r_ecef[2],
            });
            for cell in cells {
                let visible = r_ecef[0] * cell.unit_x + r_ecef[1] * cell.unit_y + r_ecef[2] * cell.unit_z
                    > EARTH_RADIUS_KM;
                if visible {
                    if record.shell_id == GROUP1_SHELL_ID {
                        visibility[frame_index].group1_counts[cell.index] += 1;
                    } else if record.shell_id == GROUP4_SHELL_ID {
                        visibility[frame_index].group4_counts[cell.index] += 1;
                    }
                }
            }
        }
        if failed {
            continue;
        }
        tracks.push(SatelliteTrack {
            record: record.clone(),
            samples,
        });
    }
    tracks.sort_by(|a, b| {
        a.record
            .shell_id
            .cmp(&b.record.shell_id)
            .then_with(|| a.record.group_slug.cmp(&b.record.group_slug))
            .then_with(|| compare_norad(&a.record.norad_cat_id, &b.record.norad_cat_id))
    });
    Ok((tracks, visibility))
}

fn build_shell_info(
    datasets: &[ShellDataset],
    shell_max_epochs: &BTreeMap<String, String>,
    tracks: &[SatelliteTrack],
) -> Vec<ShellInfo> {
    let mut counts = BTreeMap::<String, usize>::new();
    for track in tracks {
        *counts.entry(track.record.shell_id.clone()).or_insert(0) += 1;
    }
    datasets
        .iter()
        .map(|dataset| ShellInfo {
            shell_id: dataset.shell_id.clone(),
            display_name: dataset.display_name.clone(),
            color: dataset.color.clone(),
            expected_group_count: dataset.groups.len(),
            available_group_count: dataset
                .groups
                .iter()
                .filter(|group| group.history_path.is_some())
                .count(),
            expected_satellite_count: dataset.groups.iter().map(|group| group.satellite_count).sum(),
            available_satellite_count: counts.get(&dataset.shell_id).copied().unwrap_or(0),
            latest_epoch_text: shell_max_epochs.get(&dataset.shell_id).cloned(),
            missing_groups: dataset.missing_groups.clone(),
        })
        .collect()
}

fn write_summary_csv(path: &Path, shell_info: &[ShellInfo]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "shell_id,display_name,color,expected_group_count,available_group_count,expected_satellite_count,available_satellite_count,latest_epoch_utc,missing_groups"
    )?;
    for item in shell_info {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{}",
            csv_escape(&item.shell_id),
            csv_escape(&item.display_name),
            csv_escape(&item.color),
            item.expected_group_count,
            item.available_group_count,
            item.expected_satellite_count,
            item.available_satellite_count,
            csv_escape(item.latest_epoch_text.as_deref().unwrap_or("")),
            csv_escape(&item.missing_groups.join("|")),
        )?;
    }
    writer.flush()
}

fn write_sqlite_database(
    path: &Path,
    shell_info: &[ShellInfo],
    datasets: &[ShellDataset],
    tracks: &[SatelliteTrack],
    frame_times: &[DateTime<Utc>],
    cells: &[Cell],
    visibility: &[FrameCellVisibility],
    center_utc: DateTime<Utc>,
    latest_common_utc: DateTime<Utc>,
) -> io::Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    let mut child = Command::new("sqlite3")
        .arg(path)
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to start sqlite3: {}", error),
            )
        })?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            "failed to open sqlite3 stdin",
        )
    })?;

    writeln!(stdin, "PRAGMA synchronous = OFF;")?;
    writeln!(stdin, "BEGIN;")?;
    writeln!(
        stdin,
        "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
    )?;
    writeln!(
        stdin,
        "CREATE TABLE shells (shell_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, color TEXT NOT NULL, expected_group_count INTEGER NOT NULL, available_group_count INTEGER NOT NULL, expected_satellite_count INTEGER NOT NULL, available_satellite_count INTEGER NOT NULL, latest_epoch_utc TEXT);"
    )?;
    writeln!(
        stdin,
        "CREATE TABLE groups (group_slug TEXT PRIMARY KEY, shell_id TEXT NOT NULL, group_name TEXT NOT NULL, launch_date TEXT NOT NULL, satellite_count INTEGER NOT NULL, history_path TEXT);"
    )?;
    writeln!(
        stdin,
        "CREATE TABLE satellites (norad_cat_id TEXT PRIMARY KEY, shell_id TEXT NOT NULL, group_slug TEXT NOT NULL, satname TEXT NOT NULL, object_name TEXT NOT NULL, object_id TEXT NOT NULL, launch_date TEXT NOT NULL, tle_epoch_utc TEXT NOT NULL, tle_creation_date_utc TEXT NOT NULL, decay_date_utc TEXT NOT NULL, tle_line1 TEXT NOT NULL, tle_line2 TEXT NOT NULL);"
    )?;
    writeln!(
        stdin,
        "CREATE TABLE frames (frame_index INTEGER PRIMARY KEY, frame_utc TEXT NOT NULL);"
    )?;
    writeln!(
        stdin,
        "CREATE TABLE frame_samples (frame_index INTEGER NOT NULL, norad_cat_id TEXT NOT NULL, shell_id TEXT NOT NULL, lat_deg REAL NOT NULL, lon_deg REAL NOT NULL, altitude_km REAL NOT NULL, x_km REAL NOT NULL, y_km REAL NOT NULL, z_km REAL NOT NULL, PRIMARY KEY (frame_index, norad_cat_id));"
    )?;
    writeln!(
        stdin,
        "CREATE TABLE cell_visibility (frame_index INTEGER NOT NULL, cell_index INTEGER NOT NULL, lat_min_deg REAL NOT NULL, lat_max_deg REAL NOT NULL, lon_min_deg REAL NOT NULL, lon_max_deg REAL NOT NULL, group1_visible INTEGER NOT NULL, group4_visible INTEGER NOT NULL, diff_visible INTEGER NOT NULL, PRIMARY KEY (frame_index, cell_index));"
    )?;

    insert_metadata(&mut stdin, "generated_utc", &Utc::now().to_rfc3339())?;
    insert_metadata(&mut stdin, "center_utc", &center_utc.to_rfc3339())?;
    insert_metadata(
        &mut stdin,
        "latest_common_utc",
        &latest_common_utc.to_rfc3339(),
    )?;

    for item in shell_info {
        writeln!(
            stdin,
            "INSERT INTO shells VALUES ({}, {}, {}, {}, {}, {}, {}, {});",
            sql_string(&item.shell_id),
            sql_string(&item.display_name),
            sql_string(&item.color),
            item.expected_group_count,
            item.available_group_count,
            item.expected_satellite_count,
            item.available_satellite_count,
            sql_nullable(item.latest_epoch_text.as_deref()),
        )?;
    }
    for dataset in datasets {
        for group in &dataset.groups {
            writeln!(
                stdin,
                "INSERT INTO groups VALUES ({}, {}, {}, {}, {}, {});",
                sql_string(&group.group_slug),
                sql_string(&dataset.shell_id),
                sql_string(&group.group_name),
                sql_string(&group.launch_date),
                group.satellite_count,
                sql_nullable_path(group.history_path.as_ref()),
            )?;
        }
    }
    for track in tracks {
        let record = &track.record;
        writeln!(
            stdin,
            "INSERT INTO satellites VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});",
            sql_string(&record.norad_cat_id),
            sql_string(&record.shell_id),
            sql_string(&record.group_slug),
            sql_string(&record.satname),
            sql_string(&record.object_name),
            sql_string(&record.object_id),
            sql_string(&record.launch_date),
            sql_string(&record.epoch_text),
            sql_string(&record.creation_date_text),
            sql_string(&record.decay_date_text),
            sql_string(&record.tle_line1),
            sql_string(&record.tle_line2),
        )?;
    }
    for (frame_index, frame_utc) in frame_times.iter().enumerate() {
        writeln!(
            stdin,
            "INSERT INTO frames VALUES ({}, {});",
            frame_index,
            sql_string(&frame_utc.to_rfc3339()),
        )?;
    }
    for track in tracks {
        for (frame_index, sample) in track.samples.iter().enumerate() {
            writeln!(
                stdin,
                "INSERT INTO frame_samples VALUES ({}, {}, {}, {:.6}, {:.6}, {:.6}, {:.6}, {:.6}, {:.6});",
                frame_index,
                sql_string(&track.record.norad_cat_id),
                sql_string(&track.record.shell_id),
                sample.lat_deg,
                sample.lon_deg,
                sample.altitude_km,
                sample.x_km,
                sample.y_km,
                sample.z_km,
            )?;
        }
    }
    for (frame_index, frame_counts) in visibility.iter().enumerate() {
        for cell in cells {
            let group1_visible = frame_counts.group1_counts[cell.index];
            let group4_visible = frame_counts.group4_counts[cell.index];
            let diff_visible = group1_visible as i32 - group4_visible as i32;
            writeln!(
                stdin,
                "INSERT INTO cell_visibility VALUES ({}, {}, {:.6}, {:.6}, {:.6}, {:.6}, {}, {}, {});",
                frame_index,
                cell.index,
                cell.lat_min_deg,
                cell.lat_max_deg,
                cell.lon_min_deg,
                cell.lon_max_deg,
                group1_visible,
                group4_visible,
                diff_visible,
            )?;
        }
    }
    writeln!(stdin, "CREATE INDEX idx_frame_samples_shell ON frame_samples (shell_id, frame_index);")?;
    writeln!(stdin, "CREATE INDEX idx_cell_visibility_frame ON cell_visibility (frame_index);")?;
    writeln!(stdin, "COMMIT;")?;
    drop(stdin);

    let status = child.wait()?;
    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("sqlite3 exited with status {}", status),
        ));
    }
    Ok(())
}

fn insert_metadata(writer: &mut dyn Write, key: &str, value: &str) -> io::Result<()> {
    writeln!(
        writer,
        "INSERT INTO metadata VALUES ({}, {});",
        sql_string(key),
        sql_string(value),
    )
}

fn write_data_js(
    path: &Path,
    shell_info: &[ShellInfo],
    tracks: &[SatelliteTrack],
    frame_times: &[DateTime<Utc>],
    cells: &[Cell],
    visibility: &[FrameCellVisibility],
    center_utc: DateTime<Utc>,
    latest_common_utc: DateTime<Utc>,
    step_minutes: i64,
) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "window.STARLINK_SHELL_DATA = {{")?;
    writeln!(writer, "  meta: {{")?;
    writeln!(
        writer,
        "    generatedUtc: \"{}\",",
        js_escape(&Utc::now().to_rfc3339())
    )?;
    writeln!(
        writer,
        "    centerUtc: \"{}\",",
        js_escape(&center_utc.to_rfc3339())
    )?;
    writeln!(
        writer,
        "    latestCommonUtc: \"{}\",",
        js_escape(&latest_common_utc.to_rfc3339())
    )?;
    writeln!(writer, "    stepMinutes: {},", step_minutes)?;
    writeln!(writer, "    shells: [")?;
    for item in shell_info {
        writeln!(
            writer,
            "      {{shellId:\"{}\",displayName:\"{}\",color:\"{}\",expectedGroups:{},availableGroups:{},expectedSatellites:{},availableSatellites:{},latestEpochUtc:{},missingGroups:[{}]}},",
            js_escape(&item.shell_id),
            js_escape(&item.display_name),
            js_escape(&item.color),
            item.expected_group_count,
            item.available_group_count,
            item.expected_satellite_count,
            item.available_satellite_count,
            js_nullable(item.latest_epoch_text.as_deref()),
            item
                .missing_groups
                .iter()
                .map(|value| format!("\"{}\"", js_escape(value)))
                .collect::<Vec<_>>()
                .join(","),
        )?;
    }
    writeln!(writer, "    ]")?;
    writeln!(writer, "  }},")?;

    writeln!(writer, "  frames: [")?;
    for frame in frame_times {
        writeln!(writer, "    \"{}\",", js_escape(&frame.to_rfc3339()))?;
    }
    writeln!(writer, "  ],")?;

    writeln!(writer, "  cells: [")?;
    for cell in cells {
        writeln!(
            writer,
            "    [{:.3},{:.3},{:.3},{:.3}],",
            cell.lat_min_deg, cell.lat_max_deg, cell.lon_min_deg, cell.lon_max_deg
        )?;
    }
    writeln!(writer, "  ],")?;

    writeln!(writer, "  heatmap: [")?;
    for frame_counts in visibility {
        let group1 = frame_counts
            .group1_counts
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let group4 = frame_counts
            .group4_counts
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let diff = frame_counts
            .group1_counts
            .iter()
            .zip(&frame_counts.group4_counts)
            .map(|(left, right)| (*left as i32 - *right as i32).to_string())
            .collect::<Vec<_>>()
            .join(",");
        writeln!(
            writer,
            "    {{group1:[{}],group4:[{}],diff:[{}]}},",
            group1, group4, diff
        )?;
    }
    writeln!(writer, "  ],")?;

    writeln!(writer, "  satellites: [")?;
    for track in tracks {
        let positions = track
            .samples
            .iter()
            .map(|sample| {
                format!(
                    "[{:.4},{:.4},{:.3}]",
                    sample.lat_deg, sample.lon_deg, sample.altitude_km
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        writeln!(
            writer,
            "    {{norad:\"{}\",satname:\"{}\",objectName:\"{}\",shellId:\"{}\",displayName:\"{}\",color:\"{}\",groupSlug:\"{}\",groupName:\"{}\",launchDate:\"{}\",epochUtc:\"{}\",positions:[{}]}},",
            js_escape(&track.record.norad_cat_id),
            js_escape(&track.record.satname),
            js_escape(&track.record.object_name),
            js_escape(&track.record.shell_id),
            js_escape(&track.record.display_name),
            js_escape(&track.record.color),
            js_escape(&track.record.group_slug),
            js_escape(&track.record.group_name),
            js_escape(&track.record.launch_date),
            js_escape(&track.record.epoch_text),
            positions,
        )?;
    }
    writeln!(writer, "  ]")?;
    writeln!(writer, "}};")?;
    writer.flush()
}

fn write_html(
    path: &Path,
    shell_info: &[ShellInfo],
    center_utc: DateTime<Utc>,
    step_minutes: i64,
    cell_degrees: i32,
) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "<!DOCTYPE html>")?;
    writeln!(writer, "<html lang=\"en\">")?;
    writeln!(writer, "<head>")?;
    writeln!(writer, "<meta charset=\"utf-8\">")?;
    writeln!(writer, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")?;
    writeln!(writer, "<title>Starlink Shell Map</title>")?;
    writer.write_all(
        br#"<style>
:root{
  --bg:#08101a;
  --panel:#0f1724;
  --panel-2:#131f30;
  --ink:#ebf2f7;
  --muted:#9eb0c2;
  --line:#203248;
  --group1:#66d9ff;
  --group4:#ffb35c;
}
html,body{height:100%}
body{
  margin:0;
  color:var(--ink);
  background:
    radial-gradient(circle at top left, rgba(102,217,255,.15), transparent 28%),
    radial-gradient(circle at top right, rgba(255,179,92,.16), transparent 30%),
    linear-gradient(180deg, #0a1320, #08101a 48%, #060c14);
  font-family:Georgia, 'Times New Roman', serif;
}
.page{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(320px,.9fr);min-height:100vh}
.main{padding:20px 22px 22px}
.side{border-left:1px solid var(--line);background:rgba(10,17,26,.8);backdrop-filter:blur(18px);padding:20px 18px 22px}
.hero{display:flex;justify-content:space-between;gap:16px;align-items:flex-end;margin-bottom:16px}
.eyebrow{font:600 11px/1.2 ui-monospace, SFMono-Regular, Menlo, monospace;letter-spacing:.18em;text-transform:uppercase;color:#8aa1b8}
h1{margin:4px 0 0;font-size:34px;line-height:1.05}
.lede{max-width:74ch;margin:6px 0 0;color:var(--muted);font-size:14px;line-height:1.6}
.stamp{font:600 12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace;color:#b7c6d4;text-align:right}
.map-wrap{position:relative;border:1px solid var(--line);border-radius:20px;overflow:hidden;background:linear-gradient(180deg, rgba(21,43,62,.95), rgba(8,16,26,.96));box-shadow:0 30px 80px rgba(0,0,0,.3)}
#mapCanvas{display:block;width:100%;height:min(72vh,860px)}
.overlay{position:absolute;left:16px;right:16px;bottom:16px;display:flex;justify-content:space-between;gap:12px;pointer-events:none}
.pill{background:rgba(7,12,19,.72);border:1px solid rgba(164,184,203,.18);border-radius:999px;padding:10px 14px;font:600 12px/1.2 ui-monospace, SFMono-Regular, Menlo, monospace}
.grid{display:grid;gap:12px}
.card{background:linear-gradient(180deg, rgba(19,31,48,.9), rgba(12,19,29,.92));border:1px solid var(--line);border-radius:16px;padding:14px}
.card h2{margin:0 0 10px;font-size:15px}
.control-row{display:grid;gap:10px}
.inline{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
label,.muted{font:600 12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace;color:var(--muted)}
button,select,input[type=range]{width:100%}
button,select{
  border:1px solid #2a4058;
  background:#0d1724;
  color:var(--ink);
  border-radius:12px;
  padding:10px 12px;
  font:600 13px/1.2 ui-monospace, SFMono-Regular, Menlo, monospace;
}
button{cursor:pointer}
button:hover{background:#102034}
input[type=range]{accent-color:#7fd7ff}
.checks{display:grid;gap:8px}
.check{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:10px;
  padding:10px 12px;
  border-radius:12px;
  border:1px solid #223347;
  background:#0d1724;
}
.check input{width:auto}
.key{display:flex;align-items:center;gap:9px}
.swatch{width:10px;height:10px;border-radius:999px}
.stats{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.stat{padding:10px 12px;border-radius:12px;background:#0b1420;border:1px solid #1d2d41}
.stat strong{display:block;font-size:22px;line-height:1.1}
.stat span{display:block;margin-top:4px;color:var(--muted);font:600 11px/1.3 ui-monospace, SFMono-Regular, Menlo, monospace;text-transform:uppercase;letter-spacing:.08em}
.shell-list{display:grid;gap:10px}
.shell-row{padding:11px 12px;border-radius:12px;background:#0b1420;border:1px solid #1d2d41}
.shell-row h3{margin:0 0 6px;font-size:14px}
.shell-row p{margin:0;color:var(--muted);font-size:12px;line-height:1.5}
.legend-scale{display:grid;gap:8px}
.legend-bar{height:14px;border-radius:999px;border:1px solid #2a4058;background:linear-gradient(90deg,#66d9ff,#122030,#ffb35c)}
.legend-labels{display:flex;justify-content:space-between;gap:10px;font:600 11px/1.3 ui-monospace, SFMono-Regular, Menlo, monospace;color:var(--muted)}
.legend-mid{text-align:center;flex:1}
.legend-note{color:#d8e4ee;font-size:12px;line-height:1.5}
#hover{min-height:70px;color:#d8e4ee;font-size:13px;line-height:1.6}
code{font-family:ui-monospace, SFMono-Regular, Menlo, monospace;color:#bfeeff}
@media (max-width: 1080px){
  .page{grid-template-columns:1fr}
  .side{border-left:0;border-top:1px solid var(--line)}
  #mapCanvas{height:62vh}
}
</style>"#,
    )?;
    writeln!(writer, "</head>")?;
    writeln!(writer, "<body>")?;
    writeln!(writer, "<div class=\"page\">")?;
    writeln!(writer, "<section class=\"main\">")?;
    writeln!(writer, "<div class=\"hero\">")?;
    writeln!(writer, "<div><div class=\"eyebrow\">Starlink Shell Visualizer</div><h1>Group 1 vs Group 4</h1><p class=\"lede\">The map overlays shell-specific Starlink positions on top of a gridded Earth projection, then shades each surface cell by visible-satellite count. The animation uses 5-minute SGP4 samples and interpolates intermediate motion along the shortest great-circle arc on Earth.</p></div>")?;
    writeln!(
        writer,
        "<div class=\"stamp\">Center UTC<br><strong>{}</strong><br>Grid {}&deg; x {}&deg;<br>Step {} min</div>",
        center_utc.to_rfc3339(),
        cell_degrees,
        cell_degrees,
        step_minutes
    )?;
    writeln!(writer, "</div>")?;
    writeln!(writer, "<div class=\"map-wrap\"><canvas id=\"mapCanvas\"></canvas><div class=\"overlay\"><div class=\"pill\" id=\"timePill\">Loading...</div><div class=\"pill\" id=\"modePill\">Heatmap: diff (Group1 - Group4)</div></div></div>")?;
    writeln!(writer, "</section>")?;
    writeln!(writer, "<aside class=\"side\"><div class=\"grid\">")?;
    writeln!(writer, "<div class=\"card\"><h2>Playback</h2><div class=\"control-row\"><div class=\"inline\"><button id=\"playPause\">Pause</button><select id=\"speedSelect\"><option value=\"0.25\">0.25x</option><option value=\"0.5\">0.5x</option><option value=\"1\" selected>1x</option><option value=\"2\">2x</option><option value=\"4\">4x</option></select></div><label for=\"frameSlider\">Frame</label><input id=\"frameSlider\" type=\"range\" min=\"0\" max=\"0\" value=\"0\"></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2>Layers</h2><div class=\"control-row\"><label for=\"heatmapMode\">Heatmap</label><select id=\"heatmapMode\"><option value=\"diff\" selected>Group1 - Group4</option><option value=\"group1\">Group 1 visible count</option><option value=\"group4\">Group 4 visible count</option><option value=\"off\">Off</option></select><div class=\"checks\" id=\"shellChecks\"></div></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2 id=\"legendTitle\">Heatmap Legend</h2><div class=\"legend-scale\"><div class=\"legend-bar\" id=\"legendBar\"></div><div class=\"legend-labels\"><span id=\"legendMin\">Group 4 higher</span><span class=\"legend-mid\" id=\"legendMid\">equal</span><span id=\"legendMax\">Group 1 higher</span></div><div class=\"legend-note\" id=\"legendNote\">Colors are normalized to the current frame's peak absolute difference.</div></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2>Snapshot</h2><div class=\"stats\"><div class=\"stat\"><strong id=\"satelliteCount\">0</strong><span>Displayed Satellites</span></div><div class=\"stat\"><strong id=\"cellPeak\">0</strong><span>Peak Cell Count</span></div></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2>Hover</h2><div id=\"hover\">Move the pointer near a satellite.</div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2>Coverage</h2><div class=\"shell-list\">")?;
    for item in shell_info {
        writeln!(
            writer,
            "<div class=\"shell-row\"><h3><span class=\"swatch\" style=\"display:inline-block;background:{}\"></span> {}</h3><p>{} / {} groups, {} / {} satellites.<br>Latest local epoch: <code>{}</code>{}</p></div>",
            item.color,
            item.display_name,
            item.available_group_count,
            item.expected_group_count,
            item.available_satellite_count,
            item.expected_satellite_count,
            item.latest_epoch_text.as_deref().unwrap_or("n/a"),
            if item.missing_groups.is_empty() {
                String::new()
            } else {
                format!("<br>Missing local groups: {}", item.missing_groups.join(", "))
            }
        )?;
    }
    writeln!(writer, "</div></div>")?;
    writeln!(writer, "</div></aside></div>")?;
    writeln!(writer, "<script src=\"data.js\"></script>")?;
    writer.write_all(
        br#"<script>
const DATA = window.STARLINK_SHELL_DATA;
const canvas = document.getElementById('mapCanvas');
const ctx = canvas.getContext('2d');
const frameSlider = document.getElementById('frameSlider');
const playPause = document.getElementById('playPause');
const speedSelect = document.getElementById('speedSelect');
const heatmapMode = document.getElementById('heatmapMode');
const shellChecks = document.getElementById('shellChecks');
const hoverEl = document.getElementById('hover');
const timePill = document.getElementById('timePill');
const modePill = document.getElementById('modePill');
const satelliteCountEl = document.getElementById('satelliteCount');
const cellPeakEl = document.getElementById('cellPeak');
const legendTitleEl = document.getElementById('legendTitle');
const legendBarEl = document.getElementById('legendBar');
const legendMinEl = document.getElementById('legendMin');
const legendMidEl = document.getElementById('legendMid');
const legendMaxEl = document.getElementById('legendMax');
const legendNoteEl = document.getElementById('legendNote');

let dpr = Math.max(1, window.devicePixelRatio || 1);
let width = 0;
let height = 0;
let currentFrame = 0;
let playing = true;
let lastTick = performance.now();
let pointerX = -1e9;
let pointerY = -1e9;

const state = {
  visibleShells: new Set(DATA.meta.shells.map(item => item.shellId)),
};

function buildShellChecks() {
  for (const shell of DATA.meta.shells) {
    const row = document.createElement('label');
    row.className = 'check';
    row.innerHTML = `<span class="key"><span class="swatch" style="background:${shell.color}"></span><span>${shell.displayName}</span></span><input type="checkbox" checked>`;
    const input = row.querySelector('input');
    input.addEventListener('change', () => {
      if (input.checked) state.visibleShells.add(shell.shellId);
      else state.visibleShells.delete(shell.shellId);
      draw();
    });
    shellChecks.appendChild(row);
  }
}

function resize() {
  dpr = Math.max(1, window.devicePixelRatio || 1);
  const rect = canvas.getBoundingClientRect();
  width = Math.max(640, Math.round(rect.width));
  height = Math.max(420, Math.round(rect.height));
  canvas.width = Math.round(width * dpr);
  canvas.height = Math.round(height * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  draw();
}

function wrapLon(lon) {
  let value = lon;
  while (value < -180) value += 360;
  while (value > 180) value -= 360;
  return value;
}

function lerpLon(a, b, t) {
  const delta = ((b - a + 540) % 360) - 180;
  return wrapLon(a + delta * t);
}

function latLonToUnit(lat, lon) {
  const latRad = lat * Math.PI / 180;
  const lonRad = lon * Math.PI / 180;
  const cosLat = Math.cos(latRad);
  return {
    x: cosLat * Math.cos(lonRad),
    y: cosLat * Math.sin(lonRad),
    z: Math.sin(latRad),
  };
}

function normalizeVec(vec) {
  const norm = Math.hypot(vec.x, vec.y, vec.z) || 1;
  return { x: vec.x / norm, y: vec.y / norm, z: vec.z / norm };
}

function dotVec(a, b) {
  return a.x * b.x + a.y * b.y + a.z * b.z;
}

function slerpUnit(a, b, t) {
  let dot = Math.max(-1, Math.min(1, dotVec(a, b)));
  if (dot > 0.9995 || dot < -0.9995) {
    return normalizeVec({
      x: a.x + (b.x - a.x) * t,
      y: a.y + (b.y - a.y) * t,
      z: a.z + (b.z - a.z) * t,
    });
  }
  const omega = Math.acos(dot);
  const sinOmega = Math.sin(omega);
  const scaleA = Math.sin((1 - t) * omega) / sinOmega;
  const scaleB = Math.sin(t * omega) / sinOmega;
  return {
    x: a.x * scaleA + b.x * scaleB,
    y: a.y * scaleA + b.y * scaleB,
    z: a.z * scaleA + b.z * scaleB,
  };
}

function unitToLatLon(vec) {
  const unit = normalizeVec(vec);
  return {
    lat: Math.asin(Math.max(-1, Math.min(1, unit.z))) * 180 / Math.PI,
    lon: wrapLon(Math.atan2(unit.y, unit.x) * 180 / Math.PI),
  };
}

function greatCirclePoint(left, right, t) {
  const leftVec = latLonToUnit(left[0], left[1]);
  const rightVec = latLonToUnit(right[0], right[1]);
  const pointVec = slerpUnit(leftVec, rightVec, t);
  const point = unitToLatLon(pointVec);
  return {
    lat: point.lat,
    lon: point.lon,
    alt: left[2] + (right[2] - left[2]) * t,
  };
}

function greatCircleDistance(left, right) {
  const leftVec = latLonToUnit(left[0], left[1]);
  const rightVec = latLonToUnit(right[0], right[1]);
  return Math.acos(Math.max(-1, Math.min(1, dotVec(leftVec, rightVec))));
}

function interpolatePosition(positions, frame) {
  const maxIndex = positions.length - 1;
  const leftIndex = Math.max(0, Math.min(maxIndex, Math.floor(frame)));
  const rightIndex = Math.max(0, Math.min(maxIndex, Math.ceil(frame)));
  const fraction = Math.max(0, Math.min(1, frame - leftIndex));
  const left = positions[leftIndex];
  const right = positions[rightIndex];
  const point = leftIndex === rightIndex
    ? { lat: left[0], lon: left[1], alt: left[2] }
    : greatCirclePoint(left, right, fraction);
  return {
    lat: point.lat,
    lon: point.lon,
    alt: point.alt,
    left,
    right,
  };
}

function project(lat, lon) {
  return {
    x: (wrapLon(lon) + 180) / 360 * width,
    y: (90 - lat) / 180 * height,
  };
}

function drawBackground() {
  const gradient = ctx.createLinearGradient(0, 0, 0, height);
  gradient.addColorStop(0, '#18334b');
  gradient.addColorStop(0.45, '#0d1826');
  gradient.addColorStop(1, '#08101a');
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, width, height);
  ctx.fillStyle = 'rgba(125,164,199,0.08)';
  for (let lon = -150; lon <= 180; lon += 30) {
    const x = (lon + 180) / 360 * width;
    ctx.fillRect(x, 0, 1, height);
  }
  for (let lat = -60; lat <= 60; lat += 30) {
    const y = (90 - lat) / 180 * height;
    ctx.fillRect(0, y, width, 1);
  }
}

function heatValue(frameData, cellIndex, mode, fraction, nextData) {
  if (mode === 'off') return 0;
  const source = frameData[mode][cellIndex];
  if (!nextData) return source;
  const target = nextData[mode][cellIndex];
  return source + (target - source) * fraction;
}

function heatColor(value, mode, peak) {
  if (mode === 'off') return null;
  if (mode === 'diff') {
    const maxAbs = Math.max(1, peak);
    const norm = Math.max(-1, Math.min(1, value / maxAbs));
    if (Math.abs(norm) < 0.03) return 'rgba(0,0,0,0)';
    const alpha = 0.14 + Math.abs(norm) * 0.56;
    return norm >= 0
      ? `rgba(255,179,92,${alpha.toFixed(3)})`
      : `rgba(102,217,255,${alpha.toFixed(3)})`;
  }
  const norm = Math.max(0, Math.min(1, value / Math.max(1, peak)));
  const alpha = 0.08 + norm * 0.52;
  return mode === 'group1'
    ? `rgba(102,217,255,${alpha.toFixed(3)})`
    : `rgba(255,179,92,${alpha.toFixed(3)})`;
}

function updateLegend(mode, peak) {
  if (mode === 'off') {
    legendTitleEl.textContent = 'Heatmap Legend';
    legendBarEl.style.background = 'linear-gradient(90deg,#0f1724,#2a4058)';
    legendMinEl.textContent = 'off';
    legendMidEl.textContent = '';
    legendMaxEl.textContent = '';
    legendNoteEl.textContent = 'Heatmap shading is disabled.';
    return;
  }

  if (mode === 'diff') {
    legendTitleEl.textContent = 'Heatmap Legend';
    legendBarEl.style.background = 'linear-gradient(90deg,#66d9ff,#122030 50%,#ffb35c)';
    legendMinEl.textContent = `Group 4 > Group 1 (-${peak.toFixed(1)})`;
    legendMidEl.textContent = 'equal (0)';
    legendMaxEl.textContent = `Group 1 > Group 4 (+${peak.toFixed(1)})`;
    legendNoteEl.textContent = 'Blue means Group 4 has more visible satellites in that cell. Orange means Group 1 has more.';
    return;
  }

  const shellLabel = mode === 'group1' ? 'Group 1' : 'Group 4';
  const shellColor = mode === 'group1' ? '#66d9ff' : '#ffb35c';
  legendTitleEl.textContent = `${shellLabel} Heatmap`;
  legendBarEl.style.background = `linear-gradient(90deg, rgba(18,32,48,0.95), ${shellColor})`;
  legendMinEl.textContent = '0';
  legendMidEl.textContent = `${(peak * 0.5).toFixed(0)}`;
  legendMaxEl.textContent = `${peak.toFixed(0)}`;
  legendNoteEl.textContent = `${shellLabel} visible-satellite count for the current frame.`;
}

function drawHeatmap(frame) {
  const leftIndex = Math.max(0, Math.floor(frame));
  const rightIndex = Math.min(DATA.heatmap.length - 1, Math.ceil(frame));
  const fraction = Math.max(0, Math.min(1, frame - leftIndex));
  const current = DATA.heatmap[leftIndex];
  const next = DATA.heatmap[rightIndex];
  const mode = heatmapMode.value;
  const values = current[mode] || [];
  let peak = 0;
  for (let cellIndex = 0; cellIndex < DATA.cells.length; cellIndex += 1) {
    const value = heatValue(current, cellIndex, mode, fraction, next);
    peak = mode === 'diff' ? Math.max(peak, Math.abs(value)) : Math.max(peak, value);
  }
  cellPeakEl.textContent = peak.toFixed(mode === 'diff' ? 1 : 0);
  modePill.textContent = mode === 'diff'
    ? 'Heatmap: diff (Group1 - Group4)'
    : mode === 'group1'
      ? 'Heatmap: Group 1 visible count'
      : mode === 'group4'
        ? 'Heatmap: Group 4 visible count'
        : 'Heatmap: off';
  updateLegend(mode, peak);

  for (let cellIndex = 0; cellIndex < DATA.cells.length; cellIndex += 1) {
    const cell = DATA.cells[cellIndex];
    const value = heatValue(current, cellIndex, mode, fraction, next);
    const color = heatColor(value, mode, peak);
    if (!color) continue;
    const x0 = (cell[2] + 180) / 360 * width;
    const x1 = (cell[3] + 180) / 360 * width;
    const y0 = (90 - cell[1]) / 180 * height;
    const y1 = (90 - cell[0]) / 180 * height;
    ctx.fillStyle = color;
    ctx.fillRect(x0, y0, x1 - x0, y1 - y0);
  }
}

function visibleSatellites() {
  return DATA.satellites.filter(item => state.visibleShells.has(item.shellId));
}

function drawGeodesicSegment(left, right, color) {
  if (left[0] === right[0] && left[1] === right[1]) return;
  const distanceDeg = greatCircleDistance(left, right) * 180 / Math.PI;
  const steps = Math.max(8, Math.ceil(distanceDeg / 2));
  ctx.strokeStyle = color;
  ctx.lineWidth = 1;
  let previous = null;
  let open = false;
  for (let step = 0; step <= steps; step += 1) {
    const point = greatCirclePoint(left, right, step / steps);
    const projected = project(point.lat, point.lon);
    if (!previous || Math.abs(projected.x - previous.x) > width * 0.5) {
      if (open) ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(projected.x, projected.y);
      open = true;
    } else {
      ctx.lineTo(projected.x, projected.y);
    }
    previous = projected;
  }
  if (open) ctx.stroke();
}

function drawSatellites(frame) {
  const satellites = visibleSatellites();
  satelliteCountEl.textContent = satellites.length.toString();
  let hovered = null;
  for (const satellite of satellites) {
    const point = interpolatePosition(satellite.positions, frame);
    const current = project(point.lat, point.lon);
    drawGeodesicSegment(point.left, point.right, `${satellite.color}55`);
    ctx.fillStyle = satellite.color;
    ctx.beginPath();
    ctx.arc(current.x, current.y, 2.35, 0, Math.PI * 2);
    ctx.fill();
    const distance = Math.hypot(pointerX - current.x, pointerY - current.y);
    if (distance < 8 && (!hovered || distance < hovered.distance)) {
      hovered = { satellite, point, current, distance };
    }
  }
  if (hovered) {
    ctx.strokeStyle = '#ffffff';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.arc(hovered.current.x, hovered.current.y, 5.2, 0, Math.PI * 2);
    ctx.stroke();
    hoverEl.innerHTML = `<strong>${hovered.satellite.satname}</strong><br>${hovered.satellite.displayName} / ${hovered.satellite.groupName}<br>NORAD: ${hovered.satellite.norad}<br>Lat/Lon: ${hovered.point.lat.toFixed(2)} deg, ${hovered.point.lon.toFixed(2)} deg<br>Altitude: ${hovered.point.alt.toFixed(1)} km`;
  } else {
    hoverEl.textContent = 'Move the pointer near a satellite.';
  }
}

function drawLabels() {
  ctx.fillStyle = 'rgba(235,242,247,0.8)';
  ctx.font = '600 11px ui-monospace, SFMono-Regular, Menlo, monospace';
  ctx.fillText('180W', 8, height * 0.5 - 6);
  ctx.fillText('0', width * 0.5 + 6, height * 0.5 - 6);
  ctx.fillText('180E', width - 46, height * 0.5 - 6);
  ctx.fillText('60N', 10, height * 0.17);
  ctx.fillText('0', 10, height * 0.5);
  ctx.fillText('60S', 10, height * 0.84);
}

function draw() {
  drawBackground();
  drawHeatmap(currentFrame);
  drawLabels();
  drawSatellites(currentFrame);
  const leftIndex = Math.max(0, Math.floor(currentFrame));
  const rightIndex = Math.min(DATA.frames.length - 1, Math.ceil(currentFrame));
  const fraction = Math.max(0, Math.min(1, currentFrame - leftIndex));
  const leftDate = new Date(DATA.frames[leftIndex]);
  const rightDate = new Date(DATA.frames[rightIndex]);
  const currentTime = new Date(leftDate.getTime() + (rightDate.getTime() - leftDate.getTime()) * fraction);
  timePill.textContent = currentTime.toISOString();
}

function tick(now) {
  const deltaSeconds = (now - lastTick) / 1000;
  lastTick = now;
  if (playing && DATA.frames.length > 1) {
    const speed = Number(speedSelect.value);
    currentFrame += deltaSeconds * speed;
    const maxFrame = DATA.frames.length - 1;
    if (currentFrame > maxFrame) currentFrame -= maxFrame;
    frameSlider.value = Math.round(currentFrame).toString();
    draw();
  }
  requestAnimationFrame(tick);
}

playPause.addEventListener('click', () => {
  playing = !playing;
  playPause.textContent = playing ? 'Pause' : 'Play';
});
frameSlider.addEventListener('input', () => {
  currentFrame = Number(frameSlider.value);
  draw();
});
heatmapMode.addEventListener('change', draw);
canvas.addEventListener('pointermove', event => {
  const rect = canvas.getBoundingClientRect();
  pointerX = event.clientX - rect.left;
  pointerY = event.clientY - rect.top;
  draw();
});
canvas.addEventListener('pointerleave', () => {
  pointerX = -1e9;
  pointerY = -1e9;
  draw();
});
window.addEventListener('resize', resize);

frameSlider.max = String(Math.max(0, DATA.frames.length - 1));
buildShellChecks();
resize();
requestAnimationFrame(tick);
</script>"#,
    )?;
    writeln!(writer, "</body>")?;
    writeln!(writer, "</html>")?;
    writer.flush()
}

fn shell_identity(group_family: &str) -> Option<(&'static str, &'static str, &'static str)> {
    match group_family {
        "phase1" => Some((GROUP1_SHELL_ID, GROUP1_DISPLAY_NAME, GROUP1_COLOR)),
        "group4" => Some((GROUP4_SHELL_ID, GROUP4_DISPLAY_NAME, GROUP4_COLOR)),
        _ => None,
    }
}

fn filter_active_records(
    records: Vec<LatestTleRecord>,
    center_utc: DateTime<Utc>,
) -> Vec<LatestTleRecord> {
    let mut kept = Vec::new();
    let mut stale = 0usize;
    let mut decayed = 0usize;
    for record in records {
        if let Some(decay_utc) = parse_optional_utc(&record.decay_date_text) {
            if decay_utc <= center_utc {
                decayed += 1;
                continue;
            }
        }
        match parse_iso_utc(&record.epoch_text) {
            Ok(epoch_utc) if center_utc - epoch_utc <= Duration::days(MAX_TLE_AGE_DAYS) => {
                kept.push(record);
            }
            Ok(_) | Err(_) => {
                stale += 1;
            }
        }
    }
    eprintln!(
        "Filtered records to {} active satellites (skipped {} decayed, {} stale)",
        kept.len(),
        decayed,
        stale
    );
    kept
}

fn compare_norad(left: &str, right: &str) -> std::cmp::Ordering {
    match (left.parse::<u64>(), right.parse::<u64>()) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => left.cmp(right),
    }
}

fn round_down_to_step(timestamp: DateTime<Utc>, step_minutes: i64) -> DateTime<Utc> {
    let minutes = timestamp.minute() as i64;
    let remainder = minutes % step_minutes;
    let truncated = timestamp
        .with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .unwrap();
    truncated - Duration::minutes(remainder)
}

fn parse_rfc3339_utc(value: &str) -> io::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to parse RFC3339 timestamp '{}': {}", value, error),
            )
        })
}

fn parse_iso_utc(value: &str) -> io::Result<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("failed to parse UTC timestamp '{}'", value),
    ))
}

fn parse_optional_utc(value: &str) -> Option<DateTime<Utc>> {
    if value.trim().is_empty() {
        return None;
    }
    parse_iso_utc(value)
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(&format!("{}T00:00:00", value), "%Y-%m-%dT%H:%M:%S")
                .ok()
                .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        })
}

fn format_iso_utc(value: &DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

fn load_eop_records(path: &Path) -> io::Result<Vec<EopRecord>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty EOP CSV"))?;
    let headers: Vec<&str> = header.split(';').collect();
    let mjd_idx = column_index(&headers, "MJD")?;
    let xp_idx = column_index(&headers, "x_pole")?;
    let yp_idx = column_index(&headers, "y_pole")?;
    let ut1_idx = column_index(&headers, "UT1-UTC")?;
    let lod_idx = column_index(&headers, "LOD")?;

    let mut records = Vec::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(';').collect();
        records.push(EopRecord {
            mjd_utc: parse_field(&fields, mjd_idx, "MJD")?,
            xp_arcsec: parse_field(&fields, xp_idx, "x_pole")?,
            yp_arcsec: parse_field(&fields, yp_idx, "y_pole")?,
            ut1_utc_seconds: parse_field(&fields, ut1_idx, "UT1-UTC")?,
            lod_seconds: parse_field(&fields, lod_idx, "LOD")?,
        });
    }
    if records.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "EOP CSV contained no rows",
        ));
    }
    Ok(records)
}

fn interpolate_eop(records: &[EopRecord], mjd_utc: f64) -> io::Result<EopSample> {
    if mjd_utc <= records[0].mjd_utc {
        return Ok(EopSample {
            xp_rad: records[0].xp_arcsec * ARCSEC_TO_RAD,
            yp_rad: records[0].yp_arcsec * ARCSEC_TO_RAD,
            ut1_utc_seconds: records[0].ut1_utc_seconds,
            lod_seconds: records[0].lod_seconds,
        });
    }
    if mjd_utc >= records[records.len() - 1].mjd_utc {
        let last = records[records.len() - 1];
        return Ok(EopSample {
            xp_rad: last.xp_arcsec * ARCSEC_TO_RAD,
            yp_rad: last.yp_arcsec * ARCSEC_TO_RAD,
            ut1_utc_seconds: last.ut1_utc_seconds,
            lod_seconds: last.lod_seconds,
        });
    }
    for window in records.windows(2) {
        let start = window[0];
        let end = window[1];
        if mjd_utc >= start.mjd_utc && mjd_utc <= end.mjd_utc {
            let fraction = (mjd_utc - start.mjd_utc) / (end.mjd_utc - start.mjd_utc);
            return Ok(EopSample {
                xp_rad: lerp(start.xp_arcsec, end.xp_arcsec, fraction) * ARCSEC_TO_RAD,
                yp_rad: lerp(start.yp_arcsec, end.yp_arcsec, fraction) * ARCSEC_TO_RAD,
                ut1_utc_seconds: lerp(start.ut1_utc_seconds, end.ut1_utc_seconds, fraction),
                lod_seconds: lerp(start.lod_seconds, end.lod_seconds, fraction),
            });
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("failed to interpolate EOP at MJD {}", mjd_utc),
    ))
}

fn teme_to_ecef(
    r_teme: [f64; 3],
    v_teme: [f64; 3],
    jdut1: f64,
    lod_seconds: f64,
    xp_rad: f64,
    yp_rad: f64,
) -> ([f64; 3], [f64; 3]) {
    let gmst = gstime(jdut1);
    let st = [
        [gmst.cos(), -gmst.sin(), 0.0],
        [gmst.sin(), gmst.cos(), 0.0],
        [0.0, 0.0, 1.0],
    ];
    let pm = polar_motion_80(xp_rad, yp_rad);
    let theta_sa = 7.29211514670698e-05 * (1.0 - lod_seconds / 86400.0);
    let omega_earth = [0.0, 0.0, theta_sa];
    let r_pef = transpose_mul(&st, &r_teme);
    let r_ecef = transpose_mul(&pm, &r_pef);
    let v_pef = sub_vec(transpose_mul(&st, &v_teme), cross(&omega_earth, &r_pef));
    let v_ecef = transpose_mul(&pm, &v_pef);
    (r_ecef, v_ecef)
}

fn polar_motion_80(xp_rad: f64, yp_rad: f64) -> [[f64; 3]; 3] {
    let cosxp = xp_rad.cos();
    let sinxp = xp_rad.sin();
    let cosyp = yp_rad.cos();
    let sinyp = yp_rad.sin();
    [
        [cosxp, 0.0, -sinxp],
        [sinxp * sinyp, cosyp, cosxp * sinyp],
        [sinxp * cosyp, -sinyp, cosxp * cosyp],
    ]
}

fn transpose_mul(matrix: &[[f64; 3]; 3], vector: &[f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[1][0] * vector[1] + matrix[2][0] * vector[2],
        matrix[0][1] * vector[0] + matrix[1][1] * vector[1] + matrix[2][1] * vector[2],
        matrix[0][2] * vector[0] + matrix[1][2] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn cross(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn sub_vec(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

fn ecef_to_geodetic(ecef: [f64; 3]) -> (f64, f64, f64) {
    let x = ecef[0];
    let y = ecef[1];
    let z = ecef[2];
    let lon = y.atan2(x);
    let e2 = WGS84_F * (2.0 - WGS84_F);
    let p = (x * x + y * y).sqrt();
    let mut lat = z.atan2(p * (1.0 - e2));
    for _ in 0..6 {
        let sin_lat = lat.sin();
        let n = WGS84_A_KM / (1.0 - e2 * sin_lat * sin_lat).sqrt();
        lat = (z + e2 * n * sin_lat).atan2(p);
    }
    let sin_lat = lat.sin();
    let n = WGS84_A_KM / (1.0 - e2 * sin_lat * sin_lat).sqrt();
    let alt = p / lat.cos() - n;
    (lat.to_degrees(), lon.to_degrees(), alt)
}

fn datetime_to_jd(timestamp: &DateTime<Utc>) -> f64 {
    let seconds = timestamp.second() as f64 + timestamp.nanosecond() as f64 * 1.0e-9;
    let (jd, jdfrac) = jday(
        timestamp.year(),
        timestamp.month() as i32,
        timestamp.day() as i32,
        timestamp.hour() as i32,
        timestamp.minute() as i32,
        seconds,
    );
    jd + jdfrac
}

fn duration_to_minutes(duration: Duration) -> io::Result<f64> {
    let nanos = duration.num_nanoseconds().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "duration was out of range for nanosecond conversion",
        )
    })?;
    Ok(nanos as f64 / 60.0 / 1.0e9)
}

fn column_index(headers: &[&str], name: &str) -> io::Result<usize> {
    headers.iter().position(|header| *header == name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("missing column {}", name),
        )
    })
}

fn parse_field(fields: &[&str], index: usize, name: &str) -> io::Result<f64> {
    fields
        .get(index)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing field {}", name),
            )
        })?
        .trim()
        .parse::<f64>()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse {}: {}", name, error),
            )
        })
}

fn lerp(start: f64, end: f64, fraction: f64) -> f64 {
    start + (end - start) * fraction
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_nullable(value: Option<&str>) -> String {
    value.map(sql_string).unwrap_or_else(|| "NULL".to_string())
}

fn sql_nullable_path(value: Option<&PathBuf>) -> String {
    value.map(|path| sql_string(&path.display().to_string()))
        .unwrap_or_else(|| "NULL".to_string())
}

fn js_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn js_nullable(value: Option<&str>) -> String {
    value
        .map(|text| format!("\"{}\"", js_escape(text)))
        .unwrap_or_else(|| "null".to_string())
}
