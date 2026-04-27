use crate::sgp4::sgp4::{gstime, jday};
use crate::sgp4::tle::TLE;
use crate::starlink::csv::{
    build_header_map, csv_escape, get_field, parse_csv_line, require_column,
};
use crate::starlink::manifest::{load_catalog_rows, CatalogRow};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
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
const INITIAL_WINDOW_PAST_HOURS: i64 = 6;
const INITIAL_WINDOW_FUTURE_HOURS: i64 = 6;
const EVENT_WINDOW_LEAD_MINUTES: i64 = 30;
const EVENT_WINDOW_FOLLOW_HOURS: i64 = 2;
const DEFAULT_MAX_GENERATED_LAUNCH_EVENTS: usize = 1;
const DEFAULT_MAX_GENERATED_DECAY_EVENTS: usize = 1;

#[derive(Debug)]
struct Config {
    catalog: PathBuf,
    group1_history: PathBuf,
    group4_history_root: PathBuf,
    eop: PathBuf,
    output_dir: PathBuf,
    center_utc: Option<DateTime<Utc>>,
    step_minutes: i64,
    cell_degrees: i32,
    max_launch_events: usize,
    max_decay_events: usize,
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
struct CellGrid {
    cells: Vec<Cell>,
    lat_steps: usize,
    lon_steps: usize,
    cell_degrees: f64,
}

#[derive(Clone, Debug)]
struct FrameCellVisibility {
    group1_counts: Vec<u16>,
    group4_counts: Vec<u16>,
}

#[derive(Clone, Copy, Debug)]
struct FrameContext {
    frame_utc: DateTime<Utc>,
    jdut1: f64,
    lod_seconds: f64,
    xp_rad: f64,
    yp_rad: f64,
}

#[derive(Clone, Debug)]
struct TimelineEvent {
    event_id: String,
    event_type: String,
    label: String,
    shell_id: String,
    group_slug: Option<String>,
    time_utc: DateTime<Utc>,
    satellite_count: usize,
    highlight_norads: Vec<String>,
    chunk_id: String,
    chunk_path: String,
}

#[derive(Clone, Debug)]
struct WindowChunk {
    chunk_id: String,
    label: String,
    event_type: Option<String>,
    focus_utc: DateTime<Utc>,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    highlight_time_utc: Option<DateTime<Utc>>,
    highlight_norads: Vec<String>,
    frame_times: Vec<DateTime<Utc>>,
    tracks: Vec<SatelliteTrack>,
    visibility: Vec<FrameCellVisibility>,
}

#[derive(Clone, Debug)]
struct ChunkRequest {
    chunk_id: String,
    label: String,
    event_type: Option<String>,
    focus_utc: DateTime<Utc>,
    selection_utc: DateTime<Utc>,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    highlight_time_utc: Option<DateTime<Utc>>,
    highlight_norads: Vec<String>,
}

#[derive(Clone, Debug)]
struct HistoryAnalysis {
    shell_max_epochs: BTreeMap<String, String>,
    earliest_decay_by_norad: HashMap<String, DateTime<Utc>>,
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

    let history_analysis = analyze_history_inputs(&datasets)?;
    let latest_common_utc = determine_latest_common_utc(&history_analysis.shell_max_epochs)?;
    let center_utc = config
        .center_utc
        .unwrap_or_else(|| round_down_to_step(latest_common_utc, config.step_minutes));

    let cell_grid = build_cells(config.cell_degrees);
    let eop_records = load_eop_records(&config.eop)?;
    let events = build_timeline_events(
        &datasets,
        &catalog_rows,
        &history_analysis.earliest_decay_by_norad,
    )?;
    let generated_events = select_generated_events(
        &events,
        center_utc,
        config.max_launch_events,
        config.max_decay_events,
    );
    let mut chunk_requests = Vec::with_capacity(generated_events.len() + 1);
    chunk_requests.push(build_initial_chunk_request(center_utc));
    chunk_requests.extend(generated_events.iter().map(build_event_chunk_request));
    let latest_records_by_request =
        select_latest_tles_for_requests(&datasets, &catalog_rows, &chunk_requests)?;
    let mut chunks = build_chunks_from_requests(
        &chunk_requests,
        latest_records_by_request,
        &cell_grid,
        &eop_records,
        config.step_minutes,
    )?;
    let initial_chunk = chunks.remove(0);
    if initial_chunk.tracks.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no TLEs were found for the requested center UTC",
        ));
    }
    let event_chunks = chunks;
    let shell_info = build_shell_info(
        &datasets,
        &history_analysis.shell_max_epochs,
        &initial_chunk.tracks,
    );

    let summary_csv_path = config.output_dir.join("shell_summary.csv");
    let db_path = config.output_dir.join("starlink_shell_map.sqlite");
    let data_js_path = config.output_dir.join("data.js");
    let html_path = config.output_dir.join("index.html");
    let chunk_dir = config.output_dir.join("chunks");

    write_summary_csv(&summary_csv_path, &shell_info)?;
    write_sqlite_database(
        &db_path,
        &shell_info,
        &datasets,
        &initial_chunk.tracks,
        &initial_chunk.frame_times,
        &cell_grid.cells,
        &initial_chunk.visibility,
        &generated_events,
        center_utc,
        latest_common_utc,
    )?;
    write_data_js(
        &data_js_path,
        &shell_info,
        &cell_grid.cells,
        &generated_events,
        center_utc,
        latest_common_utc,
        config.step_minutes,
        &initial_chunk.chunk_id,
    )?;
    fs::create_dir_all(&chunk_dir)?;
    write_chunk_js(
        &chunk_dir.join(format!("{}.js", initial_chunk.chunk_id)),
        &initial_chunk,
    )?;
    write_html(
        &html_path,
        &shell_info,
        center_utc,
        config.step_minutes,
        config.cell_degrees,
        &format!("chunks/{}.js", initial_chunk.chunk_id),
    )?;
    for chunk in &event_chunks {
        write_chunk_js(&chunk_dir.join(format!("{}.js", chunk.chunk_id)), chunk)?;
    }

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
    let mut step_minutes = 5i64;
    let mut cell_degrees = 5i32;
    let mut max_launch_events = DEFAULT_MAX_GENERATED_LAUNCH_EVENTS;
    let mut max_decay_events = DEFAULT_MAX_GENERATED_DECAY_EVENTS;

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
            "--max-launch-events" => {
                max_launch_events = next_arg(&mut args, "--max-launch-events")?
                    .parse::<usize>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --max-launch-events: {}", error),
                        )
                    })?;
            }
            "--max-decay-events" => {
                max_decay_events = next_arg(&mut args, "--max-decay-events")?
                    .parse::<usize>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --max-decay-events: {}", error),
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
        step_minutes,
        cell_degrees,
        max_launch_events,
        max_decay_events,
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
    println!("  --step-minutes N");
    println!("  --max-launch-events N");
    println!("  --max-decay-events N");
    println!("  --cell-degrees N");
    println!();
    println!("Defaults:");
    println!("  catalog: data/starlink_group_catalog.csv");
    println!("  group1-history: starlink-group-1");
    println!("  group4-history-root: starlink-group-4");
    println!("  eop: eop/eopc04_20u24.1962-now.csv");
    println!("  output-dir: data/starlink_shell_map");
    println!("  center-utc: latest common shell epoch rounded down to step");
    println!("  step-minutes: 5");
    println!(
        "  max-launch-events: {}",
        DEFAULT_MAX_GENERATED_LAUNCH_EVENTS
    );
    println!("  max-decay-events: {}", DEFAULT_MAX_GENERATED_DECAY_EVENTS);
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
        phase1_groups
            .entry(row.group_slug.clone())
            .or_insert(GroupInfo {
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

fn analyze_history_inputs(datasets: &[ShellDataset]) -> io::Result<HistoryAnalysis> {
    let mut shell_max_epochs = BTreeMap::new();
    let mut earliest_decay_by_norad = HashMap::new();
    for dataset in datasets {
        let mut shell_max = None::<String>;
        for input in &dataset.inputs {
            eprintln!("Analyzing history input {}", input.path.display());
            let file_max =
                analyze_history_input(&input.path, &input.norad_ids, &mut earliest_decay_by_norad)?;
            if let Some(epoch) = file_max {
                if shell_max
                    .as_ref()
                    .map(|current| &epoch > current)
                    .unwrap_or(true)
                {
                    shell_max = Some(epoch);
                }
            }
        }
        if let Some(max_epoch) = shell_max {
            shell_max_epochs.insert(dataset.shell_id.clone(), max_epoch);
        }
    }
    Ok(HistoryAnalysis {
        shell_max_epochs,
        earliest_decay_by_norad,
    })
}

fn analyze_history_input(
    path: &Path,
    norad_ids: &HashSet<String>,
    earliest_decay_by_norad: &mut HashMap<String, DateTime<Utc>>,
) -> io::Result<Option<String>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "history CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(&header));
    let norad_index = require_column(&header_map, "NORAD_CAT_ID")?;
    let epoch_index = require_column(&header_map, "EPOCH")?;
    let decay_index = require_column(&header_map, "DECAY_DATE")?;

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
        } else if latest
            .as_ref()
            .map(|current| &epoch > current)
            .unwrap_or(true)
        {
            latest = Some(epoch);
        }
        if let Some(decay_utc) = parse_optional_utc(&get_field(&fields, decay_index, "DECAY_DATE")?)
        {
            earliest_decay_by_norad
                .entry(norad)
                .and_modify(|current| {
                    if decay_utc < *current {
                        *current = decay_utc;
                    }
                })
                .or_insert(decay_utc);
        }
    }
    Ok(latest)
}

fn determine_latest_common_utc(
    shell_max_epochs: &BTreeMap<String, String>,
) -> io::Result<DateTime<Utc>> {
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

fn select_latest_tles_for_requests(
    datasets: &[ShellDataset],
    catalog_rows: &[CatalogRow],
    requests: &[ChunkRequest],
) -> io::Result<Vec<Vec<LatestTleRecord>>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let mut catalog_by_norad = HashMap::<String, CatalogRow>::new();
    let mut shell_by_norad = HashMap::<String, (String, String, String)>::new();
    for row in catalog_rows {
        if let Some((shell_id, display_name, color)) = shell_identity(&row.group_family) {
            catalog_by_norad.insert(row.norad_cat_id.clone(), row.clone());
            shell_by_norad.insert(
                row.norad_cat_id.clone(),
                (
                    shell_id.to_string(),
                    display_name.to_string(),
                    color.to_string(),
                ),
            );
        }
    }

    let mut indexed_anchors = requests
        .iter()
        .enumerate()
        .map(|(index, request)| (index, format_iso_utc(&request.selection_utc)))
        .collect::<Vec<_>>();
    indexed_anchors.sort_by(|left, right| left.1.cmp(&right.1));
    let anchor_texts = indexed_anchors
        .iter()
        .map(|(_, anchor_text)| anchor_text.clone())
        .collect::<Vec<_>>();
    let max_anchor_text = anchor_texts.last().cloned().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "missing chunk request anchor")
    })?;
    let mut found_by_request =
        vec![BTreeMap::<String, LatestTleRecord>::new(); indexed_anchors.len()];
    for dataset in datasets {
        for input in &dataset.inputs {
            eprintln!(
                "Selecting TLEs for {} chunk requests from {}",
                anchor_texts.len(),
                input.path.display()
            );
            select_latest_tles_for_requests_from_history(
                &input.path,
                &input.norad_ids,
                &anchor_texts,
                &max_anchor_text,
                &catalog_by_norad,
                &shell_by_norad,
                &mut found_by_request,
            )?;
        }
    }

    let mut ordered_records = vec![None; requests.len()];
    for ((request_index, _), found) in indexed_anchors
        .into_iter()
        .zip(found_by_request.into_iter())
    {
        ordered_records[request_index] = Some(found.into_values().collect());
    }
    Ok(ordered_records
        .into_iter()
        .map(|records| records.unwrap_or_default())
        .collect())
}

fn select_latest_tles_for_requests_from_history(
    path: &Path,
    norad_ids: &HashSet<String>,
    anchor_texts: &[String],
    max_anchor_text: &str,
    catalog_by_norad: &HashMap<String, CatalogRow>,
    shell_by_norad: &HashMap<String, (String, String, String)>,
    found_by_request: &mut [BTreeMap<String, LatestTleRecord>],
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
        if epoch_text.is_empty() || epoch_text.as_str() > max_anchor_text {
            continue;
        }
        let first_anchor_index =
            anchor_texts.partition_point(|anchor_text| anchor_text < &epoch_text);
        if first_anchor_index >= anchor_texts.len() {
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
        let creation_date_text = get_field(&fields, creation_index, "CREATION_DATE")?;
        let decay_date_text = get_field(&fields, decay_index, "DECAY_DATE")?;
        let line1 = get_field(&fields, line1_index, "TLE_LINE1")?;
        let line2 = get_field(&fields, line2_index, "TLE_LINE2")?;
        let record = LatestTleRecord {
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
        };
        for found in &mut found_by_request[first_anchor_index..] {
            upsert_latest_tle_record(found, record.clone());
        }
    }
    Ok(())
}

fn upsert_latest_tle_record(
    found: &mut BTreeMap<String, LatestTleRecord>,
    record: LatestTleRecord,
) {
    let better = match found.get(&record.norad_cat_id) {
        Some(existing) => {
            record.epoch_text > existing.epoch_text
                || (record.epoch_text == existing.epoch_text
                    && record.creation_date_text > existing.creation_date_text)
        }
        None => true,
    };
    if better {
        found.insert(record.norad_cat_id.clone(), record);
    }
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

fn build_initial_chunk_request(center_utc: DateTime<Utc>) -> ChunkRequest {
    ChunkRequest {
        chunk_id: "chunk_initial".to_string(),
        label: format!("Initial window around {}", center_utc.to_rfc3339()),
        event_type: None,
        focus_utc: center_utc,
        selection_utc: center_utc,
        start_utc: center_utc - Duration::hours(INITIAL_WINDOW_PAST_HOURS),
        end_utc: center_utc + Duration::hours(INITIAL_WINDOW_FUTURE_HOURS),
        highlight_time_utc: None,
        highlight_norads: Vec::new(),
    }
}

fn build_event_chunk_request(event: &TimelineEvent) -> ChunkRequest {
    ChunkRequest {
        chunk_id: event.chunk_id.clone(),
        label: event.label.clone(),
        event_type: Some(event.event_type.clone()),
        focus_utc: event.time_utc,
        selection_utc: if event.event_type == "launch" {
            event.time_utc + Duration::days(7)
        } else {
            event.time_utc - Duration::minutes(1)
        },
        start_utc: event.time_utc - Duration::minutes(EVENT_WINDOW_LEAD_MINUTES),
        end_utc: event.time_utc + Duration::hours(EVENT_WINDOW_FOLLOW_HOURS),
        highlight_time_utc: Some(event.time_utc),
        highlight_norads: event.highlight_norads.clone(),
    }
}

fn build_chunks_from_requests(
    requests: &[ChunkRequest],
    latest_records_by_request: Vec<Vec<LatestTleRecord>>,
    cell_grid: &CellGrid,
    eop_records: &[EopRecord],
    step_minutes: i64,
) -> io::Result<Vec<WindowChunk>> {
    let mut chunks = Vec::with_capacity(requests.len());
    for (index, (request, latest_records)) in
        requests.iter().zip(latest_records_by_request).enumerate()
    {
        if index > 0 {
            eprintln!(
                "Building chunk {} / {}: {} at {}",
                index,
                requests.len() - 1,
                request.event_type.as_deref().unwrap_or("window"),
                request.focus_utc.to_rfc3339()
            );
        }
        let frame_times = build_frame_times(request.start_utc, request.end_utc, step_minutes)?;
        let latest_records = filter_active_records(latest_records, request.focus_utc);
        let frame_contexts = build_frame_contexts(&frame_times, eop_records)?;
        let (tracks, visibility) =
            compute_tracks_and_visibility(&latest_records, &frame_contexts, cell_grid)?;
        chunks.push(WindowChunk {
            chunk_id: request.chunk_id.clone(),
            label: request.label.clone(),
            event_type: request.event_type.clone(),
            focus_utc: request.focus_utc,
            start_utc: request.start_utc,
            end_utc: request.end_utc,
            highlight_time_utc: request.highlight_time_utc,
            highlight_norads: request.highlight_norads.clone(),
            frame_times,
            tracks,
            visibility,
        });
    }
    Ok(chunks)
}

fn build_timeline_events(
    datasets: &[ShellDataset],
    catalog_rows: &[CatalogRow],
    earliest_decay_by_norad: &HashMap<String, DateTime<Utc>>,
) -> io::Result<Vec<TimelineEvent>> {
    let mut events = Vec::new();
    for dataset in datasets {
        for group in &dataset.groups {
            if let Some(launch_utc) = parse_launch_date_utc(&group.launch_date) {
                events.push(TimelineEvent {
                    event_id: String::new(),
                    event_type: "launch".to_string(),
                    label: format!(
                        "{} / {} / {} satellites",
                        group.group_name, dataset.display_name, group.satellite_count
                    ),
                    shell_id: dataset.shell_id.clone(),
                    group_slug: Some(group.group_slug.clone()),
                    time_utc: launch_utc,
                    satellite_count: group.satellite_count,
                    highlight_norads: catalog_rows
                        .iter()
                        .filter(|row| row.group_slug == group.group_slug)
                        .map(|row| row.norad_cat_id.clone())
                        .collect(),
                    chunk_id: String::new(),
                    chunk_path: String::new(),
                });
            }
        }
    }

    let mut catalog_by_norad = HashMap::<String, CatalogRow>::new();
    for row in catalog_rows {
        if shell_identity(&row.group_family).is_some() {
            catalog_by_norad.insert(row.norad_cat_id.clone(), row.clone());
        }
    }
    let mut grouped_decay_rows = BTreeMap::<DateTime<Utc>, Vec<&CatalogRow>>::new();
    for (norad, decay_utc) in earliest_decay_by_norad {
        if let Some(catalog) = catalog_by_norad.get(norad) {
            grouped_decay_rows
                .entry(*decay_utc)
                .or_default()
                .push(catalog);
        }
    }
    for (decay_utc, rows) in grouped_decay_rows {
        let shell_id = rows
            .first()
            .and_then(|row| shell_identity(&row.group_family))
            .map(|(shell_id, _, _)| shell_id.to_string())
            .unwrap_or_else(|| "mixed".to_string());
        events.push(TimelineEvent {
            event_id: String::new(),
            event_type: "decay".to_string(),
            label: format!("{} satellites decaying", rows.len()),
            shell_id,
            group_slug: None,
            time_utc: decay_utc,
            satellite_count: rows.len(),
            highlight_norads: rows.iter().map(|row| row.norad_cat_id.clone()).collect(),
            chunk_id: String::new(),
            chunk_path: String::new(),
        });
    }
    events.sort_by(|left, right| {
        left.time_utc
            .cmp(&right.time_utc)
            .then_with(|| left.event_type.cmp(&right.event_type))
            .then_with(|| left.label.cmp(&right.label))
    });

    for (index, event) in events.iter_mut().enumerate() {
        event.event_id = format!("event_{index:04}");
        event.chunk_id = format!("chunk_event_{index:04}");
        event.chunk_path = format!("chunks/{}.js", event.chunk_id);
    }
    Ok(events)
}

fn select_generated_events(
    events: &[TimelineEvent],
    center_utc: DateTime<Utc>,
    max_launch_events: usize,
    max_decay_events: usize,
) -> Vec<TimelineEvent> {
    let mut launches = Vec::new();
    let mut decays = Vec::new();
    for event in events {
        if event.time_utc <= center_utc {
            continue;
        }
        if event.event_type == "launch" && launches.len() < max_launch_events {
            launches.push(event.clone());
        } else if event.event_type == "decay" && decays.len() < max_decay_events {
            decays.push(event.clone());
        }
        if launches.len() >= max_launch_events && decays.len() >= max_decay_events {
            break;
        }
    }
    let mut selected = launches;
    selected.extend(decays);
    selected.sort_by(|left, right| {
        left.time_utc
            .cmp(&right.time_utc)
            .then_with(|| left.event_type.cmp(&right.event_type))
            .then_with(|| left.label.cmp(&right.label))
    });
    selected
}

fn build_cells(cell_degrees: i32) -> CellGrid {
    let mut cells = Vec::new();
    let lat_steps = (180 / cell_degrees) as usize;
    let lon_steps = (360 / cell_degrees) as usize;
    let cell_degrees_f64 = cell_degrees as f64;
    for lat_index in 0..lat_steps {
        let lat_min_deg = -90.0 + lat_index as f64 * cell_degrees_f64;
        let lat_max_deg = lat_min_deg + cell_degrees_f64;
        let lat_center_deg = (lat_min_deg + lat_max_deg) * 0.5;
        let lat_rad = lat_center_deg.to_radians();
        let cos_lat = lat_rad.cos();
        let sin_lat = lat_rad.sin();
        for lon_index in 0..lon_steps {
            let lon_min_deg = -180.0 + lon_index as f64 * cell_degrees_f64;
            let lon_max_deg = lon_min_deg + cell_degrees_f64;
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
    CellGrid {
        cells,
        lat_steps,
        lon_steps,
        cell_degrees: cell_degrees_f64,
    }
}

fn build_frame_contexts(
    frame_times: &[DateTime<Utc>],
    eop_records: &[EopRecord],
) -> io::Result<Vec<FrameContext>> {
    let mut contexts = Vec::with_capacity(frame_times.len());
    for frame_utc in frame_times {
        let mjd_utc = datetime_to_jd(frame_utc) - MJD_OFFSET;
        let eop = interpolate_eop(eop_records, mjd_utc)?;
        let jd_utc = datetime_to_jd(frame_utc);
        contexts.push(FrameContext {
            frame_utc: *frame_utc,
            jdut1: jd_utc + eop.ut1_utc_seconds / 86400.0,
            lod_seconds: eop.lod_seconds,
            xp_rad: eop.xp_rad,
            yp_rad: eop.yp_rad,
        });
    }
    Ok(contexts)
}

fn candidate_cell_indices(
    cell_grid: &CellGrid,
    sat_lat_deg: f64,
    sat_lon_deg: f64,
    altitude_km: f64,
) -> Vec<usize> {
    let mut indices = Vec::new();
    let sat_radius_km = EARTH_RADIUS_KM + altitude_km.max(0.0);
    let horizon_angle_rad = (EARTH_RADIUS_KM / sat_radius_km).clamp(-1.0, 1.0).acos();
    let horizon_angle_deg = horizon_angle_rad.to_degrees();
    let lat_min_deg = (sat_lat_deg - horizon_angle_deg).max(-90.0);
    let lat_max_deg = (sat_lat_deg + horizon_angle_deg).min(90.0);
    let row_start = (((lat_min_deg + 90.0) / cell_grid.cell_degrees).floor() as isize)
        .clamp(0, cell_grid.lat_steps as isize - 1) as usize;
    let row_end = (((lat_max_deg + 90.0) / cell_grid.cell_degrees).floor() as isize)
        .clamp(0, cell_grid.lat_steps as isize - 1) as usize;
    let sat_lon_norm = wrap_lon_360(sat_lon_deg + 180.0);

    for row in row_start..=row_end {
        let row_center_deg = -90.0 + (row as f64 + 0.5) * cell_grid.cell_degrees;
        let cos_lat = row_center_deg.to_radians().cos().abs();
        let lon_span_deg = if cos_lat < 1e-6 {
            180.0
        } else {
            (horizon_angle_deg / cos_lat).min(180.0)
        };
        if lon_span_deg >= 180.0 {
            for col in 0..cell_grid.lon_steps {
                indices.push(row * cell_grid.lon_steps + col);
            }
            continue;
        }

        let start_norm = sat_lon_norm - lon_span_deg;
        let end_norm = sat_lon_norm + lon_span_deg;
        push_wrapped_columns(
            &mut indices,
            row,
            start_norm,
            end_norm,
            cell_grid.lon_steps,
            cell_grid.cell_degrees,
        );
    }

    indices
}

fn push_wrapped_columns(
    indices: &mut Vec<usize>,
    row: usize,
    start_norm: f64,
    end_norm: f64,
    lon_steps: usize,
    cell_degrees: f64,
) {
    if start_norm < 0.0 {
        push_column_range(
            indices,
            row,
            start_norm + 360.0,
            360.0 - f64::EPSILON,
            lon_steps,
            cell_degrees,
        );
        push_column_range(indices, row, 0.0, end_norm, lon_steps, cell_degrees);
    } else if end_norm >= 360.0 {
        push_column_range(
            indices,
            row,
            start_norm,
            360.0 - f64::EPSILON,
            lon_steps,
            cell_degrees,
        );
        push_column_range(indices, row, 0.0, end_norm - 360.0, lon_steps, cell_degrees);
    } else {
        push_column_range(indices, row, start_norm, end_norm, lon_steps, cell_degrees);
    }
}

fn push_column_range(
    indices: &mut Vec<usize>,
    row: usize,
    start_norm: f64,
    end_norm: f64,
    lon_steps: usize,
    cell_degrees: f64,
) {
    let col_start = (start_norm / cell_degrees)
        .floor()
        .clamp(0.0, lon_steps as f64 - 1.0) as usize;
    let col_end = (end_norm / cell_degrees)
        .floor()
        .clamp(0.0, lon_steps as f64 - 1.0) as usize;
    for col in col_start..=col_end {
        indices.push(row * lon_steps + col);
    }
}

fn wrap_lon_360(value: f64) -> f64 {
    let mut wrapped = value % 360.0;
    if wrapped < 0.0 {
        wrapped += 360.0;
    }
    wrapped
}

fn compute_tracks_and_visibility(
    records: &[LatestTleRecord],
    frame_contexts: &[FrameContext],
    cell_grid: &CellGrid,
) -> io::Result<(Vec<SatelliteTrack>, Vec<FrameCellVisibility>)> {
    let mut tracks = Vec::new();
    let mut visibility = frame_contexts
        .iter()
        .map(|_| FrameCellVisibility {
            group1_counts: vec![0u16; cell_grid.cells.len()],
            group4_counts: vec![0u16; cell_grid.cells.len()],
        })
        .collect::<Vec<_>>();

    for (index, record) in records.iter().enumerate() {
        if index % 100 == 0 {
            eprintln!("Propagating satellite {} / {}", index + 1, records.len());
        }
        let mut tle = TLE::new();
        tle.name = record.satname.clone();
        tle.parse_lines(&record.tle_line1, &record.tle_line2);
        let mut samples = Vec::with_capacity(frame_contexts.len());
        let mut failed = false;
        for (frame_index, frame_context) in frame_contexts.iter().enumerate() {
            let mins_after_epoch = duration_to_minutes(frame_context.frame_utc - tle.epoch)?;
            let (r_teme, v_teme) = tle.get_rv(mins_after_epoch);
            if tle.sgp4_error != 0 {
                eprintln!(
                    "Skipping NORAD {} due to SGP4 error {} at {}",
                    record.norad_cat_id,
                    tle.sgp4_error,
                    frame_context.frame_utc.to_rfc3339()
                );
                failed = true;
                break;
            }
            let (r_ecef, _) = teme_to_ecef(
                r_teme,
                v_teme,
                frame_context.jdut1,
                frame_context.lod_seconds,
                frame_context.xp_rad,
                frame_context.yp_rad,
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
            if !record_is_active_at(record, frame_context.frame_utc) {
                continue;
            }
            for cell_index in candidate_cell_indices(cell_grid, lat_deg, lon_deg, altitude_km) {
                let cell = &cell_grid.cells[cell_index];
                let visible =
                    r_ecef[0] * cell.unit_x + r_ecef[1] * cell.unit_y + r_ecef[2] * cell.unit_z
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
            expected_satellite_count: dataset
                .groups
                .iter()
                .map(|group| group.satellite_count)
                .sum(),
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
    events: &[TimelineEvent],
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
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "failed to open sqlite3 stdin"))?;

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
    writeln!(
        stdin,
        "CREATE TABLE launch_events (event_id TEXT PRIMARY KEY, shell_id TEXT NOT NULL, group_slug TEXT, event_utc TEXT NOT NULL, label TEXT NOT NULL, satellite_count INTEGER NOT NULL, chunk_id TEXT NOT NULL, chunk_path TEXT NOT NULL);"
    )?;
    writeln!(
        stdin,
        "CREATE TABLE decay_events (event_id TEXT PRIMARY KEY, shell_id TEXT NOT NULL, event_utc TEXT NOT NULL, label TEXT NOT NULL, satellite_count INTEGER NOT NULL, chunk_id TEXT NOT NULL, chunk_path TEXT NOT NULL);"
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
    for event in events {
        if event.event_type == "launch" {
            writeln!(
                stdin,
                "INSERT INTO launch_events VALUES ({}, {}, {}, {}, {}, {}, {}, {});",
                sql_string(&event.event_id),
                sql_string(&event.shell_id),
                sql_nullable(event.group_slug.as_deref()),
                sql_string(&event.time_utc.to_rfc3339()),
                sql_string(&event.label),
                event.satellite_count,
                sql_string(&event.chunk_id),
                sql_string(&event.chunk_path),
            )?;
        } else if event.event_type == "decay" {
            writeln!(
                stdin,
                "INSERT INTO decay_events VALUES ({}, {}, {}, {}, {}, {}, {});",
                sql_string(&event.event_id),
                sql_string(&event.shell_id),
                sql_string(&event.time_utc.to_rfc3339()),
                sql_string(&event.label),
                event.satellite_count,
                sql_string(&event.chunk_id),
                sql_string(&event.chunk_path),
            )?;
        }
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
    writeln!(
        stdin,
        "CREATE INDEX idx_frame_samples_shell ON frame_samples (shell_id, frame_index);"
    )?;
    writeln!(
        stdin,
        "CREATE INDEX idx_cell_visibility_frame ON cell_visibility (frame_index);"
    )?;
    writeln!(
        stdin,
        "CREATE INDEX idx_launch_events_utc ON launch_events (event_utc);"
    )?;
    writeln!(
        stdin,
        "CREATE INDEX idx_decay_events_utc ON decay_events (event_utc);"
    )?;
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
    cells: &[Cell],
    events: &[TimelineEvent],
    center_utc: DateTime<Utc>,
    latest_common_utc: DateTime<Utc>,
    step_minutes: i64,
    initial_chunk_id: &str,
) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "window.STARLINK_SHELL_CHUNKS = window.STARLINK_SHELL_CHUNKS || {{}};"
    )?;
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
    writeln!(
        writer,
        "    initialChunkId: \"{}\"",
        js_escape(initial_chunk_id)
    )?;
    writeln!(writer, "  }},")?;

    writeln!(writer, "  shells: [")?;
    for item in shell_info {
        writeln!(
            writer,
            "    {{shellId:\"{}\",displayName:\"{}\",color:\"{}\",expectedGroups:{},availableGroups:{},expectedSatellites:{},availableSatellites:{},latestEpochUtc:{},missingGroups:[{}]}},",
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

    writeln!(writer, "  events: [")?;
    for event in events {
        writeln!(
            writer,
            "    {{eventId:\"{}\",type:\"{}\",label:\"{}\",shellId:\"{}\",groupSlug:{},timeUtc:\"{}\",satelliteCount:{},chunkId:\"{}\",chunkPath:\"{}\"}},",
            js_escape(&event.event_id),
            js_escape(&event.event_type),
            js_escape(&event.label),
            js_escape(&event.shell_id),
            js_nullable(event.group_slug.as_deref()),
            js_escape(&event.time_utc.to_rfc3339()),
            event.satellite_count,
            js_escape(&event.chunk_id),
            js_escape(&event.chunk_path),
        )?;
    }
    writeln!(writer, "  ]")?;
    writeln!(writer, "}};")?;
    writer.flush()
}

fn write_chunk_js(path: &Path, chunk: &WindowChunk) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "window.STARLINK_SHELL_CHUNKS = window.STARLINK_SHELL_CHUNKS || {{}};"
    )?;
    writeln!(
        writer,
        "window.STARLINK_SHELL_CHUNKS[\"{}\"] = {{",
        js_escape(&chunk.chunk_id)
    )?;
    writeln!(writer, "  chunkId: \"{}\",", js_escape(&chunk.chunk_id))?;
    writeln!(writer, "  label: \"{}\",", js_escape(&chunk.label))?;
    writeln!(
        writer,
        "  eventType: {},",
        js_nullable(chunk.event_type.as_deref())
    )?;
    writeln!(
        writer,
        "  focusUtc: \"{}\",",
        js_escape(&chunk.focus_utc.to_rfc3339())
    )?;
    writeln!(
        writer,
        "  startUtc: \"{}\",",
        js_escape(&chunk.start_utc.to_rfc3339())
    )?;
    writeln!(
        writer,
        "  endUtc: \"{}\",",
        js_escape(&chunk.end_utc.to_rfc3339())
    )?;
    writeln!(
        writer,
        "  highlightTimeUtc: {},",
        js_nullable(
            chunk
                .highlight_time_utc
                .map(|value| value.to_rfc3339())
                .as_deref()
        )
    )?;
    writeln!(
        writer,
        "  highlightNorads: [{}],",
        chunk
            .highlight_norads
            .iter()
            .map(|value| format!("\"{}\"", js_escape(value)))
            .collect::<Vec<_>>()
            .join(",")
    )?;
    writeln!(writer, "  frames: [")?;
    for frame in &chunk.frame_times {
        writeln!(writer, "    \"{}\",", js_escape(&frame.to_rfc3339()))?;
    }
    writeln!(writer, "  ],")?;

    writeln!(writer, "  heatmap: [")?;
    for frame_counts in &chunk.visibility {
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
        writeln!(writer, "    {{group1:[{}],group4:[{}]}},", group1, group4)?;
    }
    writeln!(writer, "  ],")?;

    writeln!(writer, "  satellites: [")?;
    for track in &chunk.tracks {
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
            "    {{norad:\"{}\",satname:\"{}\",objectName:\"{}\",shellId:\"{}\",displayName:\"{}\",color:\"{}\",groupSlug:\"{}\",groupName:\"{}\",launchDate:\"{}\",epochUtc:\"{}\",decayUtc:{},positions:[{}]}},",
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
            js_nullable(parse_optional_utc(&track.record.decay_date_text).map(|value| value.to_rfc3339()).as_deref()),
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
    initial_chunk_script: &str,
) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "<!DOCTYPE html>")?;
    writeln!(writer, "<html lang=\"en\">")?;
    writeln!(writer, "<head>")?;
    writeln!(writer, "<meta charset=\"utf-8\">")?;
    writeln!(
        writer,
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    )?;
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
.inline > *{flex:1 1 0}
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
    writeln!(writer, "<div class=\"card\"><h2>Playback</h2><div class=\"control-row\"><div class=\"inline\"><button id=\"playPause\">Pause</button><button id=\"projectionToggle\">Switch To Globe</button><select id=\"speedSelect\"><option value=\"0.03125\">0.03125x</option><option value=\"0.0625\">0.0625x</option><option value=\"0.125\">0.125x</option><option value=\"0.25\" selected>0.25x</option><option value=\"0.5\">0.5x</option><option value=\"1\">1x</option><option value=\"2\">2x</option><option value=\"4\">4x</option></select></div><div class=\"inline\"><button id=\"nextLaunch\">Next Launch</button><button id=\"nextDecay\">Next Decay</button></div><div class=\"muted\" id=\"eventSummary\">Loading next launch/decay dates...</div><label for=\"frameSlider\">Frame</label><input id=\"frameSlider\" type=\"range\" min=\"0\" max=\"0\" value=\"0\"></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2>Layers</h2><div class=\"control-row\"><label for=\"heatmapMode\">Heatmap</label><select id=\"heatmapMode\"><option value=\"diff\" selected>Group1 - Group4</option><option value=\"group1\">Group 1 visible count</option><option value=\"group4\">Group 4 visible count</option><option value=\"off\">Off</option></select><div class=\"checks\" id=\"shellChecks\"></div></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2 id=\"legendTitle\">Heatmap Legend</h2><div class=\"legend-scale\"><div class=\"legend-bar\" id=\"legendBar\"></div><div class=\"legend-labels\"><span id=\"legendMin\">Group 4 higher</span><span class=\"legend-mid\" id=\"legendMid\">equal</span><span id=\"legendMax\">Group 1 higher</span></div><div class=\"legend-note\" id=\"legendNote\">Colors are normalized to the current frame's peak absolute difference.</div></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2>Snapshot</h2><div class=\"stats\"><div class=\"stat\"><strong id=\"satelliteCount\">0</strong><span>Displayed Satellites</span></div><div class=\"stat\"><strong id=\"cellPeak\">0</strong><span>Peak Cell Count</span></div></div></div>")?;
    writeln!(writer, "<div class=\"card\"><h2>Hover</h2><div id=\"hover\">Move the pointer near a satellite.</div></div>")?;
    writeln!(
        writer,
        "<div class=\"card\"><h2>Coverage</h2><div class=\"shell-list\">"
    )?;
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
    writeln!(
        writer,
        "<script src=\"{}\"></script>",
        js_escape(initial_chunk_script)
    )?;
    writer.write_all(
        br#"<script>
const DATA = window.STARLINK_SHELL_DATA;
const CHUNKS = window.STARLINK_SHELL_CHUNKS || (window.STARLINK_SHELL_CHUNKS = {});
const WINDOW_STATE = { data: CHUNKS[DATA.meta.initialChunkId] };
DATA.meta.shells = DATA.shells;
Object.defineProperty(DATA, 'frames', { get() { return WINDOW_STATE.data.frames; } });
Object.defineProperty(DATA, 'heatmap', { get() { return WINDOW_STATE.data.heatmap; } });
Object.defineProperty(DATA, 'satellites', { get() { return WINDOW_STATE.data.satellites; } });
const canvas = document.getElementById('mapCanvas');
const ctx = canvas.getContext('2d');
const frameSlider = document.getElementById('frameSlider');
const playPause = document.getElementById('playPause');
const projectionToggle = document.getElementById('projectionToggle');
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
const nextLaunchBtn = document.getElementById('nextLaunch');
const nextDecayBtn = document.getElementById('nextDecay');
const eventSummaryEl = document.getElementById('eventSummary');

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
  projectionMode: 'map',
  globeYaw: -0.9,
  globePitch: 0.35,
  dragging: false,
  dragLastX: 0,
  dragLastY: 0,
  visibleSatelliteCache: null,
  segmentCache: new WeakMap(),
  events: { launches: [], decays: [] },
  frameTimesMs: [],
  frameStartMs: 0,
  frameEndMs: 0,
  frameStepMs: Math.max(60000, DATA.meta.stepMinutes * 60 * 1000),
  highlightTimeMs: WINDOW_STATE.data.highlightTimeUtc ? Date.parse(WINDOW_STATE.data.highlightTimeUtc) : null,
  highlightNorads: new Set(WINDOW_STATE.data.highlightNorads || []),
};

function normalizeSatellites(satellites) {
  for (const satellite of satellites) {
    satellite.launchMs = Date.parse(`${satellite.launchDate}T00:00:00Z`);
    satellite.decayMs = satellite.decayUtc ? Date.parse(satellite.decayUtc) : null;
  }
}

function refreshFrameCache() {
  state.frameTimesMs = DATA.frames.map(value => Date.parse(value));
  state.frameStartMs = state.frameTimesMs[0] || 0;
  state.frameEndMs = state.frameTimesMs[state.frameTimesMs.length - 1] || state.frameStartMs;
  state.frameStepMs = state.frameTimesMs.length > 1
    ? state.frameTimesMs[1] - state.frameTimesMs[0]
    : Math.max(60000, DATA.meta.stepMinutes * 60 * 1000);
}
normalizeSatellites(DATA.satellites);
refreshFrameCache();

function updateProjectionToggle() {
  projectionToggle.textContent = state.projectionMode === 'map'
    ? 'Switch To Globe'
    : 'Switch To Map';
}

function buildShellChecks() {
  for (const shell of DATA.meta.shells) {
    const row = document.createElement('label');
    row.className = 'check';
    row.innerHTML = `<span class="key"><span class="swatch" style="background:${shell.color}"></span><span>${shell.displayName}</span></span><input type="checkbox" checked>`;
    const input = row.querySelector('input');
    input.addEventListener('change', () => {
      if (input.checked) state.visibleShells.add(shell.shellId);
      else state.visibleShells.delete(shell.shellId);
      state.visibleSatelliteCache = null;
      draw();
    });
    shellChecks.appendChild(row);
  }
}

function frameTimeMs(frame) {
  const maxIndex = state.frameTimesMs.length - 1;
  const leftIndex = Math.max(0, Math.min(maxIndex, Math.floor(frame)));
  const rightIndex = Math.max(0, Math.min(maxIndex, Math.ceil(frame)));
  const fraction = Math.max(0, Math.min(1, frame - leftIndex));
  const leftMs = state.frameTimesMs[leftIndex];
  const rightMs = state.frameTimesMs[rightIndex];
  return leftMs + (rightMs - leftMs) * fraction;
}

function frameForTimeMs(targetMs) {
  if (targetMs <= state.frameStartMs) return 0;
  if (targetMs >= state.frameEndMs) return Math.max(0, state.frameTimesMs.length - 1);
  return (targetMs - state.frameStartMs) / state.frameStepMs;
}

function skipLeadMs() {
  return Math.max(60000, state.frameStepMs * 0.2);
}

function launchHighlightMs() {
  return 45 * 60 * 1000;
}

function decayHighlightMs() {
  return 45 * 60 * 1000;
}

function loadChunk(chunkId) {
  if (CHUNKS[chunkId]) return Promise.resolve(CHUNKS[chunkId]);
  const event = DATA.events.find(item => item.chunkId === chunkId);
  if (!event) return Promise.reject(new Error(`Unknown chunk ${chunkId}`));
  return new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = event.chunkPath;
    script.onload = () => {
      if (CHUNKS[chunkId]) resolve(CHUNKS[chunkId]);
      else reject(new Error(`Chunk ${chunkId} did not register itself`));
    };
    script.onerror = () => reject(new Error(`Failed to load ${event.chunkPath}`));
    document.head.appendChild(script);
  });
}

async function activateChunk(chunkId, targetMs = null) {
  const chunk = await loadChunk(chunkId);
  WINDOW_STATE.data = chunk;
  normalizeSatellites(DATA.satellites);
  refreshFrameCache();
  state.visibleSatelliteCache = null;
  state.segmentCache = new WeakMap();
  state.highlightTimeMs = chunk.highlightTimeUtc ? Date.parse(chunk.highlightTimeUtc) : null;
  state.highlightNorads = new Set(chunk.highlightNorads || []);
  frameSlider.max = String(Math.max(0, DATA.frames.length - 1));
  currentFrame = targetMs == null ? frameForTimeMs(Date.parse(chunk.focusUtc)) : frameForTimeMs(targetMs);
  frameSlider.value = Math.round(currentFrame).toString();
  draw();
}

function preloadChunk(chunkId) {
  if (!chunkId || CHUNKS[chunkId]) return;
  loadChunk(chunkId).catch(() => {});
}

function findNextEvent(events, currentMs) {
  return events.find(event => event.timeMs > currentMs + 1) || null;
}

function eventLabel(event) {
  return `${new Date(event.timeMs).toISOString()} / ${event.label}`;
}

function updateEventControls(currentMs) {
  const nextLaunch = findNextEvent(state.events.launches, currentMs);
  const nextDecay = findNextEvent(state.events.decays, currentMs);
  nextLaunchBtn.disabled = !nextLaunch;
  nextDecayBtn.disabled = !nextDecay;
  if (!nextLaunch && !nextDecay) {
    eventSummaryEl.textContent = 'No future launch or decay event remains in the local manifest.';
    return;
  }
  const parts = [];
  if (nextLaunch) parts.push(`Launch: ${eventLabel(nextLaunch)}`);
  if (nextDecay) parts.push(`Decay: ${eventLabel(nextDecay)}`);
  eventSummaryEl.textContent = parts.join(' | ');
  preloadChunk(nextLaunch && nextLaunch.chunkId);
  preloadChunk(nextDecay && nextDecay.chunkId);
}

async function jumpToNextEvent(type) {
  const currentMs = frameTimeMs(currentFrame);
  const events = type === 'launch' ? state.events.launches : state.events.decays;
  const event = findNextEvent(events, currentMs);
  if (!event) return;
  const leadMs = type === 'launch'
    ? skipLeadMs()
    : Math.min(skipLeadMs(), decayHighlightMs() * 0.8);
  await activateChunk(event.chunkId, event.timeMs - leadMs);
  playing = true;
  playPause.textContent = 'Pause';
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
    leftIndex,
    rightIndex,
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

function globeLayout() {
  return {
    cx: width * 0.5,
    cy: height * 0.5,
    radius: Math.min(width, height) * 0.34,
  };
}

function globeVector(lat, lon, alt) {
  const latRad = lat * Math.PI / 180;
  const lonRad = lon * Math.PI / 180;
  const radius = 1 + alt / 6378.137;
  const cosLat = Math.cos(latRad);
  return {
    x: radius * cosLat * Math.cos(lonRad),
    y: radius * Math.sin(latRad),
    z: radius * cosLat * Math.sin(lonRad),
  };
}

function rotateGlobe(vec) {
  const cy = Math.cos(state.globeYaw);
  const sy = Math.sin(state.globeYaw);
  const cp = Math.cos(state.globePitch);
  const sp = Math.sin(state.globePitch);
  const x1 = cy * vec.x + sy * vec.z;
  const z1 = -sy * vec.x + cy * vec.z;
  const y2 = cp * vec.y - sp * z1;
  const z2 = sp * vec.y + cp * z1;
  return { x: x1, y: y2, z: z2 };
}

function projectGlobe(lat, lon, alt = 0) {
  const layout = globeLayout();
  const rotated = rotateGlobe(globeVector(lat, lon, alt));
  return {
    x: layout.cx + rotated.x * layout.radius,
    y: layout.cy - rotated.y * layout.radius,
    z: rotated.z,
    layout,
  };
}

function drawGlobeBackdrop() {
  const layout = globeLayout();
  const gradient = ctx.createLinearGradient(0, 0, 0, height);
  gradient.addColorStop(0, '#14283c');
  gradient.addColorStop(0.6, '#09131d');
  gradient.addColorStop(1, '#050a12');
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, width, height);

  const globeGradient = ctx.createRadialGradient(
    layout.cx - layout.radius * 0.22,
    layout.cy - layout.radius * 0.34,
    layout.radius * 0.10,
    layout.cx,
    layout.cy,
    layout.radius
  );
  globeGradient.addColorStop(0, 'rgba(74,138,195,0.95)');
  globeGradient.addColorStop(0.58, 'rgba(18,62,98,0.98)');
  globeGradient.addColorStop(1, 'rgba(5,16,30,1)');
  ctx.fillStyle = globeGradient;
  ctx.beginPath();
  ctx.arc(layout.cx, layout.cy, layout.radius, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = 'rgba(160,196,226,0.18)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.arc(layout.cx, layout.cy, layout.radius, 0, Math.PI * 2);
  ctx.stroke();
}

function strokeGlobePolyline(samples, color, lineWidth = 1) {
  ctx.strokeStyle = color;
  ctx.lineWidth = lineWidth;
  let started = false;
  ctx.beginPath();
  for (const sample of samples) {
    const projected = projectGlobe(sample.lat, sample.lon, sample.alt || 0);
    if (projected.z <= 0) {
      if (started) {
        ctx.stroke();
        ctx.beginPath();
        started = false;
      }
      continue;
    }
    if (!started) {
      ctx.moveTo(projected.x, projected.y);
      started = true;
    } else {
      ctx.lineTo(projected.x, projected.y);
    }
  }
  if (started) ctx.stroke();
}

function drawGlobeGraticule() {
  const samples = [];
  for (let lat = -60; lat <= 60; lat += 30) {
    const line = [];
    for (let lon = -180; lon <= 180; lon += 5) {
      line.push({ lat, lon, alt: 0 });
    }
    samples.push(line);
  }
  for (let lon = -150; lon <= 180; lon += 30) {
    const line = [];
    for (let lat = -90; lat <= 90; lat += 4) {
      line.push({ lat, lon, alt: 0 });
    }
    samples.push(line);
  }
  for (const line of samples) {
    strokeGlobePolyline(line, 'rgba(134,173,206,0.20)');
  }
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
  const source = mode === 'diff'
    ? frameData.group1[cellIndex] - frameData.group4[cellIndex]
    : frameData[mode][cellIndex];
  if (!nextData) return source;
  const target = mode === 'diff'
    ? nextData.group1[cellIndex] - nextData.group4[cellIndex]
    : nextData[mode][cellIndex];
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
  if (state.projectionMode === 'globe') {
    legendTitleEl.textContent = 'Heatmap Legend';
    legendBarEl.style.background = 'linear-gradient(90deg,#0f1724,#2a4058)';
    legendMinEl.textContent = 'map only';
    legendMidEl.textContent = '';
    legendMaxEl.textContent = '';
    legendNoteEl.textContent = 'Heatmap colors are shown only in map projection. Globe mode focuses on the shell point distribution.';
    return;
  }

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

function satelliteLifecycle(satellite, currentMs) {
  if (Number.isFinite(satellite.launchMs) && currentMs < satellite.launchMs) {
    return { visible: false, highlight: false, phase: 'prelaunch' };
  }
  if (Number.isFinite(satellite.decayMs) && currentMs >= satellite.decayMs) {
    return { visible: false, highlight: false, phase: 'postdecay' };
  }
  const launchHighlight = WINDOW_STATE.data.eventType === 'launch'
    && state.highlightNorads.has(satellite.norad)
    && Number.isFinite(state.highlightTimeMs)
    && currentMs >= state.highlightTimeMs
    && currentMs < state.highlightTimeMs + launchHighlightMs();
  const decayHighlight = WINDOW_STATE.data.eventType === 'decay'
    && state.highlightNorads.has(satellite.norad)
    && Number.isFinite(state.highlightTimeMs)
    && currentMs >= state.highlightTimeMs - decayHighlightMs()
    && currentMs < state.highlightTimeMs;
  return {
    visible: true,
    highlight: launchHighlight || decayHighlight,
    phase: decayHighlight ? 'decay' : launchHighlight ? 'launch' : 'steady',
  };
}

function visibleSatellites() {
  if (state.visibleSatelliteCache) return state.visibleSatelliteCache;
  state.visibleSatelliteCache = DATA.satellites.filter(item => state.visibleShells.has(item.shellId));
  return state.visibleSatelliteCache;
}

function buildSegmentSamples(left, right) {
  if (left[0] === right[0] && left[1] === right[1]) return [];
  const distanceDeg = greatCircleDistance(left, right) * 180 / Math.PI;
  const steps = Math.max(8, Math.ceil(distanceDeg / 2));
  const samples = [];
  for (let step = 0; step <= steps; step += 1) {
    const point = greatCirclePoint(left, right, step / steps);
    samples.push({ lat: point.lat, lon: point.lon, alt: 0 });
  }
  return samples;
}

function getSegmentSamples(satellite, left, right, leftIndex, rightIndex) {
  if (leftIndex === rightIndex) return [];
  let cache = state.segmentCache.get(satellite);
  if (!cache) {
    cache = new Map();
    state.segmentCache.set(satellite, cache);
  }
  const key = `${leftIndex}:${rightIndex}`;
  let samples = cache.get(key);
  if (!samples) {
    samples = buildSegmentSamples(left, right);
    cache.set(key, samples);
  }
  return samples;
}

function drawGeodesicSegment(samples, color, lineWidth = 1) {
  if (!samples.length) return;
  ctx.strokeStyle = color;
  ctx.lineWidth = lineWidth;
  let previous = null;
  let open = false;
  for (const sample of samples) {
    const projected = project(sample.lat, sample.lon);
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

function drawGlobeGeodesicSegment(samples, color, lineWidth = 1) {
  if (!samples.length) return;
  strokeGlobePolyline(samples, color, lineWidth);
}

function drawSatellites(frame) {
  const satellites = visibleSatellites();
  const currentMs = frameTimeMs(frame);
  let displayedSatellites = 0;
  let hovered = null;
  for (const satellite of satellites) {
    const lifecycle = satelliteLifecycle(satellite, currentMs);
    if (!lifecycle.visible) continue;
    displayedSatellites += 1;
    const point = interpolatePosition(satellite.positions, frame);
    const current = project(point.lat, point.lon);
    const segmentColor = lifecycle.highlight ? 'rgba(255,244,180,0.95)' : `${satellite.color}55`;
    drawGeodesicSegment(
      getSegmentSamples(satellite, point.left, point.right, point.leftIndex, point.rightIndex),
      segmentColor,
      lifecycle.highlight ? 2.4 : 1
    );
    if (lifecycle.highlight) {
      ctx.strokeStyle = 'rgba(255,244,180,0.95)';
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(current.x, current.y, 6.1, 0, Math.PI * 2);
      ctx.stroke();
    }
    ctx.fillStyle = satellite.color;
    ctx.beginPath();
    ctx.arc(current.x, current.y, lifecycle.highlight ? 3.4 : 2.35, 0, Math.PI * 2);
    ctx.fill();
    const distance = Math.hypot(pointerX - current.x, pointerY - current.y);
    if (distance < 8 && (!hovered || distance < hovered.distance)) {
      hovered = { satellite, point, current, distance, lifecycle };
    }
  }
  satelliteCountEl.textContent = displayedSatellites.toString();
  if (hovered) {
    ctx.strokeStyle = '#ffffff';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.arc(hovered.current.x, hovered.current.y, 5.2, 0, Math.PI * 2);
    ctx.stroke();
    hoverEl.innerHTML = `<strong>${hovered.satellite.satname}</strong><br>${hovered.satellite.displayName} / ${hovered.satellite.groupName}<br>NORAD: ${hovered.satellite.norad}<br>Lat/Lon: ${hovered.point.lat.toFixed(2)} deg, ${hovered.point.lon.toFixed(2)} deg<br>Altitude: ${hovered.point.alt.toFixed(1)} km${hovered.lifecycle.phase === 'launch' ? '<br><strong>Launch highlight</strong>' : hovered.lifecycle.phase === 'decay' ? '<br><strong>Decay highlight</strong>' : ''}`;
  } else {
    hoverEl.textContent = 'Move the pointer near a satellite.';
  }
}

function drawGlobe(frame) {
  drawGlobeBackdrop();
  drawGlobeGraticule();
  updateLegend(heatmapMode.value, 0);
  cellPeakEl.textContent = '--';
  modePill.textContent = 'Projection: globe';
  const satellites = visibleSatellites();
  const currentMs = frameTimeMs(frame);
  let displayedSatellites = 0;
  let hovered = null;
  for (const satellite of satellites) {
    const lifecycle = satelliteLifecycle(satellite, currentMs);
    if (!lifecycle.visible) continue;
    const point = interpolatePosition(satellite.positions, frame);
    drawGlobeGeodesicSegment(
      getSegmentSamples(satellite, point.left, point.right, point.leftIndex, point.rightIndex),
      lifecycle.highlight ? 'rgba(255,244,180,0.95)' : `${satellite.color}55`,
      lifecycle.highlight ? 2.4 : 1
    );
    const current = projectGlobe(point.lat, point.lon, point.alt);
    if (current.z <= 0) {
      continue;
    }
    displayedSatellites += 1;
    const radius = Math.max(1.8, 2.15 + point.alt / 2200) + (lifecycle.highlight ? 1.0 : 0);
    if (lifecycle.highlight) {
      ctx.strokeStyle = 'rgba(255,244,180,0.95)';
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(current.x, current.y, radius + 3.4, 0, Math.PI * 2);
      ctx.stroke();
    }
    ctx.fillStyle = satellite.color;
    ctx.beginPath();
    ctx.arc(current.x, current.y, radius, 0, Math.PI * 2);
    ctx.fill();
    const distance = Math.hypot(pointerX - current.x, pointerY - current.y);
    if (distance < 10 && (!hovered || distance < hovered.distance)) {
      hovered = { satellite, point, current, distance, lifecycle };
    }
  }
  satelliteCountEl.textContent = displayedSatellites.toString();
  if (hovered) {
    ctx.strokeStyle = '#ffffff';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.arc(hovered.current.x, hovered.current.y, 6.0, 0, Math.PI * 2);
    ctx.stroke();
    hoverEl.innerHTML = `<strong>${hovered.satellite.satname}</strong><br>${hovered.satellite.displayName} / ${hovered.satellite.groupName}<br>NORAD: ${hovered.satellite.norad}<br>Lat/Lon: ${hovered.point.lat.toFixed(2)} deg, ${hovered.point.lon.toFixed(2)} deg<br>Altitude: ${hovered.point.alt.toFixed(1)} km${hovered.lifecycle.phase === 'launch' ? '<br><strong>Launch highlight</strong>' : hovered.lifecycle.phase === 'decay' ? '<br><strong>Decay highlight</strong>' : ''}`;
  } else {
    hoverEl.textContent = 'Drag to rotate the globe. Move the pointer near a front-side satellite.';
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
  if (state.projectionMode === 'globe') {
    drawGlobe(currentFrame);
  } else {
    drawBackground();
    drawHeatmap(currentFrame);
    drawLabels();
    drawSatellites(currentFrame);
  }
  const currentTime = new Date(frameTimeMs(currentFrame));
  timePill.textContent = currentTime.toISOString();
  updateEventControls(currentTime.getTime());
}

function tick(now) {
  const deltaSeconds = (now - lastTick) / 1000;
  lastTick = now;
  if (playing && DATA.frames.length > 1) {
    const speed = Number(speedSelect.value);
    currentFrame += deltaSeconds * speed;
    const maxFrame = DATA.frames.length - 1;
    if (currentFrame > maxFrame) {
      currentFrame = maxFrame;
      playing = false;
      playPause.textContent = 'Play';
    }
    frameSlider.value = Math.round(currentFrame).toString();
    draw();
  }
  requestAnimationFrame(tick);
}

playPause.addEventListener('click', () => {
  playing = !playing;
  playPause.textContent = playing ? 'Pause' : 'Play';
});
nextLaunchBtn.addEventListener('click', () => {
  void jumpToNextEvent('launch');
});
nextDecayBtn.addEventListener('click', () => {
  void jumpToNextEvent('decay');
});
projectionToggle.addEventListener('click', () => {
  state.projectionMode = state.projectionMode === 'map' ? 'globe' : 'map';
  updateProjectionToggle();
  draw();
});
frameSlider.addEventListener('input', () => {
  currentFrame = Number(frameSlider.value);
  draw();
});
heatmapMode.addEventListener('change', draw);
canvas.addEventListener('pointerdown', event => {
  if (state.projectionMode !== 'globe') return;
  state.dragging = true;
  state.dragLastX = event.clientX;
  state.dragLastY = event.clientY;
  if (canvas.setPointerCapture) canvas.setPointerCapture(event.pointerId);
});
canvas.addEventListener('pointermove', event => {
  const rect = canvas.getBoundingClientRect();
  pointerX = event.clientX - rect.left;
  pointerY = event.clientY - rect.top;
  if (state.projectionMode === 'globe' && state.dragging) {
    state.globeYaw += (event.clientX - state.dragLastX) * 0.008;
    state.globePitch += (event.clientY - state.dragLastY) * 0.008;
    state.globePitch = Math.max(-1.25, Math.min(1.25, state.globePitch));
    state.dragLastX = event.clientX;
    state.dragLastY = event.clientY;
  }
  draw();
});
canvas.addEventListener('pointerup', () => {
  state.dragging = false;
});
canvas.addEventListener('pointercancel', () => {
  state.dragging = false;
});
canvas.addEventListener('pointerleave', () => {
  state.dragging = false;
  pointerX = -1e9;
  pointerY = -1e9;
  draw();
});
window.addEventListener('resize', resize);

frameSlider.max = String(Math.max(0, DATA.frames.length - 1));
state.events = {
  launches: DATA.events
    .filter(item => item.type === 'launch')
    .map(item => ({ ...item, timeMs: Date.parse(item.timeUtc) }))
    .sort((left, right) => left.timeMs - right.timeMs),
  decays: DATA.events
    .filter(item => item.type === 'decay')
    .map(item => ({ ...item, timeMs: Date.parse(item.timeUtc) }))
    .sort((left, right) => left.timeMs - right.timeMs),
};
currentFrame = frameForTimeMs(Date.parse(DATA.meta.centerUtc));
frameSlider.value = Math.round(currentFrame).toString();
buildShellChecks();
updateProjectionToggle();
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
    let mut not_launched = 0usize;
    for record in records {
        if let Some(launch_utc) = parse_launch_date_utc(&record.launch_date) {
            if launch_utc > center_utc {
                not_launched += 1;
                continue;
            }
        }
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
        "Filtered records to {} active satellites (skipped {} future-launch, {} decayed, {} stale)",
        kept.len(),
        not_launched,
        decayed,
        stale
    );
    kept
}

fn record_is_active_at(record: &LatestTleRecord, frame_utc: DateTime<Utc>) -> bool {
    if let Some(launch_utc) = parse_launch_date_utc(&record.launch_date) {
        if frame_utc < launch_utc {
            return false;
        }
    }
    if let Some(decay_utc) = parse_optional_utc(&record.decay_date_text) {
        if frame_utc >= decay_utc {
            return false;
        }
    }
    true
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
    parse_iso_utc(value).ok().or_else(|| {
        NaiveDateTime::parse_from_str(&format!("{}T00:00:00", value), "%Y-%m-%dT%H:%M:%S")
            .ok()
            .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
    })
}

fn parse_launch_date_utc(value: &str) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let date = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").ok()?;
    let naive = date.and_hms_opt(0, 0, 0)?;
    Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
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
    headers
        .iter()
        .position(|header| *header == name)
        .ok_or_else(|| {
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
    value
        .map(|path| sql_string(&path.display().to_string()))
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
