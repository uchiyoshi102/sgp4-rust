use crate::sgp4::sgp4::{gstime, jday};
use crate::sgp4::tle::{read_tles, TLE};
use crate::spacex::catalog::{load_catalog_rows, CatalogRow};
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

const EARTH_RADIUS_KM: f64 = 6378.137;
const WGS84_A_KM: f64 = 6378.137;
const WGS84_F: f64 = 1.0 / 298.257223563;
const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / (180.0 * 3600.0);
const MJD_OFFSET: f64 = 2_400_000.5;
const DEFAULT_WINDOW_HOURS: i64 = 1;
const DEFAULT_STEP_MINUTES: i64 = 10;
const DEFAULT_CELL_DEGREES: i32 = 10;
const SHELL_PALETTE: [&str; 12] = [
    "#6fd3ff", "#ffb45e", "#8be28b", "#f37f8d", "#a99dff", "#ffd866", "#4fd1c5", "#f6ad55",
    "#90cdf4", "#f687b3", "#c6f6d5", "#fbd38d",
];

#[derive(Debug)]
pub struct Config {
    catalog: Option<PathBuf>,
    tle: PathBuf,
    eop: PathBuf,
    output_dir: PathBuf,
    center_utc: Option<DateTime<Utc>>,
    step_minutes: i64,
    window_hours: i64,
    cell_degrees: i32,
}

#[derive(Clone, Debug)]
struct Cell {
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

#[derive(Clone, Copy, Debug)]
struct FrameContext {
    frame_utc: DateTime<Utc>,
    jdut1: f64,
    lod_seconds: f64,
    xp_rad: f64,
    yp_rad: f64,
}

#[derive(Debug)]
struct JoinedSatellite {
    catalog: CatalogRow,
    tle: TLE,
    shell_key: String,
    shell_name: String,
    shell_altitude_km: f64,
    shell_inclination_deg: f64,
}

#[derive(Clone, Debug)]
struct ShellAccumulator {
    norads: BTreeSet<String>,
    launch_ids: BTreeSet<String>,
    operators: BTreeSet<String>,
    sample_payloads: BTreeSet<String>,
    altitude_sum: f64,
    inclination_sum: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DataRoot {
    meta: DataMeta,
    cells: Vec<[f64; 4]>,
    shells: Vec<ShellView>,
    frames: Vec<String>,
    satellites: Vec<SatelliteView>,
    coverage: Vec<CoverageFrame>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DataMeta {
    title: String,
    center_utc: String,
    step_minutes: i64,
    window_hours: i64,
    cell_degrees: i32,
    total_satellites: usize,
    visible_shells: usize,
    tle_path: String,
    catalog_path: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ShellView {
    shell_id: String,
    shell_name: String,
    color: String,
    order: usize,
    mean_altitude_km: f64,
    mean_inclination_deg: f64,
    satellite_count: usize,
    launch_count: usize,
    operators: Vec<String>,
    payload_examples: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SatelliteView {
    norad_cat_id: String,
    payload_name: String,
    launch_name: String,
    launch_date: String,
    shell_id: String,
    orbit_hint: String,
    regime_hint: String,
    epoch_utc: String,
    samples: Vec<[f64; 3]>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoverageFrame {
    shell_counts: Vec<Vec<u16>>,
}

pub fn run_from_args() -> io::Result<()> {
    run(parse_args()?)
}

fn run(config: Config) -> io::Result<()> {
    if config.step_minutes <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--step-minutes must be greater than 0",
        ));
    }
    if config.window_hours <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--window-hours must be greater than 0",
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

    let joined = join_inputs(config.catalog.as_deref(), &config.tle)?;
    if joined.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "no usable STARLINK TLEs were found in {}",
                config.tle.display()
            ),
        ));
    }

    let center_utc = config
        .center_utc
        .unwrap_or_else(|| round_down_to_step(latest_epoch(&joined), config.step_minutes));
    let frame_times = build_frame_times(center_utc, config.window_hours, config.step_minutes)?;
    let frame_contexts = build_frame_contexts(&frame_times, &load_eop_records(&config.eop)?)?;
    let cell_grid = build_cells(config.cell_degrees);
    let shells = build_shell_views(&joined);
    let shell_index = shells
        .iter()
        .enumerate()
        .map(|(index, shell)| (shell.shell_id.clone(), index))
        .collect::<HashMap<_, _>>();
    let (satellites, coverage) =
        build_tracks_and_coverage(&joined, &frame_contexts, &cell_grid, &shell_index)?;

    let data = DataRoot {
        meta: DataMeta {
            title: "Starlink Shell Map".to_string(),
            center_utc: center_utc.to_rfc3339(),
            step_minutes: config.step_minutes,
            window_hours: config.window_hours,
            cell_degrees: config.cell_degrees,
            total_satellites: satellites.len(),
            visible_shells: shells.len(),
            tle_path: config.tle.display().to_string(),
            catalog_path: config
                .catalog
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "TLE line 0 names only".to_string()),
        },
        cells: cell_grid
            .cells
            .iter()
            .map(|cell| {
                [
                    cell.lat_min_deg,
                    cell.lat_max_deg,
                    cell.lon_min_deg,
                    cell.lon_max_deg,
                ]
            })
            .collect(),
        shells: shells.clone(),
        frames: frame_times.iter().map(DateTime::to_rfc3339).collect(),
        satellites,
        coverage,
    };

    let summary_path = config.output_dir.join("shell_summary.csv");
    let data_js_path = config.output_dir.join("data.js");
    let html_path = config.output_dir.join("index.html");
    write_summary_csv(&summary_path, &shells)?;
    write_data_js(&data_js_path, &data)?;
    ensure_index_html(&html_path)?;

    eprintln!("Wrote {}", summary_path.display());
    eprintln!("Wrote {}", data_js_path.display());
    eprintln!("Wrote {}", html_path.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut catalog = None;
    let mut tle = root.join("data/starlink_space_track_current.tle");
    let mut eop = root.join("eop/eopc04_20u24.1962-now.csv");
    let mut output_dir = root.join("data/starlink_shell_map");
    let mut center_utc = None;
    let mut step_minutes = DEFAULT_STEP_MINUTES;
    let mut window_hours = DEFAULT_WINDOW_HOURS;
    let mut cell_degrees = DEFAULT_CELL_DEGREES;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--catalog" => catalog = Some(PathBuf::from(next_arg(&mut args, "--catalog")?)),
            "--tle" => tle = PathBuf::from(next_arg(&mut args, "--tle")?),
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
                            format!("invalid --step-minutes: {error}"),
                        )
                    })?;
            }
            "--window-hours" => {
                window_hours = next_arg(&mut args, "--window-hours")?
                    .parse::<i64>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --window-hours: {error}"),
                        )
                    })?;
            }
            "--cell-degrees" => {
                cell_degrees = next_arg(&mut args, "--cell-degrees")?
                    .parse::<i32>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --cell-degrees: {error}"),
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
        tle,
        eop,
        output_dir,
        center_utc,
        step_minutes,
        window_hours,
        cell_degrees,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_shell_map -- [options]");
    println!("Options:");
    println!("  --catalog PATH       optional metadata CSV; defaults to TLE-only mode");
    println!("  --tle PATH           default: data/starlink_space_track_current.tle");
    println!("  --eop PATH");
    println!("  --output-dir PATH");
    println!("  --center-utc RFC3339");
    println!("  --step-minutes N");
    println!("  --window-hours N");
    println!("  --cell-degrees N");
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing value for {flag}"),
        )
    })
}

fn parse_rfc3339_utc(value: &str) -> io::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to parse RFC3339 timestamp '{value}': {error}"),
            )
        })
}

fn join_inputs(catalog_path: Option<&Path>, tle_path: &Path) -> io::Result<Vec<JoinedSatellite>> {
    match catalog_path {
        Some(path) => join_catalog_with_tles(&load_catalog_rows(path)?, tle_path),
        None => join_tles_only(tle_path),
    }
}

fn join_catalog_with_tles(
    catalog_rows: &[CatalogRow],
    tle_path: &Path,
) -> io::Result<Vec<JoinedSatellite>> {
    let catalog_by_norad = catalog_rows
        .iter()
        .map(|row| (row.norad_cat_id.clone(), row.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut joined = Vec::new();
    for mut tle in read_tles(tle_path)? {
        let Some(catalog) = catalog_by_norad.get(&tle.object_id) else {
            continue;
        };
        let (shell_key, shell_name, shell_altitude_km, shell_inclination_deg) =
            classify_shell(catalog, &tle);
        tle.name = if tle.name.is_empty() {
            catalog.payload_name.clone()
        } else {
            tle.name.clone()
        };
        joined.push(JoinedSatellite {
            catalog: catalog.clone(),
            tle,
            shell_key,
            shell_name,
            shell_altitude_km,
            shell_inclination_deg,
        });
    }
    joined.sort_by(|left, right| {
        left.shell_altitude_km
            .partial_cmp(&right.shell_altitude_km)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.shell_inclination_deg
                    .partial_cmp(&right.shell_inclination_deg)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.catalog.launch_date.cmp(&right.catalog.launch_date))
            .then_with(|| left.catalog.payload_name.cmp(&right.catalog.payload_name))
    });
    Ok(joined)
}

fn join_tles_only(tle_path: &Path) -> io::Result<Vec<JoinedSatellite>> {
    let mut joined = Vec::new();
    for mut tle in read_tles(tle_path)? {
        let payload_name = if tle.name.trim().is_empty() {
            format!("STARLINK {}", tle.object_id)
        } else {
            tle.name.trim().to_string()
        };
        if !payload_name.to_ascii_uppercase().contains("STARLINK") {
            continue;
        }
        tle.name = payload_name.clone();
        let catalog = CatalogRow {
            launch_id: String::new(),
            launch_name: String::new(),
            launch_date: String::new(),
            payload_id: tle.object_id.clone(),
            payload_name,
            norad_cat_id: tle.object_id.clone(),
            customers: "SpaceX".to_string(),
            manufacturer: "SpaceX".to_string(),
            nationality: "US".to_string(),
            orbit_hint: String::new(),
            regime_hint: "leo".to_string(),
        };
        let (shell_key, shell_name, shell_altitude_km, shell_inclination_deg) =
            classify_shell(&catalog, &tle);
        joined.push(JoinedSatellite {
            catalog,
            tle,
            shell_key,
            shell_name,
            shell_altitude_km,
            shell_inclination_deg,
        });
    }
    joined.sort_by(|left, right| {
        left.shell_altitude_km
            .partial_cmp(&right.shell_altitude_km)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.shell_inclination_deg
                    .partial_cmp(&right.shell_inclination_deg)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.catalog.payload_name.cmp(&right.catalog.payload_name))
    });
    Ok(joined)
}

fn classify_shell(catalog: &CatalogRow, tle: &TLE) -> (String, String, f64, f64) {
    let altitude_km = mean_altitude_km_from_tle(tle);
    let inclination_bin = (tle.inc_deg * 2.0).round() / 2.0;
    if catalog
        .payload_name
        .to_ascii_uppercase()
        .starts_with("STARLINK")
        && altitude_km < 450.0
    {
        let shell_key = format!("inc-{inclination_bin:.1}-transfer");
        let shell_name = format!("{inclination_bin:.1}° / transfer band");
        return (shell_key, shell_name, 400.0, inclination_bin);
    }
    let altitude_bin = (altitude_km / 50.0).round() * 50.0;
    let shell_key = format!("inc-{inclination_bin:.1}-alt-{altitude_bin:.0}");
    let shell_name = format!("{inclination_bin:.1}° / {altitude_bin:.0} km");
    (shell_key, shell_name, altitude_bin, inclination_bin)
}

fn mean_altitude_km_from_tle(tle: &TLE) -> f64 {
    let mean_motion_rad_s = tle.n * 2.0 * std::f64::consts::PI / 86400.0;
    let semi_major_axis_km = (398600.4418 / (mean_motion_rad_s * mean_motion_rad_s)).cbrt();
    semi_major_axis_km - EARTH_RADIUS_KM
}

fn latest_epoch(joined: &[JoinedSatellite]) -> DateTime<Utc> {
    joined
        .iter()
        .map(|item| item.tle.epoch)
        .max()
        .unwrap_or_else(Utc::now)
}

fn round_down_to_step(timestamp: DateTime<Utc>, step_minutes: i64) -> DateTime<Utc> {
    let step_seconds = step_minutes * 60;
    let timestamp_seconds = timestamp.timestamp();
    let floored = timestamp_seconds - timestamp_seconds.rem_euclid(step_seconds);
    DateTime::<Utc>::from_timestamp(floored, 0).unwrap_or(timestamp)
}

fn build_frame_times(
    center_utc: DateTime<Utc>,
    window_hours: i64,
    step_minutes: i64,
) -> io::Result<Vec<DateTime<Utc>>> {
    let start = center_utc - Duration::hours(window_hours);
    let end = center_utc + Duration::hours(window_hours);
    let mut frames = Vec::new();
    let mut current = start;
    while current <= end {
        frames.push(current);
        current += Duration::minutes(step_minutes);
    }
    if frames.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no frames were generated",
        ));
    }
    Ok(frames)
}

fn build_shell_views(joined: &[JoinedSatellite]) -> Vec<ShellView> {
    let mut accumulators = BTreeMap::<String, ShellAccumulator>::new();
    let mut shell_meta = BTreeMap::<String, (String, f64, f64)>::new();
    for satellite in joined {
        shell_meta.insert(
            satellite.shell_key.clone(),
            (
                satellite.shell_name.clone(),
                satellite.shell_altitude_km,
                satellite.shell_inclination_deg,
            ),
        );
        let entry = accumulators
            .entry(satellite.shell_key.clone())
            .or_insert(ShellAccumulator {
                norads: BTreeSet::new(),
                launch_ids: BTreeSet::new(),
                operators: BTreeSet::new(),
                sample_payloads: BTreeSet::new(),
                altitude_sum: 0.0,
                inclination_sum: 0.0,
            });
        entry.norads.insert(satellite.catalog.norad_cat_id.clone());
        if !satellite.catalog.launch_id.is_empty() {
            entry.launch_ids.insert(satellite.catalog.launch_id.clone());
        }
        if !satellite.catalog.customers.is_empty() {
            entry.operators.insert(satellite.catalog.customers.clone());
        }
        if entry.sample_payloads.len() < 4 {
            entry
                .sample_payloads
                .insert(satellite.catalog.payload_name.clone());
        }
        entry.altitude_sum += satellite.shell_altitude_km;
        entry.inclination_sum += satellite.shell_inclination_deg;
    }

    let mut shells = accumulators
        .into_iter()
        .map(|(shell_id, accumulator)| {
            let meta = shell_meta.get(&shell_id).unwrap();
            let satellite_count = accumulator.norads.len();
            ShellView {
                shell_id,
                shell_name: meta.0.clone(),
                color: String::new(),
                order: 0,
                mean_altitude_km: accumulator.altitude_sum / satellite_count as f64,
                mean_inclination_deg: accumulator.inclination_sum / satellite_count as f64,
                satellite_count,
                launch_count: accumulator.launch_ids.len(),
                operators: accumulator.operators.into_iter().collect(),
                payload_examples: accumulator.sample_payloads.into_iter().collect(),
            }
        })
        .collect::<Vec<_>>();

    shells.sort_by(|left, right| {
        left.mean_altitude_km
            .partial_cmp(&right.mean_altitude_km)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.mean_inclination_deg
                    .partial_cmp(&right.mean_inclination_deg)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.shell_name.cmp(&right.shell_name))
    });
    for (index, shell) in shells.iter_mut().enumerate() {
        shell.order = index;
        shell.color = SHELL_PALETTE[index % SHELL_PALETTE.len()].to_string();
    }
    shells
}

fn build_tracks_and_coverage(
    joined: &[JoinedSatellite],
    frame_contexts: &[FrameContext],
    cell_grid: &CellGrid,
    shell_index: &HashMap<String, usize>,
) -> io::Result<(Vec<SatelliteView>, Vec<CoverageFrame>)> {
    let mut coverage = frame_contexts
        .iter()
        .map(|_| CoverageFrame {
            shell_counts: vec![vec![0u16; cell_grid.cells.len()]; shell_index.len()],
        })
        .collect::<Vec<_>>();
    let mut satellites = Vec::with_capacity(joined.len());

    for (satellite_index, satellite) in joined.iter().enumerate() {
        if satellite_index % 100 == 0 {
            eprintln!(
                "Propagating satellite {} / {}",
                satellite_index + 1,
                joined.len()
            );
        }
        let shell_position = *shell_index.get(&satellite.shell_key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "shell '{}' was missing from shell index",
                    satellite.shell_key
                ),
            )
        })?;
        let mut tle = TLE::new();
        tle.name = satellite.tle.name.clone();
        tle.parse_lines(&satellite.tle.line1, &satellite.tle.line2);
        let mut samples = Vec::with_capacity(frame_contexts.len());
        for (frame_index, frame_context) in frame_contexts.iter().enumerate() {
            let mins_after_epoch =
                duration_to_minutes(frame_context.frame_utc.signed_duration_since(tle.epoch))?;
            let (r_teme, v_teme) = tle.get_rv(mins_after_epoch);
            if tle.sgp4_error != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "SGP4 failed for NORAD {} with error {}",
                        satellite.catalog.norad_cat_id, tle.sgp4_error
                    ),
                ));
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
            samples.push([lat_deg, lon_deg, altitude_km]);
            for cell_index in candidate_cell_indices(cell_grid, lat_deg, lon_deg, altitude_km) {
                let cell = &cell_grid.cells[cell_index];
                let visible =
                    r_ecef[0] * cell.unit_x + r_ecef[1] * cell.unit_y + r_ecef[2] * cell.unit_z
                        > EARTH_RADIUS_KM;
                if visible {
                    coverage[frame_index].shell_counts[shell_position][cell_index] += 1;
                }
            }
        }
        satellites.push(SatelliteView {
            norad_cat_id: satellite.catalog.norad_cat_id.clone(),
            payload_name: satellite.catalog.payload_name.clone(),
            launch_name: satellite.catalog.launch_name.clone(),
            launch_date: satellite.catalog.launch_date.clone(),
            shell_id: satellite.shell_key.clone(),
            orbit_hint: satellite.catalog.orbit_hint.clone(),
            regime_hint: satellite.catalog.regime_hint.clone(),
            epoch_utc: satellite.tle.epoch.to_rfc3339(),
            samples,
        });
    }

    Ok((satellites, coverage))
}

fn write_summary_csv(path: &Path, shells: &[ShellView]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "shell_id,shell_name,color,order,mean_altitude_km,mean_inclination_deg,satellite_count,launch_count,operators,payload_examples"
    )?;
    for shell in shells {
        writeln!(
            writer,
            "{},{},{},{},{:.3},{:.3},{},{},{},{}",
            csv_escape(&shell.shell_id),
            csv_escape(&shell.shell_name),
            csv_escape(&shell.color),
            shell.order,
            shell.mean_altitude_km,
            shell.mean_inclination_deg,
            shell.satellite_count,
            shell.launch_count,
            csv_escape(&shell.operators.join("|")),
            csv_escape(&shell.payload_examples.join("|")),
        )?;
    }
    writer.flush()
}

fn write_data_js(path: &Path, data: &DataRoot) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "window.SPACEX_LEO_SHELL_DATA = {};",
        serde_json::to_string(data).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize shell map data: {error}"),
            )
        })?
    )?;
    writer.flush()
}

fn ensure_index_html(path: &Path) -> io::Result<()> {
    let template_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("starlink_shell_map")
        .join("index.html");
    if path == template_path {
        return Ok(());
    }
    if !template_path.exists() {
        return Ok(());
    }
    fs::copy(&template_path, path)?;
    Ok(())
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

fn load_eop_records(path: &Path) -> io::Result<Vec<EopRecord>> {
    let mut lines = read_lines(path)?;
    let header = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty EOP CSV"))??;
    let headers: Vec<&str> = header.split(';').collect();
    let mjd_index = column_index(&headers, "MJD")?;
    let xp_index = column_index(&headers, "x_pole")?;
    let yp_index = column_index(&headers, "y_pole")?;
    let ut1_index = column_index(&headers, "UT1-UTC")?;
    let lod_index = column_index(&headers, "LOD")?;
    let mut records = Vec::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(';').collect();
        records.push(EopRecord {
            mjd_utc: parse_field(&fields, mjd_index, "MJD")?,
            xp_arcsec: parse_field(&fields, xp_index, "x_pole")?,
            yp_arcsec: parse_field(&fields, yp_index, "y_pole")?,
            ut1_utc_seconds: parse_field(&fields, ut1_index, "UT1-UTC")?,
            lod_seconds: parse_field(&fields, lod_index, "LOD")?,
        });
    }
    Ok(records)
}

fn interpolate_eop(records: &[EopRecord], mjd_utc: f64) -> io::Result<EopSample> {
    if mjd_utc <= records[0].mjd_utc {
        let first = records[0];
        return Ok(EopSample {
            xp_rad: first.xp_arcsec * ARCSEC_TO_RAD,
            yp_rad: first.yp_arcsec * ARCSEC_TO_RAD,
            ut1_utc_seconds: first.ut1_utc_seconds,
            lod_seconds: first.lod_seconds,
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
    for pair in records.windows(2) {
        let start = pair[0];
        let end = pair[1];
        if mjd_utc >= start.mjd_utc && mjd_utc <= end.mjd_utc {
            let span = end.mjd_utc - start.mjd_utc;
            let fraction = if span.abs() < 1e-12 {
                0.0
            } else {
                (mjd_utc - start.mjd_utc) / span
            };
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
        format!("failed to interpolate EOP for MJD {mjd_utc}"),
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
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing column {name}")))
}

fn parse_field(fields: &[&str], index: usize, name: &str) -> io::Result<f64> {
    fields
        .get(index)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing field {name}")))?
        .trim()
        .parse::<f64>()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse {name}: {error}"),
            )
        })
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(BufReader::new(file).lines())
}

fn lerp(start: f64, end: f64, fraction: f64) -> f64 {
    start + (end - start) * fraction
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
