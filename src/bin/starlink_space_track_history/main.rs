use sgp4::starlink::csv::{
    build_header_map, csv_escape, get_field, parse_csv_line, require_column,
};
use sgp4::starlink::spacetrack::{
    build_gp_history_csv_url_candidates, build_gp_history_zip_url, build_query_window, can_try_zip,
    describe_query_window, download_binary, extract_zip, fetch_csv_text, inspect_csv_response,
    list_csv_files, load_credentials, login_to_space_track, merge_csv_files, split_ids,
    try_zip_download, window_file_stem, write_url_manifest, CookieJar, CsvResponseCheck,
    QueryWindow,
};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

const DEFAULT_NAME_PREFIX: &str = "STARLINK";

#[derive(Debug)]
struct Config {
    output_dir: PathBuf,
    name_prefix: String,
    start_date: Option<String>,
    end_date: Option<String>,
    chunk_size: usize,
    identity: Option<String>,
    catalog_only: bool,
    resume: bool,
    dry_run: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SatcatRow {
    norad_cat_id: String,
    satname: String,
    launch: String,
    decay: String,
    object_type: String,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let satcat_url = build_satcat_name_query_url(&config.name_prefix);
    let query_window = build_query_window(config.start_date.clone(), config.end_date.clone())?;

    if config.dry_run {
        println!("satcat_url={satcat_url}");
        println!("history_window={}", describe_query_window(&query_window));
        println!("output_dir={}", config.output_dir.display());
        return Ok(());
    }

    let credentials = load_credentials(config.identity)?;
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(&credentials, cookie_jar.path())?;
    fs::create_dir_all(&config.output_dir)?;

    let catalog_path = config.output_dir.join("starlink_satcat.csv");
    let satcat_body = fetch_csv_text(cookie_jar.path(), &satcat_url)?;
    let rows = parse_satcat_rows(&satcat_body, &config.name_prefix)?;
    if rows.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "SATCAT query returned no rows whose SATNAME starts with {}",
                config.name_prefix
            ),
        ));
    }
    write_satcat_rows(&catalog_path, &rows)?;
    eprintln!(
        "Wrote {} SATCAT rows to {}",
        rows.len(),
        catalog_path.display()
    );

    if config.catalog_only {
        return Ok(());
    }

    let ids = unique_norad_ids(&rows);
    download_history(
        cookie_jar.path(),
        &config.output_dir,
        &ids,
        config.chunk_size,
        &query_window,
        config.resume,
    )?;
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut output_dir = root.join("starlink-space-track-history");
    let mut name_prefix = DEFAULT_NAME_PREFIX.to_string();
    let mut start_date = None;
    let mut end_date = None;
    let mut chunk_size = 20usize;
    let mut identity = None;
    let mut catalog_only = false;
    let mut resume = false;
    let mut dry_run = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-dir" => output_dir = PathBuf::from(next_arg(&mut args, "--output-dir")?),
            "--name-prefix" => name_prefix = next_arg(&mut args, "--name-prefix")?.to_uppercase(),
            "--start-date" => start_date = Some(next_arg(&mut args, "--start-date")?),
            "--end-date" => end_date = Some(next_arg(&mut args, "--end-date")?),
            "--chunk-size" => {
                chunk_size = next_arg(&mut args, "--chunk-size")?
                    .parse::<usize>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --chunk-size: {error}"),
                        )
                    })?;
                if chunk_size == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "--chunk-size must be greater than 0",
                    ));
                }
            }
            "--identity" => identity = Some(next_arg(&mut args, "--identity")?),
            "--catalog-only" => catalog_only = true,
            "--resume" => resume = true,
            "--dry-run" => dry_run = true,
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
        output_dir,
        name_prefix,
        start_date,
        end_date,
        chunk_size,
        identity,
        catalog_only,
        resume,
        dry_run,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_space_track_history -- [options]");
    println!("Options:");
    println!("  --output-dir PATH       default: starlink-space-track-history");
    println!("  --name-prefix TEXT      default: STARLINK");
    println!("  --start-date YYYY-MM-DD optional GP_HISTORY creation-date lower bound");
    println!("  --end-date YYYY-MM-DD   optional GP_HISTORY creation-date upper bound");
    println!("  --chunk-size N          default: 20");
    println!("  --identity USER         also read from SPACE_TRACK_IDENTITY");
    println!("  --catalog-only          write SATCAT matches without GP_HISTORY download");
    println!("  --resume                skip valid existing batches and continue missing ones");
    println!("  --dry-run               print query plan without logging in");
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing value for {flag}"),
        )
    })
}

fn build_satcat_name_query_url(name_prefix: &str) -> String {
    format!(
        "https://www.space-track.org/basicspacedata/query/class/satcat/SATNAME/{}~~/orderby/NORAD_CAT_ID/predicates/NORAD_CAT_ID,SATNAME,LAUNCH,DECAY,OBJECT_TYPE/format/csv/emptyresult/show",
        encode_path_value(name_prefix)
    )
}

fn encode_path_value(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            b' ' => "%20".to_string(),
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn parse_satcat_rows(csv_body: &str, name_prefix: &str) -> io::Result<Vec<SatcatRow>> {
    let mut lines = csv_body.lines();
    let header = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Space-Track CSV was empty"))?;
    let header_map = build_header_map(&parse_csv_line(header));
    let norad_index = require_column(&header_map, "NORAD_CAT_ID")?;
    let satname_index = require_column(&header_map, "SATNAME")?;
    let launch_index = require_column(&header_map, "LAUNCH")?;
    let decay_index = require_column(&header_map, "DECAY")?;
    let object_type_index = require_column(&header_map, "OBJECT_TYPE")?;

    let mut rows = BTreeSet::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(line);
        let satname = get_field(&fields, satname_index, "SATNAME")?;
        if !satname.to_ascii_uppercase().starts_with(name_prefix) {
            continue;
        }
        rows.insert(SatcatRow {
            norad_cat_id: get_field(&fields, norad_index, "NORAD_CAT_ID")?,
            satname,
            launch: get_field(&fields, launch_index, "LAUNCH")?,
            decay: get_field(&fields, decay_index, "DECAY")?,
            object_type: get_field(&fields, object_type_index, "OBJECT_TYPE")?,
        });
    }
    Ok(rows.into_iter().collect())
}

fn write_satcat_rows(path: &Path, rows: &[SatcatRow]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "norad_cat_id,satname,launch,decay,object_type")?;
    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{}",
            csv_escape(&row.norad_cat_id),
            csv_escape(&row.satname),
            csv_escape(&row.launch),
            csv_escape(&row.decay),
            csv_escape(&row.object_type),
        )?;
    }
    writer.flush()
}

fn unique_norad_ids(rows: &[SatcatRow]) -> Vec<String> {
    let mut ids = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        if !row.norad_cat_id.is_empty() && seen.insert(row.norad_cat_id.clone()) {
            ids.push(row.norad_cat_id.clone());
        }
    }
    ids
}

fn download_history(
    cookie_path: &Path,
    output_dir: &Path,
    ids: &[String],
    chunk_size: usize,
    query_window: &QueryWindow,
    resume: bool,
) -> io::Result<()> {
    let stem = window_file_stem(query_window);
    let zip_path = output_dir.join(format!("starlink_gp_history_{stem}.zip"));
    let merged_csv_path = output_dir.join(format!("starlink_gp_history_{stem}.csv"));
    let stable_csv_path = output_dir.join("starlink_gp_history.csv");
    let urls_csv_path = output_dir.join(format!("gp_history_urls_{stem}.csv"));

    if resume && stable_csv_path.exists() {
        eprintln!("Using existing {}", stable_csv_path.display());
        return Ok(());
    }

    let zip_url = build_gp_history_zip_url(ids, query_window);
    let chunks = split_ids(ids, chunk_size);
    let batch_urls = chunks
        .iter()
        .map(|chunk| build_gp_history_csv_url_candidates(chunk, query_window))
        .collect::<Vec<_>>();
    write_url_manifest(&zip_url, &batch_urls, &urls_csv_path)?;

    if can_try_zip(&zip_url) && try_zip_download(cookie_path, &zip_url, &zip_path)? {
        let extracted_dir = output_dir.join("zip_contents");
        extract_zip(&zip_path, &extracted_dir)?;
        let csv_paths = list_csv_files(&extracted_dir)?;
        merge_csv_files(&csv_paths, &merged_csv_path)?;
        fs::copy(&merged_csv_path, &stable_csv_path)?;
        return Ok(());
    }

    let batch_dir = output_dir.join("batches");
    fs::create_dir_all(&batch_dir)?;
    let resume_from_batches = resume && batch_dir.exists();
    let mut batch_paths = Vec::new();
    let mut next_batch_index = 1usize;
    for ids_chunk in chunks {
        download_chunk_recursive(
            cookie_path,
            &ids_chunk,
            query_window,
            &batch_dir,
            &mut next_batch_index,
            &mut batch_paths,
            resume_from_batches,
        )?;
    }
    merge_csv_files(&batch_paths, &merged_csv_path)?;
    fs::copy(&merged_csv_path, &stable_csv_path)?;
    Ok(())
}

fn download_chunk_recursive(
    cookie_path: &Path,
    ids: &[String],
    query_window: &QueryWindow,
    batch_dir: &Path,
    next_batch_index: &mut usize,
    batch_paths: &mut Vec<PathBuf>,
    resume: bool,
) -> io::Result<()> {
    let batch_number = *next_batch_index;
    *next_batch_index += 1;
    let batch_path = batch_path(batch_dir, batch_number);
    let empty_marker_path = empty_marker_path(batch_dir, batch_number);

    if resume && empty_marker_path.exists() {
        eprintln!("Skipping empty batch {batch_number:05}");
        return Ok(());
    }
    if resume && batch_path.exists() {
        match inspect_csv_response(&batch_path) {
            Ok(CsvResponseCheck::Valid) => {
                eprintln!("Skipping completed batch {batch_number:05}");
                batch_paths.push(batch_path);
                return Ok(());
            }
            Ok(CsvResponseCheck::Empty) => {
                eprintln!("Skipping previously empty batch {batch_number:05}");
                let _ = fs::remove_file(&batch_path);
                File::create(&empty_marker_path)?;
                return Ok(());
            }
            Err(error) => {
                eprintln!("Re-downloading invalid batch {batch_number:05}: {}", error);
                let _ = fs::remove_file(&batch_path);
            }
        }
    }

    let urls = build_gp_history_csv_url_candidates(ids, query_window);
    let mut last_error = None::<io::Error>;

    for url in &urls {
        download_binary(cookie_path, url, &batch_path)?;
        match inspect_csv_response(&batch_path) {
            Ok(CsvResponseCheck::Valid) => {
                batch_paths.push(batch_path.clone());
                return Ok(());
            }
            Ok(CsvResponseCheck::Empty) => {
                let _ = fs::remove_file(&batch_path);
                File::create(&empty_marker_path)?;
                return Ok(());
            }
            Err(error) => {
                last_error = Some(error);
                let _ = fs::remove_file(&batch_path);
            }
        }
    }

    if ids.len() > 1 {
        let split_at = ids.len() / 2;
        let (left, right) = ids.split_at(split_at);
        download_chunk_recursive(
            cookie_path,
            left,
            query_window,
            batch_dir,
            next_batch_index,
            batch_paths,
            resume,
        )?;
        download_chunk_recursive(
            cookie_path,
            right,
            query_window,
            batch_dir,
            next_batch_index,
            batch_paths,
            resume,
        )?;
        return Ok(());
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "response did not look like GP_HISTORY CSV",
        )
    }))
}

fn batch_path(batch_dir: &Path, batch_number: usize) -> PathBuf {
    batch_dir.join(format!("batch_{batch_number:05}.csv"))
}

fn empty_marker_path(batch_dir: &Path, batch_number: usize) -> PathBuf {
    batch_dir.join(format!("batch_{batch_number:05}.empty"))
}
