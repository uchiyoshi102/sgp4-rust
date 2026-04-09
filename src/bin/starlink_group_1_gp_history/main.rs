use sgp4::starlink::csv::{build_header_map, get_field, parse_csv_line, require_column};
use sgp4::starlink::spacetrack::{
    build_gp_history_csv_url_candidates, build_gp_history_zip_url, build_query_window, can_try_zip,
    describe_query_window, download_binary, extract_zip, inspect_csv_response, list_csv_files,
    load_credentials, login_to_space_track, merge_csv_files, split_ids, try_zip_download,
    window_file_stem, write_url_manifest, CookieJar, CsvResponseCheck, QueryWindow,
};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct Config {
    input: PathBuf,
    output_dir: PathBuf,
    start_date: Option<String>,
    end_date: Option<String>,
    chunk_size: usize,
    identity: Option<String>,
    dry_run: bool,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let norad_ids = load_norad_ids(&config.input)?;
    let query_window = build_query_window(config.start_date.clone(), config.end_date.clone())?;
    let stem = window_file_stem(&query_window);
    let zip_path = config
        .output_dir
        .join(format!("starlink_gp_history_{stem}.zip"));
    let merged_csv_path = config
        .output_dir
        .join(format!("starlink_gp_history_{stem}.csv"));
    let stable_csv_path = config.output_dir.join("starlink_gp_history.csv");
    let urls_csv_path = config
        .output_dir
        .join(format!("gp_history_urls_{stem}.csv"));

    if config.dry_run {
        print_plan(
            &config,
            &norad_ids,
            &query_window,
            &zip_path,
            &merged_csv_path,
            &stable_csv_path,
            &urls_csv_path,
        );
        return Ok(());
    }

    fs::create_dir_all(&config.output_dir)?;
    let credentials = load_credentials(config.identity)?;
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(&credentials, cookie_jar.path())?;

    download_history(
        cookie_jar.path(),
        &config.output_dir,
        &norad_ids,
        config.chunk_size,
        &query_window,
    )?;

    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut input = root.join("data/starlink_satcat.csv");
    let mut output_dir = root.join("starlink-group-1");
    let mut start_date = None;
    let mut end_date = None;
    let mut chunk_size = 20usize;
    let mut identity = None;
    let mut dry_run = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                input = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --input")
                })?);
            }
            "--output-dir" => {
                output_dir = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --output-dir",
                    )
                })?);
            }
            "--start-date" => {
                start_date = Some(args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --start-date",
                    )
                })?);
            }
            "--end-date" => {
                end_date = Some(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --end-date")
                })?);
            }
            "--chunk-size" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --chunk-size",
                    )
                })?;
                chunk_size = value.parse::<usize>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid --chunk-size '{}': {}", value, error),
                    )
                })?;
                if chunk_size == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "--chunk-size must be greater than 0",
                    ));
                }
            }
            "--identity" => {
                identity = Some(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --identity")
                })?);
            }
            "--dry-run" => dry_run = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                ));
            }
        }
    }

    Ok(Config {
        input,
        output_dir,
        start_date,
        end_date,
        chunk_size,
        identity,
        dry_run,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_group_1_gp_history -- [options]");
    println!("Options:");
    println!("  --input PATH");
    println!("  --output-dir PATH");
    println!("  --start-date YYYY-MM-DD");
    println!("  --end-date YYYY-MM-DD");
    println!("  --chunk-size N");
    println!("  --identity USER");
    println!("  --dry-run");
    println!();
    println!("Defaults:");
    println!("  input: data/starlink_satcat.csv");
    println!("  output-dir: starlink-group-1");
    println!("  date window: full history unless --start-date/--end-date are provided");
    println!("  chunk-size: 20");
}

fn load_norad_ids(path: &Path) -> io::Result<Vec<String>> {
    let csv_body = fs::read_to_string(path)?;
    let mut lines = csv_body.lines();
    let header = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "input CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(header));
    let norad_index = require_column(&header_map, "norad_cat_id")?;

    let mut unique = Vec::new();
    let mut seen = BTreeSet::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(line);
        let value = get_field(&fields, norad_index, "norad_cat_id")?;
        if !value.is_empty() && seen.insert(value.clone()) {
            unique.push(value);
        }
    }

    if unique.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no norad_cat_id values found in input CSV",
        ));
    }

    Ok(unique)
}

fn print_plan(
    config: &Config,
    ids: &[String],
    query_window: &QueryWindow,
    zip_path: &Path,
    merged_csv_path: &Path,
    stable_csv_path: &Path,
    urls_csv_path: &Path,
) {
    let zip_url = build_gp_history_zip_url(ids, query_window);
    let chunks = split_ids(ids, config.chunk_size);
    println!("input={}", config.input.display());
    println!("output_dir={}", config.output_dir.display());
    println!("window={}", describe_query_window(query_window));
    println!("zip_output={}", zip_path.display());
    println!("merged_csv={}", merged_csv_path.display());
    println!("stable_csv={}", stable_csv_path.display());
    println!("urls_csv={}", urls_csv_path.display());
    println!("norad_cat_id_count={}", ids.len());
    println!("chunk_count={}", chunks.len());
    println!("zip_url={zip_url}");
    if let Some(first_chunk) = chunks.first() {
        let first_batch_urls = build_gp_history_csv_url_candidates(first_chunk, query_window);
        println!("first_batch_url={}", first_batch_urls[0]);
    }
}

fn download_history(
    cookie_path: &Path,
    output_dir: &Path,
    ids: &[String],
    chunk_size: usize,
    query_window: &QueryWindow,
) -> io::Result<()> {
    let stem = window_file_stem(query_window);
    let zip_path = output_dir.join(format!("starlink_gp_history_{stem}.zip"));
    let merged_csv_path = output_dir.join(format!("starlink_gp_history_{stem}.csv"));
    let stable_csv_path = output_dir.join("starlink_gp_history.csv");
    let urls_csv_path = output_dir.join(format!("gp_history_urls_{stem}.csv"));

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
        if csv_paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "zip download succeeded but no CSV files were found in {}",
                    extracted_dir.display()
                ),
            ));
        }
        merge_csv_files(&csv_paths, &merged_csv_path)?;
        fs::copy(&merged_csv_path, &stable_csv_path)?;
        eprintln!("Downloaded ZIP: {}", zip_path.display());
        eprintln!("Merged CSV: {}", merged_csv_path.display());
        eprintln!("Stable CSV: {}", stable_csv_path.display());
        return Ok(());
    }

    let batch_dir = output_dir.join("batches");
    fs::create_dir_all(&batch_dir)?;
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
        )?;
    }
    if batch_paths.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "all GP_HISTORY batch requests came back empty or invalid",
        ));
    }

    merge_csv_files(&batch_paths, &merged_csv_path)?;
    fs::copy(&merged_csv_path, &stable_csv_path)?;
    eprintln!("Merged CSV: {}", merged_csv_path.display());
    eprintln!("Stable CSV: {}", stable_csv_path.display());
    Ok(())
}

fn download_chunk_recursive(
    cookie_path: &Path,
    ids: &[String],
    query_window: &QueryWindow,
    batch_dir: &Path,
    next_batch_index: &mut usize,
    batch_paths: &mut Vec<PathBuf>,
) -> io::Result<()> {
    let batch_number = *next_batch_index;
    *next_batch_index += 1;

    let batch_path = batch_dir.join(format!("batch_{:03}.csv", batch_number));
    let urls = build_gp_history_csv_url_candidates(ids, query_window);
    let mut last_error = None::<io::Error>;

    for (variant_index, url) in urls.iter().enumerate() {
        download_binary(cookie_path, url, &batch_path)?;
        match inspect_csv_response(&batch_path) {
            Ok(CsvResponseCheck::Valid) => {
                eprintln!(
                    "Saved batch {} with {} NORAD IDs: {}",
                    batch_number,
                    ids.len(),
                    batch_path.display()
                );
                batch_paths.push(batch_path.clone());
                return Ok(());
            }
            Ok(CsvResponseCheck::Empty) => {
                let _ = fs::remove_file(&batch_path);
                eprintln!(
                    "Skipped empty batch {} with {} NORAD IDs",
                    batch_number,
                    ids.len()
                );
                return Ok(());
            }
            Err(error) => {
                last_error = Some(error);
                let diagnostic_path = diagnostic_path_for(&batch_path, variant_index + 1);
                let _ = fs::rename(&batch_path, &diagnostic_path);
            }
        }
    }

    if ids.len() > 1 {
        let split_at = ids.len() / 2;
        let (left, right) = ids.split_at(split_at);
        eprintln!(
            "Splitting failed batch {} ({} NORAD IDs) into {} and {} IDs",
            batch_number,
            ids.len(),
            left.len(),
            right.len()
        );
        download_chunk_recursive(
            cookie_path,
            left,
            query_window,
            batch_dir,
            next_batch_index,
            batch_paths,
        )?;
        download_chunk_recursive(
            cookie_path,
            right,
            query_window,
            batch_dir,
            next_batch_index,
            batch_paths,
        )?;
        return Ok(());
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "response did not look like GP_HISTORY CSV for NORAD {} in {}",
                ids.first().map(String::as_str).unwrap_or("unknown"),
                batch_dir.display()
            ),
        )
    }))
}

fn diagnostic_path_for(batch_path: &Path, variant_index: usize) -> PathBuf {
    let stem = batch_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("batch");
    batch_path.with_file_name(format!("{stem}_variant_{variant_index}.txt"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_unique_norad_ids_from_catalog() {
        let dir = env::temp_dir();
        let path = dir.join(format!("starlink-group-1-test-{}.csv", std::process::id()));
        fs::write(
            &path,
            "norad_cat_id,satname\n44713,STARLINK-1007\n44713,STARLINK-1007\n44714,STARLINK-1008\n",
        )
        .unwrap();

        let ids = load_norad_ids(&path).unwrap();
        assert_eq!(ids, vec!["44713", "44714"]);

        let _ = fs::remove_file(path);
    }
}
