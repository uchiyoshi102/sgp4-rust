use sgp4::spacex::catalog::{load_catalog_rows, unique_norad_ids};
use sgp4::starlink::spacetrack::{
    build_gp_history_csv_url_candidates, build_gp_history_zip_url, build_query_window, can_try_zip,
    download_binary, extract_zip, inspect_csv_response, list_csv_files, load_credentials,
    login_to_space_track, merge_csv_files, split_ids, try_zip_download, window_file_stem,
    write_url_manifest, CookieJar, CsvResponseCheck, QueryWindow,
};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct Config {
    catalog: PathBuf,
    output_dir: PathBuf,
    start_date: Option<String>,
    end_date: Option<String>,
    chunk_size: usize,
    identity: Option<String>,
    dry_run: bool,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let rows = load_catalog_rows(&config.catalog)?;
    let ids = unique_norad_ids(&rows);
    let query_window = build_query_window(config.start_date.clone(), config.end_date.clone())?;

    if config.dry_run {
        println!("norad_cat_id_count={}", ids.len());
        println!("zip_url={}", build_gp_history_zip_url(&ids, &query_window));
        return Ok(());
    }

    let credentials = load_credentials(config.identity)?;
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(&credentials, cookie_jar.path())?;
    fs::create_dir_all(&config.output_dir)?;
    download_history(
        cookie_jar.path(),
        &config.output_dir,
        &ids,
        config.chunk_size,
        &query_window,
    )?;
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut catalog = root.join("data/spacex_leo_catalog.csv");
    let mut output_dir = root.join("spacex-leo");
    let mut start_date = None;
    let mut end_date = None;
    let mut chunk_size = 20usize;
    let mut identity = None;
    let mut dry_run = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--catalog" => {
                catalog = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --catalog")
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
                chunk_size = args
                    .next()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "missing value for --chunk-size",
                        )
                    })?
                    .parse::<usize>()
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --chunk-size: {error}"),
                        )
                    })?;
            }
            "--identity" => {
                identity = Some(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --identity")
                })?);
            }
            "--dry-run" => dry_run = true,
            "--help" | "-h" => {
                println!("Usage: cargo run --bin spacex_leo_gp_history -- [options]");
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
        output_dir,
        start_date,
        end_date,
        chunk_size,
        identity,
        dry_run,
    })
}

fn download_history(
    cookie_path: &Path,
    output_dir: &Path,
    ids: &[String],
    chunk_size: usize,
    query_window: &QueryWindow,
) -> io::Result<()> {
    let stem = window_file_stem(query_window);
    let zip_path = output_dir.join(format!("spacex_leo_gp_history_{stem}.zip"));
    let merged_csv_path = output_dir.join(format!("spacex_leo_gp_history_{stem}.csv"));
    let stable_csv_path = output_dir.join("spacex_leo_gp_history.csv");
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
        merge_csv_files(&csv_paths, &merged_csv_path)?;
        fs::copy(&merged_csv_path, &stable_csv_path)?;
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
) -> io::Result<()> {
    let batch_number = *next_batch_index;
    *next_batch_index += 1;
    let batch_path = batch_dir.join(format!("batch_{batch_number:03}.csv"));
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
            "response did not look like GP_HISTORY CSV",
        )
    }))
}
