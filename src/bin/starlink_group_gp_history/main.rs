use sgp4::starlink::manifest::{group_catalog_rows, load_catalog_rows, unique_norad_ids};
use sgp4::starlink::spacetrack::{
    build_gp_history_csv_url_candidates, build_gp_history_zip_url, build_query_window, can_try_zip,
    describe_query_window, download_first_usable_csv, extract_zip, list_csv_files, load_credentials,
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
    group_slug: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    chunk_size: usize,
    identity: Option<String>,
    dry_run: bool,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let catalog_rows = load_catalog_rows(&config.catalog)?;
    let grouped = group_catalog_rows(&catalog_rows);
    let query_window = build_query_window(config.start_date.clone(), config.end_date.clone())?;

    let selected_groups = grouped
        .into_iter()
        .filter(|(group_slug, _)| {
            config
                .group_slug
                .as_ref()
                .map(|value| value == group_slug)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    if selected_groups.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no groups matched --group or the catalog was empty",
        ));
    }

    if config.dry_run {
        for (group_slug, rows) in &selected_groups {
            let ids = unique_norad_ids(rows);
            let zip_url = build_gp_history_zip_url(&ids, &query_window);
            let batch_urls = split_ids(&ids, config.chunk_size)
                .iter()
                .map(|chunk| build_gp_history_csv_url_candidates(chunk, &query_window))
                .collect::<Vec<_>>();
            println!("group_slug={group_slug}");
            println!("norad_cat_id_count={}", ids.len());
            println!("window={}", describe_query_window(&query_window));
            println!("zip_url={zip_url}");
            if let Some(first_batch) = batch_urls.first() {
                println!("first_batch_url={}", first_batch[0]);
            }
            println!();
        }
        return Ok(());
    }

    let credentials = load_credentials(config.identity)?;
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(&credentials, cookie_jar.path())?;

    for (group_slug, rows) in &selected_groups {
        let group_dir = config.output_dir.join(group_slug);
        fs::create_dir_all(&group_dir)?;
        let ids = unique_norad_ids(rows);
        download_group_history(
            cookie_jar.path(),
            &group_dir,
            &ids,
            config.chunk_size,
            &query_window,
        )?;
        eprintln!(
            "Downloaded group {} with {} NORAD IDs into {}",
            group_slug,
            ids.len(),
            group_dir.display()
        );
    }

    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut catalog = root.join("data/starlink_group_catalog.csv");
    let mut output_dir = root.join("starlink-groups");
    let mut group_slug = None;
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
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --output-dir")
                })?);
            }
            "--group" => {
                group_slug = Some(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --group")
                })?);
            }
            "--start-date" => {
                start_date = Some(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --start-date")
                })?);
            }
            "--end-date" => {
                end_date = Some(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --end-date")
                })?);
            }
            "--chunk-size" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --chunk-size")
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
                ))
            }
        }
    }

    Ok(Config {
        catalog,
        output_dir,
        group_slug,
        start_date,
        end_date,
        chunk_size,
        identity,
        dry_run,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_group_gp_history -- [options]");
    println!("Options:");
    println!("  --catalog PATH");
    println!("  --output-dir PATH");
    println!("  --group GROUP_SLUG");
    println!("  --start-date YYYY-MM-DD");
    println!("  --end-date YYYY-MM-DD");
    println!("  --chunk-size N");
    println!("  --identity USER");
    println!("  --dry-run");
    println!();
    println!("Defaults:");
    println!("  catalog: data/starlink_group_catalog.csv");
    println!("  output-dir: starlink-groups");
    println!("  chunk-size: 20");
    println!("  date window: full history unless --start-date/--end-date are provided");
}

fn download_group_history(
    cookie_path: &Path,
    group_dir: &Path,
    ids: &[String],
    chunk_size: usize,
    query_window: &QueryWindow,
) -> io::Result<()> {
    let stem = window_file_stem(query_window);
    let zip_path = group_dir.join(format!("starlink_gp_history_{stem}.zip"));
    let merged_csv_path = group_dir.join(format!("starlink_gp_history_{stem}.csv"));
    let stable_csv_path = group_dir.join("starlink_gp_history.csv");
    let urls_csv_path = group_dir.join(format!("gp_history_urls_{stem}.csv"));

    let zip_url = build_gp_history_zip_url(ids, query_window);
    let chunks = split_ids(ids, chunk_size);
    let batch_urls = chunks
        .iter()
        .map(|chunk| build_gp_history_csv_url_candidates(chunk, query_window))
        .collect::<Vec<_>>();
    write_url_manifest(&zip_url, &batch_urls, &urls_csv_path)?;

    if can_try_zip(&zip_url) && try_zip_download(cookie_path, &zip_url, &zip_path)? {
        let extracted_dir = group_dir.join("zip_contents");
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
        return Ok(());
    }

    let batch_dir = group_dir.join("batches");
    fs::create_dir_all(&batch_dir)?;
    let mut batch_paths = Vec::new();
    for (index, urls) in batch_urls.iter().enumerate() {
        let batch_path = batch_dir.join(format!("batch_{:03}.csv", index + 1));
        let response_kind = download_first_usable_csv(cookie_path, urls, &batch_path)?;
        if matches!(response_kind, CsvResponseCheck::Empty) {
            eprintln!(
                "Skipped empty batch {}/{}: {}",
                index + 1,
                batch_urls.len(),
                batch_path.display()
            );
            continue;
        }
        batch_paths.push(batch_path);
    }
    if batch_paths.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "all GP_HISTORY batch requests came back empty or invalid",
        ));
    }
    merge_csv_files(&batch_paths, &merged_csv_path)?;
    fs::copy(&merged_csv_path, &stable_csv_path)?;
    Ok(())
}
