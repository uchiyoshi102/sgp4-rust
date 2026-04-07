use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const SPACE_TRACK_LOGIN_URL: &str = "https://www.space-track.org/ajaxauth/login";
const ZIP_URL_LENGTH_LIMIT: usize = 1800;

#[derive(Debug)]
struct Config {
    input: PathBuf,
    output_dir: PathBuf,
    start_date: String,
    end_date: String,
    chunk_size: usize,
    identity: Option<String>,
    dry_run: bool,
}

#[derive(Debug)]
struct Credentials {
    identity: String,
    password: String,
}

enum CsvResponseCheck {
    Valid,
    Empty,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let norad_ids = load_norad_ids(&config.input)?;
    let exclusive_end = next_date(&config.end_date)?;

    let zip_path = config.output_dir.join(format!(
        "starlink_gp_history_{}_{}.zip",
        config.start_date, config.end_date
    ));
    let merged_csv_path = config.output_dir.join(format!(
        "starlink_gp_history_{}_{}.csv",
        config.start_date, config.end_date
    ));
    let urls_csv_path = config.output_dir.join(format!(
        "gp_history_urls_{}_{}.csv",
        config.start_date, config.end_date
    ));

    if config.dry_run {
        print_plan(
            &config,
            &norad_ids,
            &exclusive_end,
            &zip_path,
            &merged_csv_path,
            &urls_csv_path,
        )?;
        return Ok(());
    }

    fs::create_dir_all(&config.output_dir)?;
    let credentials = load_credentials(config.identity.clone())?;
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(&credentials, cookie_jar.path())?;

    let zip_url = build_gp_history_zip_url(&norad_ids, &config.start_date, &exclusive_end);
    let chunks = split_ids(&norad_ids, config.chunk_size);
    let batch_urls = chunks
        .iter()
        .map(|chunk| build_gp_history_csv_url_candidates(chunk, &config.start_date, &exclusive_end))
        .collect::<Vec<_>>();
    write_url_manifest(&zip_url, &batch_urls, &urls_csv_path)?;

    if zip_url.len() <= ZIP_URL_LENGTH_LIMIT
        && try_zip_download(cookie_jar.path(), &zip_url, &zip_path)?
    {
        let extracted_dir = config.output_dir.join("zip_contents");
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
        eprintln!("Downloaded ZIP: {}", zip_path.display());
        eprintln!("Merged CSV: {}", merged_csv_path.display());
        return Ok(());
    } else if zip_url.len() > ZIP_URL_LENGTH_LIMIT {
        eprintln!(
            "Skipping ZIP download because the URL length ({}) exceeds the safe limit ({}).",
            zip_url.len(),
            ZIP_URL_LENGTH_LIMIT
        );
    }

    let batch_dir = config.output_dir.join("batches");
    fs::create_dir_all(&batch_dir)?;
    let mut batch_paths = Vec::new();
    for (index, urls) in batch_urls.iter().enumerate() {
        let batch_path = batch_dir.join(format!("batch_{:03}.csv", index + 1));
        let response_kind = download_first_usable_csv(cookie_jar.path(), urls, &batch_path)?;
        if matches!(response_kind, CsvResponseCheck::Empty) {
            eprintln!(
                "Skipped empty batch {}/{}: {}",
                index + 1,
                batch_urls.len(),
                batch_path.display()
            );
            continue;
        }
        eprintln!(
            "Saved batch {}/{}: {}",
            index + 1,
            batch_urls.len(),
            batch_path.display()
        );
        batch_paths.push(batch_path);
    }
    if batch_paths.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "all GP_HISTORY batch requests came back empty or invalid",
        ));
    }
    merge_csv_files(&batch_paths, &merged_csv_path)?;
    eprintln!("Merged CSV: {}", merged_csv_path.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut input = root.join("data/starlink_satcat.csv");
    let mut output_dir = root.join("starlink-group-1");
    let mut start_date = "2021-07-15".to_string();
    let mut end_date = "2024-08-02".to_string();
    let mut chunk_size = 20usize;
    let mut identity = None;
    let mut dry_run = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --input")
                })?;
                input = PathBuf::from(value);
            }
            "--output-dir" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --output-dir",
                    )
                })?;
                output_dir = PathBuf::from(value);
            }
            "--start-date" => {
                start_date = args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --start-date",
                    )
                })?;
            }
            "--end-date" => {
                end_date = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --end-date")
                })?;
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
                let value = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --identity")
                })?;
                identity = Some(value);
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

    validate_date(&start_date, "--start-date")?;
    validate_date(&end_date, "--end-date")?;

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
    println!("  start-date: 2021-07-15");
    println!("  end-date: 2024-08-02");
    println!("  chunk-size: 20");
}

fn validate_date(value: &str, flag: &str) -> io::Result<()> {
    let bytes = value.as_bytes();
    let valid = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} must be YYYY-MM-DD, got '{}'", flag, value),
        ))
    }
}

fn load_norad_ids(path: &Path) -> io::Result<Vec<String>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "input CSV is empty"))?;
    let header_fields = parse_csv_line(&header);
    let norad_index = header_fields
        .iter()
        .position(|field| field == "norad_cat_id")
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "input CSV is missing norad_cat_id column",
            )
        })?;

    let mut ids = Vec::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(&line);
        let value = fields.get(norad_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "row did not contain norad_cat_id column",
            )
        })?;
        ids.push(value.trim().to_string());
    }

    let mut unique = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for id in ids {
        if !id.is_empty() && seen.insert(id.clone()) {
            unique.push(id);
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

fn split_ids(ids: &[String], chunk_size: usize) -> Vec<Vec<String>> {
    ids.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
}

fn next_date(date: &str) -> io::Result<String> {
    validate_date(date, "date")?;
    let year = date[0..4].parse::<i32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid year in '{}': {}", date, error),
        )
    })?;
    let month = date[5..7].parse::<u32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid month in '{}': {}", date, error),
        )
    })?;
    let day = date[8..10].parse::<u32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid day in '{}': {}", date, error),
        )
    })?;

    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid month in '{}'", date),
            ))
        }
    };
    if day == 0 || day > days_in_month {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid day in '{}'", date),
        ));
    }

    let (next_year, next_month, next_day) = if day < days_in_month {
        (year, month, day + 1)
    } else if month < 12 {
        (year, month + 1, 1)
    } else {
        (year + 1, 1, 1)
    };

    Ok(format!(
        "{:04}-{:02}-{:02}",
        next_year, next_month, next_day
    ))
}

fn print_plan(
    config: &Config,
    ids: &[String],
    exclusive_end: &str,
    zip_path: &Path,
    merged_csv_path: &Path,
    urls_csv_path: &Path,
) -> io::Result<()> {
    let zip_url = build_gp_history_zip_url(ids, &config.start_date, exclusive_end);
    let chunks = split_ids(ids, config.chunk_size);
    println!("input={}", config.input.display());
    println!("output_dir={}", config.output_dir.display());
    println!("zip_output={}", zip_path.display());
    println!("merged_csv={}", merged_csv_path.display());
    println!("urls_csv={}", urls_csv_path.display());
    println!("norad_cat_id_count={}", ids.len());
    println!("chunk_count={}", chunks.len());
    println!("zip_url={}", zip_url);
    if let Some(first_chunk) = chunks.first() {
        let first_batch_urls =
            build_gp_history_csv_url_candidates(first_chunk, &config.start_date, exclusive_end);
        println!("first_batch_url={}", first_batch_urls[0]);
    }
    Ok(())
}

fn load_credentials(cli_identity: Option<String>) -> io::Result<Credentials> {
    let identity = cli_identity
        .or_else(|| env::var("SPACE_TRACK_IDENTITY").ok())
        .unwrap_or(prompt("Space-Track username/email: ")?);
    let password = env::var("SPACE_TRACK_PASSWORD")
        .ok()
        .unwrap_or(prompt_password("Space-Track password: ")?);

    if identity.trim().is_empty() || password.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Space-Track credentials must not be empty",
        ));
    }

    Ok(Credentials { identity, password })
}

fn prompt(message: &str) -> io::Result<String> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(message.as_bytes())?;
    stdout.flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

fn prompt_password(message: &str) -> io::Result<String> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(message.as_bytes())?;
    stdout.flush()?;

    let hide_echo = Command::new("stty")
        .arg("-echo")
        .stdin(Stdio::inherit())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    let mut input = String::new();
    let read_result = io::stdin().read_line(&mut input);

    if hide_echo {
        let _ = Command::new("stty")
            .arg("echo")
            .stdin(Stdio::inherit())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        println!();
    }

    read_result?;
    Ok(input.trim_end_matches(&['\r', '\n'][..]).to_string())
}

fn login_to_space_track(credentials: &Credentials, cookie_path: &Path) -> io::Result<()> {
    run_command(
        Command::new("curl")
            .arg("-L")
            .arg("-sS")
            .arg("-c")
            .arg(path_as_str(cookie_path)?)
            .arg("--data-urlencode")
            .arg(format!("identity={}", credentials.identity))
            .arg("--data-urlencode")
            .arg(format!("password={}", credentials.password))
            .arg(SPACE_TRACK_LOGIN_URL),
        "failed to login to Space-Track",
    )?;
    Ok(())
}

fn build_gp_history_zip_url(ids: &[String], start_date: &str, exclusive_end: &str) -> String {
    format!(
        "https://www.space-track.org/basicspacedata/query/class/gp_history/norad_cat_id/{}/CREATION_DATE/%3E{}/CREATION_DATE/%3C{}/format/zip",
        ids.join(","),
        start_date,
        exclusive_end
    )
}

fn build_gp_history_csv_url_candidates(
    ids: &[String],
    start_date: &str,
    exclusive_end: &str,
) -> Vec<String> {
    vec![
        format!(
            "https://www.space-track.org/basicspacedata/query/class/gp_history/norad_cat_id/{}/CREATION_DATE/%3E{}/CREATION_DATE/%3C{}/format/csv/emptyresult/show",
            ids.join(","),
            start_date,
            exclusive_end
        ),
        format!(
            "https://www.space-track.org/basicspacedata/query/class/gp_history/norad_cat_id/{}/CREATION_DATE/{}/--{}/format/csv/emptyresult/show",
            ids.join(","),
            start_date,
            exclusive_end
        ),
        format!(
            "https://www.space-track.org/basicspacedata/query/class/gp_history/NORAD_CAT_ID/{}/CREATION_DATE/%3E{}/CREATION_DATE/%3C{}/format/csv/emptyresult/show",
            ids.join(","),
            start_date,
            exclusive_end
        ),
    ]
}

fn write_url_manifest(
    zip_url: &str,
    batch_urls: &[Vec<String>],
    output_path: &Path,
) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(output_path)?);
    writeln!(writer, "kind,index,variant,url")?;
    writeln!(writer, "zip,0,0,{}", csv_escape(zip_url))?;
    for (index, urls) in batch_urls.iter().enumerate() {
        for (variant, url) in urls.iter().enumerate() {
            writeln!(
                writer,
                "batch,{},{},{}",
                index + 1,
                variant + 1,
                csv_escape(url)
            )?;
        }
    }
    writer.flush()
}

fn try_zip_download(cookie_path: &Path, url: &str, zip_path: &Path) -> io::Result<bool> {
    let response_path = zip_path.with_extension("response");
    download_binary(cookie_path, url, &response_path)?;
    if is_zip_file(&response_path)? {
        fs::rename(&response_path, zip_path)?;
        return Ok(true);
    }
    let diagnostic_path = zip_path.with_extension("txt");
    fs::rename(&response_path, &diagnostic_path)?;
    eprintln!(
        "ZIP download did not return a ZIP file. Saved response to {} and falling back to batch CSV.",
        diagnostic_path.display()
    );
    Ok(false)
}

fn download_binary(cookie_path: &Path, url: &str, output_path: &Path) -> io::Result<()> {
    run_command(
        Command::new("curl")
            .arg("-L")
            .arg("-sS")
            .arg("-b")
            .arg(path_as_str(cookie_path)?)
            .arg("-c")
            .arg(path_as_str(cookie_path)?)
            .arg("-o")
            .arg(path_as_str(output_path)?)
            .arg(url),
        &format!("failed to download {}", url),
    )
}

fn download_text(cookie_path: &Path, url: &str, output_path: &Path) -> io::Result<()> {
    download_binary(cookie_path, url, output_path)
}

fn download_first_usable_csv(
    cookie_path: &Path,
    urls: &[String],
    output_path: &Path,
) -> io::Result<CsvResponseCheck> {
    let mut last_error = None::<io::Error>;
    for url in urls {
        download_text(cookie_path, url, output_path)?;
        match inspect_csv_response(output_path) {
            Ok(result) => return Ok(result),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "response did not look like GP_HISTORY CSV: {}",
                output_path.display()
            ),
        )
    }))
}

fn is_zip_file(path: &Path) -> io::Result<bool> {
    let bytes = fs::read(path)?;
    Ok(bytes.len() >= 4 && bytes[0..4] == [0x50, 0x4B, 0x03, 0x04])
}

fn inspect_csv_response(path: &Path) -> io::Result<CsvResponseCheck> {
    let text = fs::read_to_string(path)?;
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "\"\"" || trimmed == "[]" {
        return Ok(CsvResponseCheck::Empty);
    }
    let Some(first_non_empty) = text.lines().find(|line| !line.trim().is_empty()) else {
        return Ok(CsvResponseCheck::Empty);
    };
    let upper = first_non_empty.to_ascii_uppercase();
    if first_non_empty.starts_with('<') || upper.contains("REQUEST-URI TOO LONG") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "response was HTML or gateway error, not GP_HISTORY CSV: {}",
                path.display()
            ),
        ));
    }
    if first_non_empty.contains(',') && upper.contains("NORAD_CAT_ID") {
        return Ok(CsvResponseCheck::Valid);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "response did not look like GP_HISTORY CSV: {}",
            path.display()
        ),
    ))
}

fn extract_zip(zip_path: &Path, output_dir: &Path) -> io::Result<()> {
    fs::create_dir_all(output_dir)?;
    run_command(
        Command::new("unzip")
            .arg("-o")
            .arg(path_as_str(zip_path)?)
            .arg("-d")
            .arg(path_as_str(output_dir)?),
        &format!("failed to extract {}", zip_path.display()),
    )
}

fn list_csv_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("csv"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn merge_csv_files(paths: &[PathBuf], output_path: &Path) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(output_path)?);
    let mut expected_header = None::<String>;
    let mut wrote_header = false;

    for path in paths {
        let reader = BufReader::new(File::open(path)?);
        let mut lines = reader.lines();
        let Some(header) = lines.next().transpose()? else {
            continue;
        };

        match &expected_header {
            Some(expected) if expected != &header => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("CSV header mismatch while merging {}", path.display()),
                ));
            }
            None => expected_header = Some(header.clone()),
            _ => {}
        }

        if !wrote_header {
            writeln!(writer, "{header}")?;
            wrote_header = true;
        }

        for line in lines {
            let line = line?;
            if !line.trim().is_empty() {
                writeln!(writer, "{line}")?;
            }
        }
    }

    writer.flush()
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && matches!(chars.peek(), Some('"')) {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => fields.push(std::mem::take(&mut field)),
            _ => field.push(ch),
        }
    }

    fields.push(field);
    fields
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn path_as_str(path: &Path) -> io::Result<&str> {
    path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains non-UTF-8 characters: {}", path.display()),
        )
    })
}

fn run_command(command: &mut Command, context: &str) -> io::Result<()> {
    let output = command
        .output()
        .map_err(|error| io::Error::new(io::ErrorKind::Other, format!("{}: {}", context, error)))?;
    if output.status.success() {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::Other,
        format!(
            "{}: {}",
            context,
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    ))
}

struct CookieJar {
    path: PathBuf,
}

impl CookieJar {
    fn new() -> io::Result<Self> {
        let path = env::temp_dir().join(format!(
            "space-track-cookies-{}-{}.txt",
            std::process::id(),
            current_timestamp_nanos()?
        ));
        File::create(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CookieJar {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn current_timestamp_nanos() -> io::Result<u128> {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|error| io::Error::new(io::ErrorKind::Other, format!("clock error: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_next_date() {
        assert_eq!(next_date("2024-08-02").unwrap(), "2024-08-03");
        assert_eq!(next_date("2024-12-31").unwrap(), "2025-01-01");
        assert_eq!(next_date("2024-02-28").unwrap(), "2024-02-29");
    }

    #[test]
    fn splits_ids_by_chunk_size() {
        let ids = vec!["1".into(), "2".into(), "3".into(), "4".into(), "5".into()];
        let chunks = split_ids(&ids, 2);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], vec!["1", "2"]);
        assert_eq!(chunks[2], vec!["5"]);
    }

    #[test]
    fn builds_csv_url() {
        let url = build_gp_history_csv_url_candidates(
            &["44713".into(), "44714".into(), "44715".into()],
            "2021-07-15",
            "2024-08-03",
        )[0]
        .clone();
        assert!(url.contains("/norad_cat_id/44713,44714,44715/"));
        assert!(url.contains("/CREATION_DATE/%3E2021-07-15/"));
        assert!(url.contains("/CREATION_DATE/%3C2024-08-03/"));
        assert!(url.ends_with("/format/csv/emptyresult/show"));
    }

    #[test]
    fn parses_csv_quotes() {
        let fields = parse_csv_line("\"44713\",\"STARLINK-1007\"");
        assert_eq!(fields, vec!["44713", "STARLINK-1007"]);
    }

    #[test]
    fn recognizes_zip_signature() {
        let dir = env::temp_dir();
        let path = dir.join(format!("zip-test-{}.bin", std::process::id()));
        fs::write(&path, [0x50, 0x4B, 0x03, 0x04, 0x14, 0x00]).unwrap();
        assert!(is_zip_file(&path).unwrap());
        let _ = fs::remove_file(path);
    }
}
