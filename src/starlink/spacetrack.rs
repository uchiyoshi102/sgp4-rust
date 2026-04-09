use crate::starlink::csv::{csv_escape, looks_like_html, next_date, path_as_str};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub const SPACE_TRACK_LOGIN_URL: &str = "https://www.space-track.org/ajaxauth/login";
const ZIP_URL_LENGTH_LIMIT: usize = 1800;

#[derive(Debug)]
pub struct Credentials {
    pub identity: String,
    pub password: String,
}

#[derive(Clone, Debug)]
pub enum QueryWindow {
    FullHistory,
    Bounded {
        start_date: String,
        end_date: String,
        exclusive_end: String,
    },
}

pub enum CsvResponseCheck {
    Valid,
    Empty,
}

pub struct CookieJar {
    path: PathBuf,
}

impl CookieJar {
    pub fn new() -> io::Result<Self> {
        let path = env::temp_dir().join(format!(
            "space-track-cookies-{}-{}.txt",
            std::process::id(),
            current_timestamp_nanos()?
        ));
        File::create(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CookieJar {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn load_credentials(cli_identity: Option<String>) -> io::Result<Credentials> {
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

pub fn prompt(message: &str) -> io::Result<String> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(message.as_bytes())?;
    stdout.flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

pub fn prompt_password(message: &str) -> io::Result<String> {
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

pub fn login_to_space_track(credentials: &Credentials, cookie_path: &Path) -> io::Result<()> {
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
    )
}

pub fn run_curl_text(args: &[&str]) -> io::Result<String> {
    let output = Command::new("curl").args(args).output().map_err(|error| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("failed to start curl, make sure it is installed: {error}"),
        )
    })?;

    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "curl failed with status {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }

    String::from_utf8(output.stdout).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("curl returned non-UTF-8 output: {error}"),
        )
    })
}

pub fn fetch_csv_text(cookie_path: &Path, url: &str) -> io::Result<String> {
    let body = run_curl_text(&[
        "-L",
        "-sS",
        "-b",
        path_as_str(cookie_path)?,
        "-c",
        path_as_str(cookie_path)?,
        url,
    ])?;

    if looks_like_html(&body) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Space-Track returned HTML instead of CSV. Login likely failed or the session was rejected.",
        ));
    }

    Ok(body)
}

pub fn build_satcat_query_url(start_date: &str, end_date: &str) -> String {
    format!(
        "https://www.space-track.org/basicspacedata/query/class/satcat/LAUNCH/{}--{}/predicates/NORAD_CAT_ID,SATNAME,LAUNCH/format/csv/emptyresult/show",
        start_date, end_date
    )
}

pub fn build_query_window(
    start_date: Option<String>,
    end_date: Option<String>,
) -> io::Result<QueryWindow> {
    match (start_date, end_date) {
        (None, None) => Ok(QueryWindow::FullHistory),
        (Some(start_date), Some(end_date)) => Ok(QueryWindow::Bounded {
            exclusive_end: next_date(&end_date)?,
            start_date,
            end_date,
        }),
        (Some(_), None) | (None, Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--start-date and --end-date must be provided together, or neither for full history",
        )),
    }
}

pub fn describe_query_window(window: &QueryWindow) -> String {
    match window {
        QueryWindow::FullHistory => "full-history".to_string(),
        QueryWindow::Bounded {
            start_date,
            end_date,
            ..
        } => format!("{}..{}", start_date, end_date),
    }
}

pub fn window_file_stem(window: &QueryWindow) -> String {
    match window {
        QueryWindow::FullHistory => "full_history".to_string(),
        QueryWindow::Bounded {
            start_date,
            end_date,
            ..
        } => format!("{}_{}", start_date, end_date),
    }
}

pub fn build_gp_history_zip_url(ids: &[String], window: &QueryWindow) -> String {
    build_gp_history_url(ids, window, "zip", false)
}

pub fn build_gp_history_csv_url_candidates(ids: &[String], window: &QueryWindow) -> Vec<String> {
    let mut urls = Vec::new();
    urls.push(build_gp_history_url(ids, window, "csv", false));
    urls.push(build_gp_history_url(ids, window, "csv", true));
    urls
}

fn build_gp_history_url(ids: &[String], window: &QueryWindow, format: &str, upper_case: bool) -> String {
    let field_name = if upper_case {
        "NORAD_CAT_ID"
    } else {
        "norad_cat_id"
    };
    match window {
        QueryWindow::FullHistory => format!(
            "https://www.space-track.org/basicspacedata/query/class/gp_history/{}/{}/format/{}/emptyresult/show",
            field_name,
            ids.join(","),
            format
        ),
        QueryWindow::Bounded {
            start_date,
            exclusive_end,
            ..
        } => format!(
            "https://www.space-track.org/basicspacedata/query/class/gp_history/{}/{}/CREATION_DATE/%3E{}/CREATION_DATE/%3C{}/format/{}/emptyresult/show",
            field_name,
            ids.join(","),
            start_date,
            exclusive_end,
            format
        ),
    }
}

pub fn write_url_manifest(
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

pub fn split_ids(ids: &[String], chunk_size: usize) -> Vec<Vec<String>> {
    ids.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
}

pub fn can_try_zip(url: &str) -> bool {
    url.len() <= ZIP_URL_LENGTH_LIMIT
}

pub fn try_zip_download(cookie_path: &Path, url: &str, zip_path: &Path) -> io::Result<bool> {
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

pub fn download_first_usable_csv(
    cookie_path: &Path,
    urls: &[String],
    output_path: &Path,
) -> io::Result<CsvResponseCheck> {
    let mut last_error = None::<io::Error>;
    for url in urls {
        download_binary(cookie_path, url, output_path)?;
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

pub fn extract_zip(zip_path: &Path, output_dir: &Path) -> io::Result<()> {
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

pub fn list_csv_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
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

pub fn merge_csv_files(paths: &[PathBuf], output_path: &Path) -> io::Result<()> {
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

pub fn is_zip_file(path: &Path) -> io::Result<bool> {
    let bytes = fs::read(path)?;
    Ok(bytes.len() >= 4 && bytes[0..4] == [0x50, 0x4B, 0x03, 0x04])
}

pub fn inspect_csv_response(path: &Path) -> io::Result<CsvResponseCheck> {
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

pub fn download_binary(cookie_path: &Path, url: &str, output_path: &Path) -> io::Result<()> {
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

pub fn run_command(command: &mut Command, context: &str) -> io::Result<()> {
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
    fn builds_full_history_csv_url() {
        let url = build_gp_history_csv_url_candidates(
            &["44713".into(), "44714".into(), "44715".into()],
            &QueryWindow::FullHistory,
        )[0]
        .clone();
        assert!(url.contains("/norad_cat_id/44713,44714,44715/"));
        assert!(url.ends_with("/format/csv/emptyresult/show"));
    }

    #[test]
    fn builds_bounded_csv_url() {
        let url = build_gp_history_csv_url_candidates(
            &["44713".into()],
            &QueryWindow::Bounded {
                start_date: "2021-07-15".into(),
                end_date: "2024-08-02".into(),
                exclusive_end: "2024-08-03".into(),
            },
        )[0]
        .clone();
        assert!(url.contains("/CREATION_DATE/%3E2021-07-15/"));
        assert!(url.contains("/CREATION_DATE/%3C2024-08-03/"));
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
