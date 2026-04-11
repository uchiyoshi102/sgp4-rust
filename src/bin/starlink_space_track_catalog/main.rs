use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const SPACE_TRACK_LOGIN_URL: &str = "https://www.space-track.org/ajaxauth/login";
const SPACE_TRACK_SATCAT_QUERY_URL: &str = "https://www.space-track.org/basicspacedata/query/class/satcat/LAUNCH/2019-11-11--2021-05-26/predicates/NORAD_CAT_ID,SATNAME,LAUNCH/format/csv/emptyresult/show";

#[derive(Clone, Copy, Debug)]
struct TargetLaunch {
    wikipedia_no: u8,
    launch_name: &'static str,
    launch_date: &'static str,
}

const TARGET_LAUNCHES: &[TargetLaunch] = &[
    TargetLaunch {
        wikipedia_no: 2,
        launch_name: "Launch 1",
        launch_date: "2019-11-11",
    },
    TargetLaunch {
        wikipedia_no: 3,
        launch_name: "Launch 2",
        launch_date: "2020-01-07",
    },
    TargetLaunch {
        wikipedia_no: 4,
        launch_name: "Launch 3",
        launch_date: "2020-01-29",
    },
    TargetLaunch {
        wikipedia_no: 5,
        launch_name: "Launch 4",
        launch_date: "2020-02-17",
    },
    TargetLaunch {
        wikipedia_no: 6,
        launch_name: "Launch 5",
        launch_date: "2020-03-18",
    },
    TargetLaunch {
        wikipedia_no: 7,
        launch_name: "Launch 6",
        launch_date: "2020-04-22",
    },
    TargetLaunch {
        wikipedia_no: 8,
        launch_name: "Launch 7",
        launch_date: "2020-06-04",
    },
    TargetLaunch {
        wikipedia_no: 9,
        launch_name: "Launch 8",
        launch_date: "2020-06-13",
    },
    TargetLaunch {
        wikipedia_no: 10,
        launch_name: "Launch 9",
        launch_date: "2020-08-07",
    },
    TargetLaunch {
        wikipedia_no: 11,
        launch_name: "Launch 10",
        launch_date: "2020-08-18",
    },
    TargetLaunch {
        wikipedia_no: 12,
        launch_name: "Launch 11",
        launch_date: "2020-09-03",
    },
    TargetLaunch {
        wikipedia_no: 13,
        launch_name: "Launch 12",
        launch_date: "2020-10-06",
    },
    TargetLaunch {
        wikipedia_no: 14,
        launch_name: "Launch 13",
        launch_date: "2020-10-18",
    },
    TargetLaunch {
        wikipedia_no: 15,
        launch_name: "Launch 14",
        launch_date: "2020-10-24",
    },
    TargetLaunch {
        wikipedia_no: 16,
        launch_name: "Launch 15",
        launch_date: "2020-11-25",
    },
    TargetLaunch {
        wikipedia_no: 17,
        launch_name: "Launch 16",
        launch_date: "2021-01-20",
    },
    TargetLaunch {
        wikipedia_no: 18,
        launch_name: "Launch 18",
        launch_date: "2021-02-04",
    },
    TargetLaunch {
        wikipedia_no: 19,
        launch_name: "Launch 19",
        launch_date: "2021-02-16",
    },
    TargetLaunch {
        wikipedia_no: 20,
        launch_name: "Launch 17",
        launch_date: "2021-03-04",
    },
    TargetLaunch {
        wikipedia_no: 21,
        launch_name: "Launch 20",
        launch_date: "2021-03-11",
    },
    TargetLaunch {
        wikipedia_no: 22,
        launch_name: "Launch 21",
        launch_date: "2021-03-14",
    },
    TargetLaunch {
        wikipedia_no: 23,
        launch_name: "Launch 22",
        launch_date: "2021-03-24",
    },
    TargetLaunch {
        wikipedia_no: 24,
        launch_name: "Launch 23",
        launch_date: "2021-04-07",
    },
    TargetLaunch {
        wikipedia_no: 25,
        launch_name: "Launch 24",
        launch_date: "2021-04-29",
    },
    TargetLaunch {
        wikipedia_no: 26,
        launch_name: "Launch 25",
        launch_date: "2021-05-04",
    },
    TargetLaunch {
        wikipedia_no: 27,
        launch_name: "Launch 27",
        launch_date: "2021-05-09",
    },
    TargetLaunch {
        wikipedia_no: 28,
        launch_name: "Launch 26",
        launch_date: "2021-05-15",
    },
    TargetLaunch {
        wikipedia_no: 29,
        launch_name: "Launch 28",
        launch_date: "2021-05-26",
    },
];

#[derive(Debug)]
struct Config {
    output: PathBuf,
    print_launches: bool,
    dry_run: bool,
    identity: Option<String>,
}

#[derive(Debug)]
struct Credentials {
    identity: String,
    password: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SatcatRow {
    wikipedia_no: u8,
    launch_name: String,
    launch_date: String,
    norad_cat_id: String,
    satname: String,
}

impl Ord for SatcatRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.wikipedia_no
            .cmp(&other.wikipedia_no)
            .then_with(|| compare_norad_ids(&self.norad_cat_id, &other.norad_cat_id))
            .then_with(|| self.satname.cmp(&other.satname))
    }
}

impl PartialOrd for SatcatRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn main() -> io::Result<()> {
    let config = parse_args()?;

    if config.print_launches {
        print_target_launches();
        return Ok(());
    }

    if config.dry_run {
        print_target_launches();
        println!();
        println!("Space-Track SATCAT query:");
        println!("{SPACE_TRACK_SATCAT_QUERY_URL}");
        return Ok(());
    }

    let credentials = load_credentials(config.identity)?;
    let rows = fetch_starlink_satcat_rows(&credentials)?;
    write_output(&rows, &config.output)?;

    eprintln!(
        "Fetched {} STARLINK rows across {} target launch dates.",
        rows.len(),
        TARGET_LAUNCHES.len()
    );
    eprintln!("Wrote {}", config.output.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let mut args = env::args().skip(1);
    let mut output = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/starlink_satcat.csv");
    let mut print_launches = false;
    let mut dry_run = false;
    let mut identity = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --output")
                })?;
                output = PathBuf::from(value);
            }
            "--identity" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --identity")
                })?;
                identity = Some(value);
            }
            "--print-launches" => print_launches = true,
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
        output,
        print_launches,
        dry_run,
        identity,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_space_track_catalog -- [options]");
    println!("Options:");
    println!("  --print-launches        Print the fixed Wikipedia No.2-No.29 launch/date table");
    println!("  --dry-run               Print the launch table and Space-Track query URL without logging in");
    println!(
        "  --identity USER         Space-Track username/email, otherwise prompt or env is used"
    );
    println!("  --output PATH           Write CSV output to PATH");
    println!("Default output: data/starlink_satcat.csv");
    println!();
    println!("Credential sources:");
    println!("  1. --identity USER plus SPACE_TRACK_PASSWORD");
    println!("  2. SPACE_TRACK_IDENTITY and SPACE_TRACK_PASSWORD");
    println!("  3. Interactive prompt");
}

fn print_target_launches() {
    println!("wikipedia_no,launch_name,launch_date");
    for launch in TARGET_LAUNCHES {
        println!(
            "{},{},{}",
            launch.wikipedia_no, launch.launch_name, launch.launch_date
        );
    }
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

fn fetch_starlink_satcat_rows(credentials: &Credentials) -> io::Result<Vec<SatcatRow>> {
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(credentials, cookie_jar.path())?;
    let csv_body = fetch_satcat_csv(cookie_jar.path())?;
    parse_and_filter_satcat_csv(&csv_body)
}

fn login_to_space_track(credentials: &Credentials, cookie_path: &Path) -> io::Result<()> {
    run_curl(&[
        "-L",
        "-sS",
        "-c",
        path_as_str(cookie_path)?,
        "--data-urlencode",
        &format!("identity={}", credentials.identity),
        "--data-urlencode",
        &format!("password={}", credentials.password),
        SPACE_TRACK_LOGIN_URL,
    ])
    .map(|_| ())
}

fn fetch_satcat_csv(cookie_path: &Path) -> io::Result<String> {
    run_curl(&[
        "-L",
        "-sS",
        "-b",
        path_as_str(cookie_path)?,
        "-c",
        path_as_str(cookie_path)?,
        SPACE_TRACK_SATCAT_QUERY_URL,
    ])
}

fn run_curl(args: &[&str]) -> io::Result<String> {
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

fn parse_and_filter_satcat_csv(csv_body: &str) -> io::Result<Vec<SatcatRow>> {
    if looks_like_html(csv_body) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Space-Track returned HTML instead of CSV. Login likely failed or the session was rejected.",
        ));
    }

    let mut lines = csv_body.lines();
    let header = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Space-Track CSV was empty"))?;
    let header_map = build_header_map(&parse_csv_line(header));
    let launch_index = require_column(&header_map, "LAUNCH")?;
    let satname_index = require_column(&header_map, "SATNAME")?;
    let norad_index = require_column(&header_map, "NORAD_CAT_ID")?;
    let target_launches = target_launch_lookup();

    let mut rows = BTreeSet::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        let fields = parse_csv_line(line);
        let launch_date = get_field(&fields, launch_index, "LAUNCH")?;
        let satname = get_field(&fields, satname_index, "SATNAME")?;
        let norad_cat_id = get_field(&fields, norad_index, "NORAD_CAT_ID")?;

        let Some(launch) = target_launches.get(launch_date.as_str()) else {
            continue;
        };
        if !satname.to_ascii_uppercase().contains("STARLINK") {
            continue;
        }

        rows.insert(SatcatRow {
            wikipedia_no: launch.wikipedia_no,
            launch_name: launch.launch_name.to_string(),
            launch_date,
            norad_cat_id,
            satname,
        });
    }

    Ok(rows.into_iter().collect())
}

fn target_launch_lookup() -> BTreeMap<&'static str, &'static TargetLaunch> {
    let mut lookup = BTreeMap::new();
    for launch in TARGET_LAUNCHES {
        lookup.insert(launch.launch_date, launch);
    }
    lookup
}

fn build_header_map(fields: &[String]) -> HashMap<String, usize> {
    let mut header_map = HashMap::new();
    for (index, field) in fields.iter().enumerate() {
        header_map.insert(field.clone(), index);
    }
    header_map
}

fn require_column(header_map: &HashMap<String, usize>, name: &str) -> io::Result<usize> {
    header_map.get(name).copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("required CSV column '{name}' was not found"),
        )
    })
}

fn get_field(fields: &[String], index: usize, name: &str) -> io::Result<String> {
    fields.get(index).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CSV row did not include '{name}'"),
        )
    })
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

fn looks_like_html(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("<!DOCTYPE html")
        || trimmed.starts_with("<html")
        || trimmed.starts_with("<HTML")
}

fn write_output(rows: &[SatcatRow], output_path: &Path) -> io::Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer: Box<dyn Write> = Box::new(BufWriter::new(File::create(output_path)?));

    writeln!(
        writer,
        "wikipedia_no,launch_name,launch_date,norad_cat_id,satname"
    )?;
    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{}",
            row.wikipedia_no,
            csv_escape(&row.launch_name),
            row.launch_date,
            row.norad_cat_id,
            csv_escape(&row.satname)
        )?;
    }
    writer.flush()
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn compare_norad_ids(left: &str, right: &str) -> std::cmp::Ordering {
    match (left.parse::<u64>(), right.parse::<u64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
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
    fn target_launches_cover_expected_bounds() {
        assert_eq!(TARGET_LAUNCHES.first().unwrap().launch_date, "2019-11-11");
        assert_eq!(TARGET_LAUNCHES.last().unwrap().launch_date, "2021-05-26");
        assert_eq!(TARGET_LAUNCHES.len(), 28);
    }

    #[test]
    fn parses_csv_quotes() {
        let row = parse_csv_line("44713,\"STARLINK-1007\",2019-11-11");
        assert_eq!(row, vec!["44713", "STARLINK-1007", "2019-11-11"]);
    }

    #[test]
    fn filters_rows_by_launch_date_and_starlink_name() {
        let csv = "\
NORAD_CAT_ID,SATNAME,LAUNCH\n\
44713,STARLINK-1007,2019-11-11\n\
44714,OBJECT-A,2019-11-11\n\
99999,STARLINK POLAR,2021-01-24\n";
        let rows = parse_and_filter_satcat_csv(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].norad_cat_id, "44713");
        assert_eq!(rows[0].launch_name, "Launch 1");
    }

    #[test]
    fn detects_html_login_failure() {
        let error = parse_and_filter_satcat_csv("<!DOCTYPE html><html></html>").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }
}
