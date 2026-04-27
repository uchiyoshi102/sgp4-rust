use sgp4::starlink::csv::path_as_str;
use sgp4::starlink::spacetrack::{
    load_credentials, login_to_space_track, run_curl_text, CookieJar,
};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const CACHE_MAX_AGE: Duration = Duration::from_secs(60 * 60);
const DEFAULT_NAME: &str = "STARLINK";

#[derive(Debug)]
struct Config {
    output: PathBuf,
    identity: Option<String>,
    name: String,
    field: String,
    include_decayed: bool,
    force: bool,
    dry_run: bool,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let url = build_gp_tle_query_url(&config.field, &config.name, config.include_decayed);

    if config.dry_run {
        println!("{url}");
        return Ok(());
    }

    if !config.force && cache_is_fresh(&config.output)? {
        eprintln!("Using cached {}", config.output.display());
        return Ok(());
    }

    let credentials = load_credentials(config.identity)?;
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(&credentials, cookie_jar.path())?;
    let body = fetch_tle_text(cookie_jar.path(), &url)?;
    let normalized = normalize_tle_response(&body)?;

    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = BufWriter::new(File::create(&config.output)?);
    writer.write_all(normalized.as_bytes())?;
    writer.flush()?;

    let tle_count = normalized
        .lines()
        .filter(|line| line.starts_with("1 "))
        .count();
    eprintln!(
        "Wrote {} current Space-Track STARLINK TLEs to {}",
        tle_count,
        config.output.display()
    );
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut output = root.join("data/starlink_space_track_current.tle");
    let mut identity = None;
    let mut name = DEFAULT_NAME.to_string();
    let mut field = "OBJECT_NAME".to_string();
    let mut include_decayed = false;
    let mut force = false;
    let mut dry_run = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => output = PathBuf::from(next_arg(&mut args, "--output")?),
            "--identity" => identity = Some(next_arg(&mut args, "--identity")?),
            "--name" => name = next_arg(&mut args, "--name")?.to_ascii_uppercase(),
            "--field" => field = next_arg(&mut args, "--field")?.to_ascii_uppercase(),
            "--include-decayed" => include_decayed = true,
            "--force" => force = true,
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
        output,
        identity,
        name,
        field,
        include_decayed,
        force,
        dry_run,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_space_track_tles -- [options]");
    println!("Options:");
    println!("  --output PATH          default: data/starlink_space_track_current.tle");
    println!("  --identity EMAIL       also read from SPACE_TRACK_IDENTITY");
    println!("  --name TEXT            default: STARLINK");
    println!(
        "  --field FIELD          default: OBJECT_NAME; use SATNAME for SATCAT-style experiments"
    );
    println!("  --include-decayed      omit decay_date/null-val from the GP query");
    println!("  --force                ignore the one-hour local cache");
    println!("  --dry-run              print the Space-Track URL without logging in");
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing value for {flag}"),
        )
    })
}

fn build_gp_tle_query_url(field: &str, name: &str, include_decayed: bool) -> String {
    let mut parts = vec![
        "https://www.space-track.org/basicspacedata/query/class/gp".to_string(),
        field.to_string(),
        format!("{}~~", encode_path_value(name)),
    ];
    if !include_decayed {
        parts.push("decay_date/null-val".to_string());
        parts.push("epoch/%3Enow-10".to_string());
    }
    parts.push("orderby/norad_cat_id".to_string());
    parts.push("format/tle".to_string());
    parts.push("emptyresult/show".to_string());
    parts.join("/")
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

fn fetch_tle_text(cookie_path: &Path, url: &str) -> io::Result<String> {
    let body = run_curl_text(&[
        "-L",
        "-sS",
        "-b",
        path_as_str(cookie_path)?,
        "-c",
        path_as_str(cookie_path)?,
        url,
    ])?;
    if body.trim_start().starts_with('<') {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Space-Track returned HTML instead of TLE text. Login likely failed or the query was rejected.",
        ));
    }
    Ok(body)
}

fn normalize_tle_response(body: &str) -> io::Result<String> {
    let mut output = String::new();
    let mut pending_name = None::<String>;
    let mut saw_tle = false;

    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "\"\"" {
            continue;
        }
        match trimmed.chars().next() {
            Some('1') => {
                if let Some(name) = pending_name.take() {
                    output.push_str(&name);
                    output.push('\n');
                }
                output.push_str(trimmed);
                output.push('\n');
                saw_tle = true;
            }
            Some('2') => {
                output.push_str(trimmed);
                output.push('\n');
            }
            _ => pending_name = Some(trimmed.to_string()),
        }
    }

    if !saw_tle {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Space-Track response did not contain any TLE line 1 records",
        ));
    }
    Ok(output)
}

fn cache_is_fresh(path: &Path) -> io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let modified = path.metadata()?.modified()?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_else(|_| Duration::from_secs(0));
    Ok(age <= CACHE_MAX_AGE)
}
