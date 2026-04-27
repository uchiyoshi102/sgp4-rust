use sgp4::spacex::catalog::{load_catalog_rows, unique_norad_ids};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime};

const CELESTRAK_ACTIVE_TLE_URL: &str =
    "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle";
const CACHE_MAX_AGE: Duration = Duration::from_secs(2 * 60 * 60);

#[derive(Debug)]
struct Config {
    catalog: PathBuf,
    output: PathBuf,
    force: bool,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    if !config.force && cache_is_fresh(&config.output)? {
        eprintln!("Using cached {}", config.output.display());
        return Ok(());
    }

    let ids = unique_norad_ids(&load_catalog_rows(&config.catalog)?)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let body = fetch_tle_body()?;
    let filtered = filter_tle_body(&body, &ids);
    if filtered.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CelesTrak active TLE did not match any SpaceX LEO NORAD IDs",
        ));
    }

    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = BufWriter::new(File::create(&config.output)?);
    writer.write_all(filtered.as_bytes())?;
    writer.flush()?;

    eprintln!("Wrote current TLE set to {}", config.output.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut catalog = root.join("data/spacex_leo_catalog.csv");
    let mut output = root.join("data/spacex_leo_current.tle");
    let mut force = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--catalog" => {
                catalog = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --catalog")
                })?);
            }
            "--output" => {
                output = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --output")
                })?);
            }
            "--force" => force = true,
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run --bin spacex_leo_current_tles -- [--catalog PATH] [--output PATH] [--force]"
                );
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
        output,
        force,
    })
}

fn cache_is_fresh(path: &PathBuf) -> io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let modified = path.metadata()?.modified()?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_else(|_| Duration::from_secs(0));
    Ok(age <= CACHE_MAX_AGE)
}

fn fetch_tle_body() -> io::Result<String> {
    let output = Command::new("curl")
        .arg("-L")
        .arg("-sS")
        .arg(CELESTRAK_ACTIVE_TLE_URL)
        .output()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to start curl for CelesTrak: {error}"),
            )
        })?;
    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "CelesTrak request failed with status {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CelesTrak returned non UTF-8 TLE text: {error}"),
        )
    })
}

fn filter_tle_body(body: &str, ids: &BTreeSet<String>) -> String {
    let mut output = String::new();
    let mut lines = body.lines().filter(|line| !line.trim().is_empty());
    while let Some(first) = lines.next() {
        let Some(second) = lines.next() else { break };
        let Some(third) = lines.next() else { break };
        let line1 = if first.starts_with('1') {
            first
        } else {
            second
        };
        let line2 = if first.starts_with('1') {
            second
        } else {
            third
        };
        let name = if first.starts_with('1') {
            None
        } else {
            Some(first)
        };
        if !line1.starts_with('1') || !line2.starts_with('2') {
            continue;
        }
        let norad = line1.get(2..7).unwrap_or("").trim().to_string();
        if ids.contains(&norad) {
            if let Some(name) = name {
                output.push_str(name.trim());
                output.push('\n');
            }
            output.push_str(line1.trim());
            output.push('\n');
            output.push_str(line2.trim());
            output.push('\n');
        }
    }
    output
}
