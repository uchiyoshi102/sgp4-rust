use sgp4::starlink::timeline::{
    build_timeline_snapshots, compute_group_stats, load_satellite_windows, write_group_stats_csv,
    write_satellite_windows_csv, write_timeline_html,
};
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
struct Config {
    catalog: PathBuf,
    history_root: PathBuf,
    windows_output: PathBuf,
    groups_output: PathBuf,
    html_output: PathBuf,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    if let Some(parent) = config.windows_output.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = config.groups_output.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = config.html_output.parent() {
        fs::create_dir_all(parent)?;
    }

    let windows = load_satellite_windows(&config.catalog, &config.history_root)?;
    let stats = compute_group_stats(&windows);
    let snapshots = build_timeline_snapshots(&windows);
    write_satellite_windows_csv(&config.windows_output, &windows)?;
    write_group_stats_csv(&config.groups_output, &stats)?;
    write_timeline_html(&config.html_output, &windows, &stats, &snapshots)?;

    eprintln!("Wrote {}", config.windows_output.display());
    eprintln!("Wrote {}", config.groups_output.display());
    eprintln!("Wrote {}", config.html_output.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut catalog = root.join("data/starlink_group_catalog.csv");
    let mut history_root = root.join("starlink-groups");
    let mut windows_output = root.join("data/starlink_group_satellite_windows.csv");
    let mut groups_output = root.join("data/starlink_group_stats.csv");
    let mut html_output = root.join("data/starlink_group_timelapse.html");

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--catalog" => {
                catalog = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --catalog")
                })?);
            }
            "--history-root" => {
                history_root = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --history-root",
                    )
                })?);
            }
            "--windows-output" => {
                windows_output = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --windows-output",
                    )
                })?);
            }
            "--groups-output" => {
                groups_output = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --groups-output",
                    )
                })?);
            }
            "--html-output" => {
                html_output = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --html-output",
                    )
                })?);
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
        history_root,
        windows_output,
        groups_output,
        html_output,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_group_timelapse -- [options]");
    println!("Options:");
    println!("  --catalog PATH");
    println!("  --history-root PATH");
    println!("  --windows-output PATH");
    println!("  --groups-output PATH");
    println!("  --html-output PATH");
    println!();
    println!("Defaults:");
    println!("  catalog: data/starlink_group_catalog.csv");
    println!("  history-root: starlink-groups");
    println!("  windows-output: data/starlink_group_satellite_windows.csv");
    println!("  groups-output: data/starlink_group_stats.csv");
    println!("  html-output: data/starlink_group_timelapse.html");
}
