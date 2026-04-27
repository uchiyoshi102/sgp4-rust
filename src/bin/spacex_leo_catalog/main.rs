use sgp4::spacex::catalog::{write_catalog_rows, CatalogRow};
use sgp4::spacex::public_api::{fetch_launches, fetch_payloads};
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
struct Config {
    output: PathBuf,
    dry_run: bool,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let launches = fetch_launches()?;
    let payloads = fetch_payloads()?;
    let rows = build_rows(&launches, &payloads);

    if config.dry_run {
        eprintln!("Resolved {} SpaceX LEO NORAD rows.", rows.len());
        for row in rows.iter().take(10) {
            println!(
                "{},{},{},{},{}",
                row.norad_cat_id,
                row.launch_date,
                row.launch_name,
                row.payload_name,
                row.orbit_hint
            );
        }
        return Ok(());
    }

    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent)?;
    }
    write_catalog_rows(&config.output, &rows)?;
    eprintln!("Wrote {} rows to {}", rows.len(), config.output.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut output = root.join("data/spacex_leo_catalog.csv");
    let mut dry_run = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                output = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --output")
                })?);
            }
            "--dry-run" => dry_run = true,
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run --bin spacex_leo_catalog -- [--output PATH] [--dry-run]"
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

    Ok(Config { output, dry_run })
}

fn build_rows(
    launches: &std::collections::BTreeMap<String, sgp4::spacex::public_api::LaunchDoc>,
    payloads: &[sgp4::spacex::public_api::PayloadDoc],
) -> Vec<CatalogRow> {
    let mut rows = Vec::new();
    for payload in payloads {
        let Some(launch_id) = payload.launch.as_ref() else {
            continue;
        };
        let Some(launch) = launches.get(launch_id) else {
            continue;
        };
        if payload.norad_ids.is_empty() || !payload_is_leo(payload) {
            continue;
        }
        let customers = payload.customers.join("|");
        let manufacturer = payload.manufacturers.join("|");
        let nationality = payload.nationalities.join("|");
        let orbit_hint = payload.orbit.clone().unwrap_or_default();
        let regime_hint = payload
            .orbit_params
            .as_ref()
            .and_then(|params| params.regime.clone())
            .unwrap_or_default();
        let launch_date = launch
            .date_utc
            .get(0..10)
            .unwrap_or(&launch.date_utc)
            .to_string();

        for norad in &payload.norad_ids {
            rows.push(CatalogRow {
                launch_id: launch.id.clone(),
                launch_name: launch.name.clone(),
                launch_date: launch_date.clone(),
                payload_id: payload.id.clone(),
                payload_name: payload.name.clone(),
                norad_cat_id: norad.to_string(),
                customers: customers.clone(),
                manufacturer: manufacturer.clone(),
                nationality: nationality.clone(),
                orbit_hint: orbit_hint.clone(),
                regime_hint: regime_hint.clone(),
            });
        }
    }
    rows.sort();
    rows
}

fn payload_is_leo(payload: &sgp4::spacex::public_api::PayloadDoc) -> bool {
    if let Some(params) = &payload.orbit_params {
        if let (Some(periapsis_km), Some(apoapsis_km)) = (params.periapsis_km, params.apoapsis_km) {
            if periapsis_km <= 2500.0 && apoapsis_km <= 2500.0 {
                return true;
            }
        }
        if let Some(regime) = &params.regime {
            if regime.eq_ignore_ascii_case("low-earth") {
                return true;
            }
        }
    }

    matches!(
        payload.orbit.as_deref(),
        Some("LEO") | Some("ISS") | Some("PO") | Some("SSO") | Some("VLEO")
    )
}
