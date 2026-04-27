use sgp4::starlink::csv::{build_header_map, get_field, parse_csv_line, require_column};
use sgp4::starlink::manifest::{
    launch_date_lookup, load_group_manifest, write_catalog_rows, CatalogRow,
};
use sgp4::starlink::spacetrack::{
    build_satcat_query_url, fetch_csv_text, load_credentials, login_to_space_track, CookieJar,
};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct Config {
    manifest: PathBuf,
    output: PathBuf,
    split_output_dir: Option<PathBuf>,
    print_groups: bool,
    dry_run: bool,
    identity: Option<String>,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let groups = load_group_manifest(&config.manifest)?;

    if config.print_groups {
        print_groups(&groups);
        return Ok(());
    }

    let query_url = build_query_url(&groups)?;
    if config.dry_run {
        print_groups(&groups);
        println!();
        println!("Space-Track SATCAT query:");
        println!("{query_url}");
        return Ok(());
    }

    let credentials = load_credentials(config.identity)?;
    let cookie_jar = CookieJar::new()?;
    login_to_space_track(&credentials, cookie_jar.path())?;

    let csv_body = fetch_csv_text(cookie_jar.path(), &query_url)?;
    let rows = parse_satcat_csv(&csv_body, &groups)?;
    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent)?;
    }
    write_catalog_rows(&config.output, &rows)?;

    if let Some(split_dir) = &config.split_output_dir {
        write_per_group_catalogs(split_dir, &rows)?;
    }

    eprintln!(
        "Fetched {} STARLINK rows across {} groups.",
        rows.len(),
        groups.len()
    );
    eprintln!("Wrote {}", config.output.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = env::args().skip(1);
    let mut manifest = root.join("data/starlink_group_manifest.csv");
    let mut output = root.join("data/starlink_group_catalog.csv");
    let mut split_output_dir = Some(root.join("starlink-groups"));
    let mut print_groups = false;
    let mut dry_run = false;
    let mut identity = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--manifest" => {
                manifest = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --manifest")
                })?);
            }
            "--output" => {
                output = PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --output")
                })?);
            }
            "--split-output-dir" => {
                split_output_dir = Some(PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --split-output-dir",
                    )
                })?));
            }
            "--no-split-output" => split_output_dir = None,
            "--identity" => {
                identity = Some(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --identity")
                })?);
            }
            "--print-groups" => print_groups = true,
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
        manifest,
        output,
        split_output_dir,
        print_groups,
        dry_run,
        identity,
    })
}

fn print_usage() {
    println!("Usage: cargo run --bin starlink_group_catalog -- [options]");
    println!("Options:");
    println!("  --manifest PATH");
    println!("  --output PATH");
    println!("  --split-output-dir PATH");
    println!("  --no-split-output");
    println!("  --identity USER");
    println!("  --print-groups");
    println!("  --dry-run");
    println!();
    println!("Defaults:");
    println!("  manifest: data/starlink_group_manifest.csv");
    println!("  output: data/starlink_group_catalog.csv");
    println!("  split-output-dir: starlink-groups");
}

fn print_groups(groups: &[sgp4::starlink::manifest::GroupDefinition]) {
    println!("group_slug,group_name,group_family,launch_date");
    for group in groups {
        println!(
            "{},{},{},{}",
            group.group_slug, group.group_name, group.group_family, group.launch_date
        );
    }
}

fn build_query_url(groups: &[sgp4::starlink::manifest::GroupDefinition]) -> io::Result<String> {
    let start_date = groups
        .iter()
        .map(|group| group.launch_date.as_str())
        .min()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "manifest was empty"))?;
    let end_date = groups
        .iter()
        .map(|group| group.launch_date.as_str())
        .max()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "manifest was empty"))?;
    Ok(build_satcat_query_url(start_date, end_date))
}

fn parse_satcat_csv(
    csv_body: &str,
    groups: &[sgp4::starlink::manifest::GroupDefinition],
) -> io::Result<Vec<CatalogRow>> {
    let mut lines = csv_body.lines();
    let header = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Space-Track CSV was empty"))?;
    let header_map = build_header_map(&parse_csv_line(header));
    let launch_index = require_column(&header_map, "LAUNCH")?;
    let satname_index = require_column(&header_map, "SATNAME")?;
    let norad_index = require_column(&header_map, "NORAD_CAT_ID")?;
    let group_lookup = launch_date_lookup(groups);

    let mut rows = BTreeSet::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        let fields = parse_csv_line(line);
        let launch_date = get_field(&fields, launch_index, "LAUNCH")?;
        let satname = get_field(&fields, satname_index, "SATNAME")?;
        let norad_cat_id = get_field(&fields, norad_index, "NORAD_CAT_ID")?;

        let Some(group) = group_lookup.get(launch_date.as_str()) else {
            continue;
        };
        if !satname.to_ascii_uppercase().starts_with("STARLINK") {
            continue;
        }

        rows.insert(CatalogRow {
            group_slug: group.group_slug.clone(),
            group_name: group.group_name.clone(),
            group_family: group.group_family.clone(),
            launch_date,
            norad_cat_id,
            satname,
        });
    }

    Ok(rows.into_iter().collect())
}

fn write_per_group_catalogs(output_dir: &Path, rows: &[CatalogRow]) -> io::Result<()> {
    let grouped = sgp4::starlink::manifest::group_catalog_rows(rows);
    for (group_slug, group_rows) in grouped {
        let dir = output_dir.join(&group_slug);
        fs::create_dir_all(&dir)?;
        let path = dir.join("catalog.csv");
        write_catalog_rows(&path, &group_rows)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_only_manifest_launch_dates() {
        let csv_body = "NORAD_CAT_ID,SATNAME,LAUNCH\n44713,STARLINK-1007,2019-11-11\n99999,NOT-STARLINK,2019-11-11\n";
        let groups = vec![sgp4::starlink::manifest::GroupDefinition {
            group_slug: "phase1-launch-01".into(),
            group_name: "Launch 1".into(),
            group_family: "phase1".into(),
            launch_date: "2019-11-11".into(),
        }];
        let rows = parse_satcat_csv(csv_body, &groups).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].norad_cat_id, "44713");
    }
}
