use crate::starlink::csv::{
    build_header_map, csv_escape, get_field, parse_csv_line, require_column, validate_date,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupDefinition {
    pub group_slug: String,
    pub group_name: String,
    pub group_family: String,
    pub launch_date: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRow {
    pub group_slug: String,
    pub group_name: String,
    pub group_family: String,
    pub launch_date: String,
    pub norad_cat_id: String,
    pub satname: String,
}

impl Ord for CatalogRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.launch_date
            .cmp(&other.launch_date)
            .then_with(|| self.group_slug.cmp(&other.group_slug))
            .then_with(|| compare_norad_ids(&self.norad_cat_id, &other.norad_cat_id))
            .then_with(|| self.satname.cmp(&other.satname))
    }
}

impl PartialOrd for CatalogRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn load_group_manifest(path: &Path) -> io::Result<Vec<GroupDefinition>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "manifest CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(&header));
    let slug_index = require_column(&header_map, "group_slug")?;
    let name_index = require_column(&header_map, "group_name")?;
    let family_index = require_column(&header_map, "group_family")?;
    let launch_date_index = require_column(&header_map, "launch_date")?;

    let mut rows = Vec::new();
    let mut seen_slugs = BTreeSet::new();
    let mut seen_dates = BTreeSet::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let fields = parse_csv_line(&line);
        let group_slug = get_field(&fields, slug_index, "group_slug")?;
        let group_name = get_field(&fields, name_index, "group_name")?;
        let group_family = get_field(&fields, family_index, "group_family")?;
        let launch_date = get_field(&fields, launch_date_index, "launch_date")?;
        validate_date(&launch_date, "launch_date")?;

        if !seen_slugs.insert(group_slug.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "duplicate group_slug '{}' in {}",
                    group_slug,
                    path.display()
                ),
            ));
        }
        if !seen_dates.insert(launch_date.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "duplicate launch_date '{}' in {}. Each launch date must map to one group.",
                    launch_date,
                    path.display()
                ),
            ));
        }

        rows.push(GroupDefinition {
            group_slug,
            group_name,
            group_family,
            launch_date,
        });
    }

    rows.sort_by(|a, b| {
        a.launch_date
            .cmp(&b.launch_date)
            .then_with(|| a.group_slug.cmp(&b.group_slug))
    });
    if rows.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("manifest {} contained no groups", path.display()),
        ));
    }
    Ok(rows)
}

pub fn launch_date_lookup(groups: &[GroupDefinition]) -> BTreeMap<&str, &GroupDefinition> {
    let mut lookup = BTreeMap::new();
    for group in groups {
        lookup.insert(group.launch_date.as_str(), group);
    }
    lookup
}

pub fn write_catalog_rows(path: &Path, rows: &[CatalogRow]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "group_slug,group_name,group_family,launch_date,norad_cat_id,satname"
    )?;
    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{},{}",
            csv_escape(&row.group_slug),
            csv_escape(&row.group_name),
            csv_escape(&row.group_family),
            csv_escape(&row.launch_date),
            csv_escape(&row.norad_cat_id),
            csv_escape(&row.satname),
        )?;
    }
    writer.flush()
}

pub fn load_catalog_rows(path: &Path) -> io::Result<Vec<CatalogRow>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "catalog CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(&header));
    let group_slug_index = require_column(&header_map, "group_slug")?;
    let group_name_index = require_column(&header_map, "group_name")?;
    let group_family_index = require_column(&header_map, "group_family")?;
    let launch_date_index = require_column(&header_map, "launch_date")?;
    let norad_index = require_column(&header_map, "norad_cat_id")?;
    let satname_index = require_column(&header_map, "satname")?;

    let mut rows = Vec::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let fields = parse_csv_line(&line);
        rows.push(CatalogRow {
            group_slug: get_field(&fields, group_slug_index, "group_slug")?,
            group_name: get_field(&fields, group_name_index, "group_name")?,
            group_family: get_field(&fields, group_family_index, "group_family")?,
            launch_date: get_field(&fields, launch_date_index, "launch_date")?,
            norad_cat_id: get_field(&fields, norad_index, "norad_cat_id")?,
            satname: get_field(&fields, satname_index, "satname")?,
        });
    }

    rows.sort();
    Ok(rows)
}

pub fn group_catalog_rows(rows: &[CatalogRow]) -> BTreeMap<String, Vec<CatalogRow>> {
    let mut grouped = BTreeMap::new();
    for row in rows {
        grouped
            .entry(row.group_slug.clone())
            .or_insert_with(Vec::new)
            .push(row.clone());
    }
    grouped
}

pub fn unique_norad_ids(rows: &[CatalogRow]) -> Vec<String> {
    let mut unique = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        if !row.norad_cat_id.is_empty() && seen.insert(row.norad_cat_id.clone()) {
            unique.push(row.norad_cat_id.clone());
        }
    }
    unique
}

fn compare_norad_ids(left: &str, right: &str) -> std::cmp::Ordering {
    match (left.parse::<u64>(), right.parse::<u64>()) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => left.cmp(right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_catalog_rows_by_slug() {
        let rows = vec![
            CatalogRow {
                group_slug: "g1".into(),
                group_name: "Group 1".into(),
                group_family: "phase1".into(),
                launch_date: "2019-11-11".into(),
                norad_cat_id: "44713".into(),
                satname: "STARLINK-1007".into(),
            },
            CatalogRow {
                group_slug: "g2".into(),
                group_name: "Group 2".into(),
                group_family: "phase1".into(),
                launch_date: "2020-01-07".into(),
                norad_cat_id: "44914".into(),
                satname: "STARLINK-1073".into(),
            },
        ];
        let grouped = group_catalog_rows(&rows);
        assert_eq!(grouped["g1"].len(), 1);
        assert_eq!(grouped["g2"].len(), 1);
    }
}
