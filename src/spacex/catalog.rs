use crate::starlink::csv::{
    build_header_map, csv_escape, get_field, parse_csv_line, require_column,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CatalogRow {
    pub launch_id: String,
    pub launch_name: String,
    pub launch_date: String,
    pub payload_id: String,
    pub payload_name: String,
    pub norad_cat_id: String,
    pub customers: String,
    pub manufacturer: String,
    pub nationality: String,
    pub orbit_hint: String,
    pub regime_hint: String,
}

pub fn load_catalog_rows(path: &Path) -> io::Result<Vec<CatalogRow>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "catalog CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(&header));
    let launch_id_index = require_column(&header_map, "launch_id")?;
    let launch_name_index = require_column(&header_map, "launch_name")?;
    let launch_date_index = require_column(&header_map, "launch_date")?;
    let payload_id_index = require_column(&header_map, "payload_id")?;
    let payload_name_index = require_column(&header_map, "payload_name")?;
    let norad_cat_id_index = require_column(&header_map, "norad_cat_id")?;
    let customers_index = require_column(&header_map, "customers")?;
    let manufacturer_index = require_column(&header_map, "manufacturer")?;
    let nationality_index = require_column(&header_map, "nationality")?;
    let orbit_hint_index = require_column(&header_map, "orbit_hint")?;
    let regime_hint_index = require_column(&header_map, "regime_hint")?;

    let mut rows = Vec::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(&line);
        rows.push(CatalogRow {
            launch_id: get_field(&fields, launch_id_index, "launch_id")?,
            launch_name: get_field(&fields, launch_name_index, "launch_name")?,
            launch_date: get_field(&fields, launch_date_index, "launch_date")?,
            payload_id: get_field(&fields, payload_id_index, "payload_id")?,
            payload_name: get_field(&fields, payload_name_index, "payload_name")?,
            norad_cat_id: get_field(&fields, norad_cat_id_index, "norad_cat_id")?,
            customers: get_field(&fields, customers_index, "customers")?,
            manufacturer: get_field(&fields, manufacturer_index, "manufacturer")?,
            nationality: get_field(&fields, nationality_index, "nationality")?,
            orbit_hint: get_field(&fields, orbit_hint_index, "orbit_hint")?,
            regime_hint: get_field(&fields, regime_hint_index, "regime_hint")?,
        });
    }
    rows.sort();
    Ok(rows)
}

pub fn write_catalog_rows(path: &Path, rows: &[CatalogRow]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "launch_id,launch_name,launch_date,payload_id,payload_name,norad_cat_id,customers,manufacturer,nationality,orbit_hint,regime_hint"
    )?;
    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{},{}",
            csv_escape(&row.launch_id),
            csv_escape(&row.launch_name),
            csv_escape(&row.launch_date),
            csv_escape(&row.payload_id),
            csv_escape(&row.payload_name),
            csv_escape(&row.norad_cat_id),
            csv_escape(&row.customers),
            csv_escape(&row.manufacturer),
            csv_escape(&row.nationality),
            csv_escape(&row.orbit_hint),
            csv_escape(&row.regime_hint),
        )?;
    }
    writer.flush()
}

pub fn unique_norad_ids(rows: &[CatalogRow]) -> Vec<String> {
    let mut ids = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        if !row.norad_cat_id.is_empty() && seen.insert(row.norad_cat_id.clone()) {
            ids.push(row.norad_cat_id.clone());
        }
    }
    ids
}
