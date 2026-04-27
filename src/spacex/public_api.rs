use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::io;
use std::process::Command;

const SPACEX_API_BASE: &str = "https://api.spacexdata.com/v4";

#[derive(Clone, Debug, Deserialize)]
pub struct LaunchDoc {
    pub id: String,
    pub name: String,
    pub date_utc: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PayloadDoc {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub launch: Option<String>,
    #[serde(default)]
    pub norad_ids: Vec<u64>,
    #[serde(default)]
    pub customers: Vec<String>,
    #[serde(default)]
    pub manufacturers: Vec<String>,
    #[serde(default)]
    pub nationalities: Vec<String>,
    #[serde(default)]
    pub orbit: Option<String>,
    #[serde(default)]
    pub orbit_params: Option<OrbitParams>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrbitParams {
    #[serde(default)]
    pub regime: Option<String>,
    #[serde(default)]
    pub periapsis_km: Option<f64>,
    #[serde(default)]
    pub apoapsis_km: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct QueryResponse<T> {
    docs: Vec<T>,
}

pub fn fetch_launches() -> io::Result<BTreeMap<String, LaunchDoc>> {
    let body = r#"{"query":{},"options":{"limit":1000,"sort":{"date_utc":"asc"}}}"#;
    let response: QueryResponse<LaunchDoc> = post_json("/launches/query", body)?;
    Ok(response
        .docs
        .into_iter()
        .map(|launch| (launch.id.clone(), launch))
        .collect())
}

pub fn fetch_payloads() -> io::Result<Vec<PayloadDoc>> {
    let body = r#"{"query":{},"options":{"limit":1000,"sort":{"name":"asc"}}}"#;
    let response: QueryResponse<PayloadDoc> = post_json("/payloads/query", body)?;
    Ok(response.docs)
}

fn post_json<T: DeserializeOwned>(path: &str, body: &str) -> io::Result<T> {
    let url = format!("{SPACEX_API_BASE}{path}");
    let output = Command::new("curl")
        .arg("-L")
        .arg("-sS")
        .arg("-H")
        .arg("content-type: application/json")
        .arg("--data")
        .arg(body)
        .arg(url)
        .output()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to start curl for SpaceX API: {error}"),
            )
        })?;
    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "SpaceX API request failed with status {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse SpaceX API JSON: {error}"),
        )
    })
}
