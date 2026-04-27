use crate::starlink::csv::{
    build_header_map, csv_escape, date_part, get_field, parse_csv_line, require_column,
};
use crate::starlink::manifest::{group_catalog_rows, load_catalog_rows};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

pub const GROUP_HISTORY_FILENAME: &str = "starlink_gp_history.csv";

#[derive(Clone, Debug)]
pub struct SatelliteWindow {
    pub group_slug: String,
    pub group_name: String,
    pub group_family: String,
    pub launch_date: String,
    pub norad_cat_id: String,
    pub satname: String,
    pub first_creation_date: Option<String>,
    pub last_creation_date: Option<String>,
    pub decay_date: Option<String>,
    pub tle_row_count: usize,
}

#[derive(Clone, Debug)]
pub struct GroupStats {
    pub group_slug: String,
    pub group_name: String,
    pub group_family: String,
    pub launch_date: String,
    pub total_satellites: usize,
    pub decayed_satellites: usize,
    pub active_satellites: usize,
    pub first_decay_date: Option<String>,
    pub last_decay_date: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SnapshotGroupValue {
    pub group_slug: String,
    pub active_satellites: usize,
    pub launched_satellites: usize,
    pub decayed_satellites: usize,
}

#[derive(Clone, Debug)]
pub struct TimelineSnapshot {
    pub date: String,
    pub total_active_satellites: usize,
    pub values: Vec<SnapshotGroupValue>,
}

pub fn load_satellite_windows(
    catalog_path: &Path,
    history_root: &Path,
) -> io::Result<Vec<SatelliteWindow>> {
    let catalog_rows = load_catalog_rows(catalog_path)?;
    let grouped_catalog = group_catalog_rows(&catalog_rows);
    let mut windows = Vec::new();

    for (group_slug, rows) in grouped_catalog {
        let history_path = history_root.join(&group_slug).join(GROUP_HISTORY_FILENAME);
        let history_map = if history_path.exists() {
            load_group_history_summary(&history_path)?
        } else {
            BTreeMap::new()
        };

        for row in rows {
            let summary = history_map.get(&row.norad_cat_id);
            windows.push(SatelliteWindow {
                group_slug: row.group_slug.clone(),
                group_name: row.group_name.clone(),
                group_family: row.group_family.clone(),
                launch_date: row.launch_date.clone(),
                norad_cat_id: row.norad_cat_id.clone(),
                satname: row.satname.clone(),
                first_creation_date: summary.and_then(|item| item.first_creation_date.clone()),
                last_creation_date: summary.and_then(|item| item.last_creation_date.clone()),
                decay_date: summary.and_then(|item| item.decay_date.clone()),
                tle_row_count: summary.map(|item| item.row_count).unwrap_or(0),
            });
        }
    }

    windows.sort_by(|a, b| {
        a.launch_date
            .cmp(&b.launch_date)
            .then_with(|| a.group_slug.cmp(&b.group_slug))
            .then_with(|| compare_norad_ids(&a.norad_cat_id, &b.norad_cat_id))
    });
    Ok(windows)
}

pub fn write_satellite_windows_csv(path: &Path, windows: &[SatelliteWindow]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "group_slug,group_name,group_family,launch_date,norad_cat_id,satname,first_creation_date,last_creation_date,decay_date,tle_row_count"
    )?;
    for window in windows {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{}",
            csv_escape(&window.group_slug),
            csv_escape(&window.group_name),
            csv_escape(&window.group_family),
            csv_escape(&window.launch_date),
            csv_escape(&window.norad_cat_id),
            csv_escape(&window.satname),
            csv_escape(window.first_creation_date.as_deref().unwrap_or("")),
            csv_escape(window.last_creation_date.as_deref().unwrap_or("")),
            csv_escape(window.decay_date.as_deref().unwrap_or("")),
            window.tle_row_count,
        )?;
    }
    writer.flush()
}

pub fn compute_group_stats(windows: &[SatelliteWindow]) -> Vec<GroupStats> {
    let mut grouped = BTreeMap::<String, Vec<&SatelliteWindow>>::new();
    for window in windows {
        grouped
            .entry(window.group_slug.clone())
            .or_insert_with(Vec::new)
            .push(window);
    }

    let mut stats = Vec::new();
    for (group_slug, group_windows) in grouped {
        let first = group_windows[0];
        let mut decay_dates = group_windows
            .iter()
            .filter_map(|window| window.decay_date.clone())
            .collect::<Vec<_>>();
        decay_dates.sort();
        stats.push(GroupStats {
            group_slug,
            group_name: first.group_name.clone(),
            group_family: first.group_family.clone(),
            launch_date: first.launch_date.clone(),
            total_satellites: group_windows.len(),
            decayed_satellites: decay_dates.len(),
            active_satellites: group_windows
                .iter()
                .filter(|window| window.decay_date.is_none())
                .count(),
            first_decay_date: decay_dates.first().cloned(),
            last_decay_date: decay_dates.last().cloned(),
        });
    }

    stats.sort_by(|a, b| {
        a.launch_date
            .cmp(&b.launch_date)
            .then_with(|| a.group_slug.cmp(&b.group_slug))
    });
    stats
}

pub fn write_group_stats_csv(path: &Path, stats: &[GroupStats]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "group_slug,group_name,group_family,launch_date,total_satellites,active_satellites,decayed_satellites,first_decay_date,last_decay_date"
    )?;
    for stat in stats {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{}",
            csv_escape(&stat.group_slug),
            csv_escape(&stat.group_name),
            csv_escape(&stat.group_family),
            csv_escape(&stat.launch_date),
            stat.total_satellites,
            stat.active_satellites,
            stat.decayed_satellites,
            csv_escape(stat.first_decay_date.as_deref().unwrap_or("")),
            csv_escape(stat.last_decay_date.as_deref().unwrap_or("")),
        )?;
    }
    writer.flush()
}

pub fn build_timeline_snapshots(windows: &[SatelliteWindow]) -> Vec<TimelineSnapshot> {
    if windows.is_empty() {
        return Vec::new();
    }

    let mut grouped_windows = BTreeMap::<String, Vec<&SatelliteWindow>>::new();
    for window in windows {
        grouped_windows
            .entry(window.group_slug.clone())
            .or_insert_with(Vec::new)
            .push(window);
    }
    let mut group_order = grouped_windows.keys().cloned().collect::<Vec<_>>();
    group_order.sort();

    let start_date = windows
        .iter()
        .map(|window| window.launch_date.clone())
        .min()
        .unwrap();
    let end_date = windows
        .iter()
        .flat_map(|window| {
            let mut dates = vec![window.launch_date.clone()];
            if let Some(decay_date) = &window.decay_date {
                dates.push(decay_date.clone());
            }
            if let Some(last_creation) = &window.last_creation_date {
                if let Some(date) = date_part(last_creation) {
                    dates.push(date.to_string());
                }
            }
            dates
        })
        .max()
        .unwrap();

    let mut snapshots = Vec::new();
    let mut current_date = start_date;
    loop {
        let mut total_active = 0usize;
        let mut values = Vec::new();

        for group_slug in &group_order {
            let group_windows = &grouped_windows[group_slug];
            let launched_satellites = group_windows
                .iter()
                .filter(|window| window.launch_date <= current_date)
                .count();
            let decayed_satellites = group_windows
                .iter()
                .filter(|window| {
                    window
                        .decay_date
                        .as_ref()
                        .map(|value| value <= &current_date)
                        .unwrap_or(false)
                })
                .count();
            let active_satellites = launched_satellites.saturating_sub(decayed_satellites);
            total_active += active_satellites;

            values.push(SnapshotGroupValue {
                group_slug: group_slug.clone(),
                active_satellites,
                launched_satellites,
                decayed_satellites,
            });
        }

        snapshots.push(TimelineSnapshot {
            date: current_date.clone(),
            total_active_satellites: total_active,
            values,
        });

        if current_date == end_date {
            break;
        }
        current_date = crate::starlink::csv::next_date(&current_date).unwrap();
    }

    snapshots
}

pub fn write_timeline_html(
    path: &Path,
    windows: &[SatelliteWindow],
    stats: &[GroupStats],
    snapshots: &[TimelineSnapshot],
) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    let families = collect_families(stats);
    let decay_events = collect_decay_events(windows);

    writeln!(writer, "<!DOCTYPE html>")?;
    writeln!(writer, "<html lang=\"en\">")?;
    writeln!(writer, "<head>")?;
    writeln!(writer, "<meta charset=\"utf-8\">")?;
    writeln!(
        writer,
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    )?;
    writeln!(writer, "<title>Starlink Group Timelapse</title>")?;
    writer.write_all(
        br#"<style>
html,body{height:100%}
body{margin:0;background:#0d1116;color:#edf3f7;font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
main{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(320px,0.9fr);min-height:100vh}
.hero{padding:20px 24px 14px;border-bottom:1px solid #233141;background:linear-gradient(180deg,#162130,#0d1116)}
.hero h1{margin:0 0 8px;font-size:24px}
.hero p{margin:0;color:#9bb0c1;line-height:1.5}
.left{display:flex;flex-direction:column;min-width:0}
.panel{padding:18px 24px;border-bottom:1px solid #1d2935}
.controls{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;align-items:end}
.controls label{display:block;font-size:12px;color:#87a0b5;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px}
.controls select,.controls button,.controls input{width:100%;box-sizing:border-box;background:#111923;color:#edf3f7;border:1px solid #2a3a4a;border-radius:10px;padding:10px 12px}
.controls button{cursor:pointer}
.metric-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:14px}
.metric{background:#101822;border:1px solid #233141;border-radius:12px;padding:12px 14px}
.metric .label{font-size:11px;color:#8aa2b8;text-transform:uppercase;letter-spacing:.08em}
.metric .value{margin-top:6px;font-size:22px;font-weight:700}
#timelineCanvas{width:100%;height:220px;background:linear-gradient(180deg,#132030,#0f151d);border:1px solid #213242;border-radius:14px}
.bar-list{display:flex;flex-direction:column;gap:10px;margin-top:18px}
.bar-row{display:grid;grid-template-columns:160px minmax(0,1fr) 60px;gap:12px;align-items:center}
.bar-label{font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bar-track{height:18px;background:#0f1721;border-radius:999px;overflow:hidden;border:1px solid #203040}
.bar-fill{height:100%;background:linear-gradient(90deg,#62d89c,#f3c15c)}
.bar-value{text-align:right;font-variant-numeric:tabular-nums;color:#b9cad7}
.right{border-left:1px solid #1d2935;background:#0f151c}
.section-title{margin:0 0 12px;font-size:13px;color:#9bb0c1;text-transform:uppercase;letter-spacing:.08em}
.list{display:flex;flex-direction:column;gap:8px;max-height:34vh;overflow:auto}
.list-item{padding:10px 12px;background:#121b25;border:1px solid #233141;border-radius:12px}
.list-item strong{display:block;margin-bottom:3px}
.subtle{color:#97adbf;font-size:13px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:8px 6px;border-bottom:1px solid #213040;text-align:left}
th{color:#8ba4bb;font-size:11px;text-transform:uppercase;letter-spacing:.08em}
tbody tr:hover{background:#131d28}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#f0c674}
@media (max-width: 1100px){
main{grid-template-columns:1fr}
.right{border-left:0;border-top:1px solid #1d2935}
.controls,.metric-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
.bar-row{grid-template-columns:120px minmax(0,1fr) 48px}
}
</style>"#,
    )?;
    writeln!(writer, "</head>")?;
    writeln!(writer, "<body>")?;
    writeln!(writer, "<main>")?;
    writeln!(writer, "<section class=\"left\">")?;
    writeln!(
        writer,
        "<div class=\"hero\"><h1>Starlink Group Timelapse</h1><p>Animate launch-to-decay activity by group. The slider uses launch dates for activation and <code>DECAY_DATE</code> for reentry/removal ordering. Select a group to inspect its members.</p></div>"
    )?;
    writeln!(writer, "<div class=\"panel\">")?;
    writeln!(writer, "<div class=\"controls\">")?;
    writeln!(
        writer,
        "<div><label for=\"familySelect\">Family</label><select id=\"familySelect\"><option value=\"all\">All families</option>"
    )?;
    for family in &families {
        writeln!(writer, "<option value=\"{}\">{}</option>", family, family)?;
    }
    writeln!(writer, "</select></div>")?;
    writeln!(
        writer,
        "<div><label for=\"groupSelect\">Group</label><select id=\"groupSelect\"><option value=\"all\">All groups</option></select></div>"
    )?;
    writeln!(
        writer,
        "<div><label for=\"dateRange\">Date</label><input id=\"dateRange\" type=\"range\" min=\"0\" max=\"0\" step=\"1\" value=\"0\"></div>"
    )?;
    writeln!(
        writer,
        "<div><label>&nbsp;</label><button id=\"playButton\" type=\"button\">Play</button></div>"
    )?;
    writeln!(writer, "</div>")?;
    writeln!(
        writer,
        "<div class=\"metric-grid\"><div class=\"metric\"><div class=\"label\">Snapshot Date</div><div class=\"value\" id=\"metricDate\">-</div></div><div class=\"metric\"><div class=\"label\">Active Satellites</div><div class=\"value\" id=\"metricActive\">0</div></div><div class=\"metric\"><div class=\"label\">Visible Groups</div><div class=\"value\" id=\"metricGroups\">0</div></div><div class=\"metric\"><div class=\"label\">Decayed Satellites</div><div class=\"value\" id=\"metricDecayed\">0</div></div></div>"
    )?;
    writeln!(writer, "</div>")?;
    writeln!(writer, "<div class=\"panel\"><h2 class=\"section-title\">Constellation Activity</h2><canvas id=\"timelineCanvas\" width=\"960\" height=\"220\"></canvas><div class=\"bar-list\" id=\"barList\"></div></div>")?;
    writeln!(writer, "</section>")?;
    writeln!(writer, "<aside class=\"right\">")?;
    writeln!(writer, "<div class=\"panel\"><h2 class=\"section-title\">Selected Group</h2><div id=\"groupSummary\" class=\"subtle\">Select a group to inspect individual satellites.</div></div>")?;
    writeln!(writer, "<div class=\"panel\"><h2 class=\"section-title\">Group Satellites</h2><table><thead><tr><th>Satellite</th><th>NORAD</th><th>Launch</th><th>Decay</th><th>TLEs</th></tr></thead><tbody id=\"satelliteRows\"></tbody></table></div>")?;
    writeln!(writer, "<div class=\"panel\"><h2 class=\"section-title\">Decay Order</h2><div class=\"list\" id=\"decayList\"></div></div>")?;
    writeln!(writer, "</aside>")?;
    writeln!(writer, "</main>")?;
    writeln!(writer, "<script>")?;
    write_json_data(&mut writer, "GROUP_STATS", stats)?;
    write_json_data(&mut writer, "SATELLITE_WINDOWS", windows)?;
    write_json_data(&mut writer, "SNAPSHOTS", snapshots)?;
    write_json_data(&mut writer, "DECAY_EVENTS", &decay_events)?;
    writer.write_all(
        br#"const familySelect=document.getElementById('familySelect');
const groupSelect=document.getElementById('groupSelect');
const dateRange=document.getElementById('dateRange');
const playButton=document.getElementById('playButton');
const metricDate=document.getElementById('metricDate');
const metricActive=document.getElementById('metricActive');
const metricGroups=document.getElementById('metricGroups');
const metricDecayed=document.getElementById('metricDecayed');
const barList=document.getElementById('barList');
const groupSummary=document.getElementById('groupSummary');
const satelliteRows=document.getElementById('satelliteRows');
const decayList=document.getElementById('decayList');
const timelineCanvas=document.getElementById('timelineCanvas');
const timelineCtx=timelineCanvas.getContext('2d');
let timer=null;

function buildGroupOptions() {
  const family=familySelect.value;
  const groups=GROUP_STATS.filter(group => family === 'all' || group.group_family === family);
  const previous=groupSelect.value;
  groupSelect.innerHTML='<option value="all">All groups</option>';
  for (const group of groups) {
    const option=document.createElement('option');
    option.value=group.group_slug;
    option.textContent=group.group_name;
    groupSelect.appendChild(option);
  }
  if ([...groupSelect.options].some(option => option.value === previous)) {
    groupSelect.value=previous;
  }
}

function selectedGroups() {
  const family=familySelect.value;
  const group=groupSelect.value;
  return GROUP_STATS.filter(item => {
    if (family !== 'all' && item.group_family !== family) return false;
    if (group !== 'all' && item.group_slug !== group) return false;
    return true;
  });
}

function filteredSnapshot() {
  const snapshot=SNAPSHOTS[Number(dateRange.value) || 0];
  const visible=new Map(selectedGroups().map(group => [group.group_slug, group]));
  const values=snapshot.values
    .filter(item => visible.has(item.group_slug))
    .map(item => ({...item, meta: visible.get(item.group_slug)}))
    .sort((a,b) => b.active_satellites - a.active_satellites || a.meta.launch_date.localeCompare(b.meta.launch_date));
  return {snapshot, values};
}

function drawTimeline() {
  const dpr=window.devicePixelRatio||1;
  const rect=timelineCanvas.getBoundingClientRect();
  const width=Math.max(480, Math.round(rect.width));
  const height=220;
  timelineCanvas.width=width*dpr;
  timelineCanvas.height=height*dpr;
  timelineCtx.setTransform(dpr,0,0,dpr,0,0);
  timelineCtx.clearRect(0,0,width,height);
  timelineCtx.fillStyle='#0f151d';
  timelineCtx.fillRect(0,0,width,height);

  const allTotals=SNAPSHOTS.map(item => item.total_active_satellites);
  const maxValue=Math.max(...allTotals,1);
  const padding={left:44,right:20,top:14,bottom:28};
  const plotWidth=width-padding.left-padding.right;
  const plotHeight=height-padding.top-padding.bottom;

  timelineCtx.strokeStyle='rgba(140,176,203,0.18)';
  timelineCtx.lineWidth=1;
  for (let i=0;i<4;i++) {
    const y=padding.top+(plotHeight*i/3);
    timelineCtx.beginPath();
    timelineCtx.moveTo(padding.left,y);
    timelineCtx.lineTo(width-padding.right,y);
    timelineCtx.stroke();
  }

  timelineCtx.strokeStyle='#69d4a8';
  timelineCtx.lineWidth=2;
  timelineCtx.beginPath();
  SNAPSHOTS.forEach((item,index) => {
    const x=padding.left + (plotWidth * index / Math.max(SNAPSHOTS.length - 1, 1));
    const y=padding.top + plotHeight - (plotHeight * item.total_active_satellites / maxValue);
    if (index === 0) timelineCtx.moveTo(x,y); else timelineCtx.lineTo(x,y);
  });
  timelineCtx.stroke();

  const markerIndex=Number(dateRange.value) || 0;
  const markerX=padding.left + (plotWidth * markerIndex / Math.max(SNAPSHOTS.length - 1, 1));
  timelineCtx.strokeStyle='#f3c15c';
  timelineCtx.lineWidth=2;
  timelineCtx.beginPath();
  timelineCtx.moveTo(markerX,padding.top);
  timelineCtx.lineTo(markerX,height-padding.bottom);
  timelineCtx.stroke();

  timelineCtx.fillStyle='#8da5ba';
  timelineCtx.font='12px ui-sans-serif, system-ui, sans-serif';
  timelineCtx.fillText(SNAPSHOTS[0].date, padding.left, height-8);
  timelineCtx.fillText(SNAPSHOTS[SNAPSHOTS.length-1].date, width-padding.right-86, height-8);
  timelineCtx.fillText(String(maxValue), 8, padding.top+8);
}

function renderBars(values) {
  barList.innerHTML='';
  const maxValue=Math.max(...values.map(item => item.active_satellites), 1);
  for (const item of values.slice(0, 18)) {
    const row=document.createElement('div');
    row.className='bar-row';
    row.innerHTML=`<div class="bar-label" title="${item.meta.group_name}">${item.meta.group_name}</div><div class="bar-track"><div class="bar-fill" style="width:${(item.active_satellites/maxValue*100).toFixed(2)}%"></div></div><div class="bar-value">${item.active_satellites}</div>`;
    barList.appendChild(row);
  }
  if (!values.length) {
    barList.innerHTML='<div class="subtle">No groups match the current filter.</div>';
  }
}

function renderGroupDetails(snapshot) {
  const slug=groupSelect.value;
  if (slug === 'all') {
    groupSummary.textContent='Select a single group to inspect its satellites and decay history.';
    satelliteRows.innerHTML='';
    return;
  }

  const group=GROUP_STATS.find(item => item.group_slug === slug);
  const value=snapshot.values.find(item => item.group_slug === slug) || {active_satellites:0, launched_satellites:0, decayed_satellites:0};
  groupSummary.innerHTML=`<strong>${group.group_name}</strong><br><span class="subtle">Family: ${group.group_family}<br>Launch date: ${group.launch_date}<br>Launched by snapshot: ${value.launched_satellites}<br>Decayed by snapshot: ${value.decayed_satellites}<br>Active by snapshot: ${value.active_satellites}</span>`;

  const rows=SATELLITE_WINDOWS
    .filter(window => window.group_slug === slug)
    .sort((a,b) => (a.decay_date || '9999-99-99').localeCompare(b.decay_date || '9999-99-99') || a.satname.localeCompare(b.satname));
  satelliteRows.innerHTML='';
  for (const item of rows) {
    const tr=document.createElement('tr');
    tr.innerHTML=`<td>${item.satname}</td><td>${item.norad_cat_id}</td><td>${item.launch_date}</td><td>${item.decay_date || ''}</td><td>${item.tle_row_count}</td>`;
    satelliteRows.appendChild(tr);
  }
}

function renderDecayList() {
  const family=familySelect.value;
  const slug=groupSelect.value;
  const events=DECAY_EVENTS.filter(item => {
    if (family !== 'all' && item.group_family !== family) return false;
    if (slug !== 'all' && item.group_slug !== slug) return false;
    return true;
  }).slice(0, 18);
  decayList.innerHTML='';
  if (!events.length) {
    decayList.innerHTML='<div class="subtle">No decay events in the current filter.</div>';
    return;
  }
  for (const event of events) {
    const div=document.createElement('div');
    div.className='list-item';
    div.innerHTML=`<strong>${event.decay_date}</strong><span class="subtle">${event.group_name} / ${event.satname} / NORAD ${event.norad_cat_id}</span>`;
    decayList.appendChild(div);
  }
}

function render() {
  const current=filteredSnapshot();
  metricDate.textContent=current.snapshot.date;
  metricActive.textContent=current.values.reduce((sum,item) => sum + item.active_satellites, 0);
  metricGroups.textContent=current.values.length;
  metricDecayed.textContent=current.values.reduce((sum,item) => sum + item.decayed_satellites, 0);
  renderBars(current.values);
  renderGroupDetails(current.snapshot);
  renderDecayList();
  drawTimeline();
}

function togglePlayback() {
  if (timer) {
    clearInterval(timer);
    timer=null;
    playButton.textContent='Play';
    return;
  }
  playButton.textContent='Pause';
  timer=setInterval(() => {
    const next=(Number(dateRange.value) + 1) % SNAPSHOTS.length;
    dateRange.value=String(next);
    render();
  }, 180);
}

function init() {
  buildGroupOptions();
  dateRange.max=String(Math.max(SNAPSHOTS.length - 1, 0));
  dateRange.value=String(Math.max(SNAPSHOTS.length - 1, 0));
  render();
}

familySelect.addEventListener('change', () => { buildGroupOptions(); render(); });
groupSelect.addEventListener('change', render);
dateRange.addEventListener('input', render);
playButton.addEventListener('click', togglePlayback);
window.addEventListener('resize', drawTimeline);
init();
</script>"#,
    )?;
    writeln!(writer, "</body>")?;
    writeln!(writer, "</html>")?;
    writer.flush()
}

#[derive(Clone, Debug, Default)]
struct HistorySummary {
    first_creation_date: Option<String>,
    last_creation_date: Option<String>,
    decay_date: Option<String>,
    row_count: usize,
}

fn load_group_history_summary(path: &Path) -> io::Result<BTreeMap<String, HistorySummary>> {
    let reader = BufReader::new(File::open(path)?);
    let mut lines = reader.lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "group history CSV is empty"))?;
    let header_map = build_header_map(&parse_csv_line(&header));
    let norad_index = require_column(&header_map, "NORAD_CAT_ID")?;
    let creation_index = require_column(&header_map, "CREATION_DATE")?;
    let decay_index = require_column(&header_map, "DECAY_DATE")?;

    let mut summary = BTreeMap::<String, HistorySummary>::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let fields = parse_csv_line(&line);
        let norad_cat_id = get_field(&fields, norad_index, "NORAD_CAT_ID")?;
        let creation_date = get_field(&fields, creation_index, "CREATION_DATE")?;
        let decay_date = get_field(&fields, decay_index, "DECAY_DATE")?;
        let entry = summary.entry(norad_cat_id).or_default();
        entry.row_count += 1;

        if !creation_date.is_empty() {
            entry.first_creation_date =
                min_option(entry.first_creation_date.take(), creation_date.clone());
            entry.last_creation_date = max_option(entry.last_creation_date.take(), creation_date);
        }
        if !decay_date.is_empty() {
            entry.decay_date = min_option(entry.decay_date.take(), decay_date);
        }
    }

    Ok(summary)
}

fn collect_families(stats: &[GroupStats]) -> Vec<String> {
    let mut families = BTreeSet::new();
    for stat in stats {
        families.insert(stat.group_family.clone());
    }
    families.into_iter().collect()
}

fn collect_decay_events(windows: &[SatelliteWindow]) -> Vec<BTreeMap<&'static str, String>> {
    let mut events = windows
        .iter()
        .filter_map(|window| {
            window.decay_date.as_ref().map(|decay_date| {
                let mut map = BTreeMap::new();
                map.insert("decay_date", decay_date.clone());
                map.insert("group_slug", window.group_slug.clone());
                map.insert("group_name", window.group_name.clone());
                map.insert("group_family", window.group_family.clone());
                map.insert("satname", window.satname.clone());
                map.insert("norad_cat_id", window.norad_cat_id.clone());
                map
            })
        })
        .collect::<Vec<_>>();
    events.sort_by(|a, b| {
        a["decay_date"]
            .cmp(&b["decay_date"])
            .then_with(|| a["group_slug"].cmp(&b["group_slug"]))
            .then_with(|| a["norad_cat_id"].cmp(&b["norad_cat_id"]))
    });
    events
}

fn write_json_data<T: JsonWritable>(
    writer: &mut dyn Write,
    name: &str,
    rows: &[T],
) -> io::Result<()> {
    write!(writer, "const {}=[", name)?;
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            write!(writer, ",")?;
        }
        row.write_json(writer)?;
    }
    writeln!(writer, "];")
}

trait JsonWritable {
    fn write_json(&self, writer: &mut dyn Write) -> io::Result<()>;
}

impl JsonWritable for GroupStats {
    fn write_json(&self, writer: &mut dyn Write) -> io::Result<()> {
        write!(
            writer,
            "{{group_slug:\"{}\",group_name:\"{}\",group_family:\"{}\",launch_date:\"{}\",total_satellites:{},active_satellites:{},decayed_satellites:{},first_decay_date:\"{}\",last_decay_date:\"{}\"}}",
            js_escape(&self.group_slug),
            js_escape(&self.group_name),
            js_escape(&self.group_family),
            js_escape(&self.launch_date),
            self.total_satellites,
            self.active_satellites,
            self.decayed_satellites,
            js_escape(self.first_decay_date.as_deref().unwrap_or("")),
            js_escape(self.last_decay_date.as_deref().unwrap_or("")),
        )
    }
}

impl JsonWritable for SatelliteWindow {
    fn write_json(&self, writer: &mut dyn Write) -> io::Result<()> {
        write!(
            writer,
            "{{group_slug:\"{}\",group_name:\"{}\",group_family:\"{}\",launch_date:\"{}\",norad_cat_id:\"{}\",satname:\"{}\",first_creation_date:\"{}\",last_creation_date:\"{}\",decay_date:\"{}\",tle_row_count:{}}}",
            js_escape(&self.group_slug),
            js_escape(&self.group_name),
            js_escape(&self.group_family),
            js_escape(&self.launch_date),
            js_escape(&self.norad_cat_id),
            js_escape(&self.satname),
            js_escape(self.first_creation_date.as_deref().unwrap_or("")),
            js_escape(self.last_creation_date.as_deref().unwrap_or("")),
            js_escape(self.decay_date.as_deref().unwrap_or("")),
            self.tle_row_count,
        )
    }
}

impl JsonWritable for TimelineSnapshot {
    fn write_json(&self, writer: &mut dyn Write) -> io::Result<()> {
        write!(
            writer,
            "{{date:\"{}\",total_active_satellites:{},values:[",
            js_escape(&self.date),
            self.total_active_satellites
        )?;
        for (index, value) in self.values.iter().enumerate() {
            if index > 0 {
                write!(writer, ",")?;
            }
            write!(
                writer,
                "{{group_slug:\"{}\",active_satellites:{},launched_satellites:{},decayed_satellites:{}}}",
                js_escape(&value.group_slug),
                value.active_satellites,
                value.launched_satellites,
                value.decayed_satellites
            )?;
        }
        write!(writer, "]}}")
    }
}

impl JsonWritable for BTreeMap<&'static str, String> {
    fn write_json(&self, writer: &mut dyn Write) -> io::Result<()> {
        write!(
            writer,
            "{{decay_date:\"{}\",group_slug:\"{}\",group_name:\"{}\",group_family:\"{}\",satname:\"{}\",norad_cat_id:\"{}\"}}",
            js_escape(&self["decay_date"]),
            js_escape(&self["group_slug"]),
            js_escape(&self["group_name"]),
            js_escape(&self["group_family"]),
            js_escape(&self["satname"]),
            js_escape(&self["norad_cat_id"]),
        )
    }
}

fn js_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn compare_norad_ids(left: &str, right: &str) -> std::cmp::Ordering {
    match (left.parse::<u64>(), right.parse::<u64>()) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => left.cmp(right),
    }
}

fn min_option(current: Option<String>, next: String) -> Option<String> {
    match current {
        Some(current) => Some(current.min(next)),
        None => Some(next),
    }
}

fn max_option(current: Option<String>, next: String) -> Option<String> {
    match current {
        Some(current) => Some(current.max(next)),
        None => Some(next),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_group_stats() {
        let windows = vec![
            SatelliteWindow {
                group_slug: "g1".into(),
                group_name: "Group 1".into(),
                group_family: "phase1".into(),
                launch_date: "2020-01-01".into(),
                norad_cat_id: "1".into(),
                satname: "STARLINK-1".into(),
                first_creation_date: None,
                last_creation_date: None,
                decay_date: Some("2020-02-01".into()),
                tle_row_count: 10,
            },
            SatelliteWindow {
                group_slug: "g1".into(),
                group_name: "Group 1".into(),
                group_family: "phase1".into(),
                launch_date: "2020-01-01".into(),
                norad_cat_id: "2".into(),
                satname: "STARLINK-2".into(),
                first_creation_date: None,
                last_creation_date: None,
                decay_date: None,
                tle_row_count: 8,
            },
        ];
        let stats = compute_group_stats(&windows);
        assert_eq!(stats[0].total_satellites, 2);
        assert_eq!(stats[0].decayed_satellites, 1);
        assert_eq!(stats[0].active_satellites, 1);
    }

    #[test]
    fn builds_daily_snapshots() {
        let windows = vec![SatelliteWindow {
            group_slug: "g1".into(),
            group_name: "Group 1".into(),
            group_family: "phase1".into(),
            launch_date: "2020-01-01".into(),
            norad_cat_id: "1".into(),
            satname: "STARLINK-1".into(),
            first_creation_date: None,
            last_creation_date: None,
            decay_date: Some("2020-01-03".into()),
            tle_row_count: 5,
        }];
        let snapshots = build_timeline_snapshots(&windows);
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].total_active_satellites, 1);
        assert_eq!(snapshots[2].total_active_satellites, 0);
    }
}
