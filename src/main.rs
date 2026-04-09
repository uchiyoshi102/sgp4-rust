mod sgp4;

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use sgp4::sgp4::{gstime, jday, PI};
use sgp4::tle::TLE;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

const ARCSEC_TO_RAD: f64 = PI / (180.0 * 3600.0);
const MJD_OFFSET: f64 = 2_400_000.5;
const TAI_MINUS_UTC_SECONDS: f64 = 37.0;
const EARTH_RADIUS_KM: f64 = 6378.137;

#[derive(Clone, Copy, Debug)]
struct EopRecord {
    mjd_utc: f64,
    xp_arcsec: f64,
    yp_arcsec: f64,
    ut1_utc_seconds: f64,
    lod_seconds: f64,
}

#[derive(Clone, Copy, Debug)]
struct EopSample {
    xp_rad: f64,
    yp_rad: f64,
    ut1_utc_seconds: f64,
    lod_seconds: f64,
}

#[derive(Debug)]
struct ViewerPoint {
    name: String,
    object_id: String,
    epoch_utc: String,
    x_km: f64,
    y_km: f64,
    z_km: f64,
    altitude_km: f64,
}

fn main() -> io::Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let tle_path = root.join("data/starlink.TLE");
    let eop_path = root.join("eop/eopc04_20u24.1962-now.csv");
    let output_path = root.join("data/starlink_ecef.csv");
    let viewer_path = root.join("data/starlink_ecef_viewer.html");

    let mut tles = sgp4::tle::read_tles(&tle_path)?;
    let eop_records = load_eop_records(&eop_path)?;
    let render_utc = requested_render_utc_from_args()?.or_else(|| select_common_render_utc(&tles));
    if let Some(timestamp) = render_utc {
        println!(
            "Aligning all satellites to common UTC: {}",
            timestamp.to_rfc3339()
        );
    }
    warn_if_eop_range_mismatch(&tles, &eop_records, render_utc);

    write_ecef_csv(&output_path, &mut tles, &eop_records, render_utc)?;
    write_sphere_viewer_html(&output_path, &viewer_path)?;

    println!("Wrote ECEF CSV: {}", output_path.display());
    println!("Wrote sphere viewer: {}", viewer_path.display());
    Ok(())
}

fn requested_render_utc_from_args() -> io::Result<Option<DateTime<Utc>>> {
    let mut args = env::args().skip(1);
    let mut render_utc = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--utc" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --utc, expected RFC3339 like 2026-04-04T00:00:00Z",
                    )
                })?;
                render_utc = Some(parse_utc_arg(&value)?);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {}. Use --utc 2026-04-04T00:00:00Z", arg),
                ));
            }
        }
    }

    Ok(render_utc)
}

fn parse_utc_arg(value: &str) -> io::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "failed to parse --utc value '{}': {}. Expected RFC3339 like 2026-04-04T00:00:00Z",
                    value, err
                ),
            )
        })
}

fn print_usage() {
    println!("Usage: cargo run -- [--utc 2026-04-04T00:00:00Z]");
}

fn write_ecef_csv(
    path: &Path,
    tles: &mut [TLE],
    eop_records: &[EopRecord],
    render_utc: Option<DateTime<Utc>>,
) -> io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    writeln!(
        writer,
        "name,object_id,epoch_utc,minutes_after_epoch,mjd_utc,jdut1,ut1_utc_s,lod_s,xp_rad,yp_rad,teme_x_km,teme_y_km,teme_z_km,teme_vx_km_s,teme_vy_km_s,teme_vz_km_s,ecef_x_km,ecef_y_km,ecef_z_km,ecef_vx_km_s,ecef_vy_km_s,ecef_vz_km_s"
    )?;

    for tle in tles.iter_mut() {
        for (timestamp, mins_after_epoch) in render_samples(tle, render_utc)? {
            let jd_utc = datetime_to_jd(&timestamp);
            let mjd_utc = jd_utc - MJD_OFFSET;
            let eop = interpolate_eop(eop_records, mjd_utc)?;
            let jdut1 = jd_utc + eop.ut1_utc_seconds / 86400.0;
            let jd_tt = jd_utc + (TAI_MINUS_UTC_SECONDS + 32.184) / 86400.0;
            let ttt = (jd_tt - 2451545.0) / 36525.0;

            let (r_teme, v_teme) = tle.get_rv(mins_after_epoch);
            let (r_ecef, v_ecef) = teme_to_ecef(
                r_teme,
                v_teme,
                ttt,
                jdut1,
                eop.lod_seconds,
                eop.xp_rad,
                eop.yp_rad,
            );

            writeln!(
                writer,
                "{},{},{},{:.8},{:.8},{:.8},{:.7},{:.7},{:.12},{:.12},{:.8},{:.8},{:.8},{:.9},{:.9},{:.9},{:.8},{:.8},{:.8},{:.9},{:.9},{:.9}",
                csv_escape(&tle.name),
                csv_escape(&tle.object_id),
                timestamp.to_rfc3339(),
                mins_after_epoch,
                mjd_utc,
                jdut1,
                eop.ut1_utc_seconds,
                eop.lod_seconds,
                eop.xp_rad,
                eop.yp_rad,
                r_teme[0],
                r_teme[1],
                r_teme[2],
                v_teme[0],
                v_teme[1],
                v_teme[2],
                r_ecef[0],
                r_ecef[1],
                r_ecef[2],
                v_ecef[0],
                v_ecef[1],
                v_ecef[2],
            )?;
        }
    }

    writer.flush()
}

fn write_sphere_viewer_html(csv_path: &Path, html_path: &Path) -> io::Result<()> {
    let points = load_viewer_points(csv_path)?;
    let file = File::create(html_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "<!DOCTYPE html>")?;
    writeln!(writer, "<html lang=\"en\">")?;
    writeln!(writer, "<head>")?;
    writeln!(writer, "<meta charset=\"utf-8\">")?;
    writeln!(
        writer,
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    )?;
    writeln!(writer, "<title>Starlink ECEF Sphere Viewer</title>")?;
    writeln!(
        writer,
        "<style>html,body{{height:100%}}body{{margin:0;background:#08111c;color:#e6eef8;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;display:flex;flex-direction:column;min-height:100vh}}header{{padding:16px 20px;border-bottom:1px solid #203246;background:#0d1724}}h1{{margin:0;font-size:20px}}p{{margin:6px 0 0;color:#9fb3c8;font-size:13px}}#wrap{{display:flex;flex:1;min-height:calc(100vh - 86px)}}canvas{{display:block;flex:1;min-width:0;min-height:640px;background:radial-gradient(circle at top,#17314d,#08111c 60%)}}aside{{width:280px;padding:16px 18px;border-left:1px solid #203246;background:#0b1420}}.label{{color:#8ca4bc;font-size:12px;text-transform:uppercase;letter-spacing:.08em}}.value{{margin:4px 0 16px;font-size:13px;line-height:1.5}}code{{font-family:inherit;color:#f7b955}}@media (max-width: 960px){{#wrap{{flex-direction:column;min-height:auto}}canvas{{min-height:60vh}}aside{{width:auto;border-left:0;border-top:1px solid #203246}}}}</style>"
    )?;
    writeln!(writer, "</head>")?;
    writeln!(writer, "<body>")?;
    writeln!(writer, "<header><h1>Starlink ECEF Sphere Viewer</h1><p>Drag to rotate. Use the mouse wheel to zoom. The sphere is Earth, points come from <code>starlink_ecef.csv</code>, and the XYZ axes are drawn in ECEF.</p></header>")?;
    writeln!(writer, "<div id=\"wrap\">")?;
    writeln!(
        writer,
        "<canvas id=\"scene\" width=\"1280\" height=\"860\"></canvas>"
    )?;
    writeln!(writer, "<aside><div class=\"label\">Dataset</div><div class=\"value\">{} points</div><div class=\"label\">Earth Radius</div><div class=\"value\">{:.3} km</div><div class=\"label\">Axes</div><div class=\"value\">X: red<br>Y: green<br>Z: blue</div><div class=\"label\">Hover</div><div class=\"value\" id=\"hover\">Move the pointer near a point.</div></aside>", points.len(), EARTH_RADIUS_KM)?;
    writeln!(writer, "</div>")?;

    writeln!(writer, "<script>")?;
    writeln!(writer, "const EARTH_RADIUS_KM = {:.6};", EARTH_RADIUS_KM)?;
    writeln!(writer, "const POINTS = [")?;
    for point in &points {
        writeln!(
            writer,
            "{{name:\"{}\",id:\"{}\",epoch:\"{}\",x:{:.8},y:{:.8},z:{:.8},alt:{:.8}}},",
            js_escape(&point.name),
            js_escape(&point.object_id),
            js_escape(&point.epoch_utc),
            point.x_km,
            point.y_km,
            point.z_km,
            point.altitude_km
        )?;
    }
    writeln!(writer, "];")?;
    writer.write_all(
        br#"const canvas=document.getElementById('scene');const ctx=canvas.getContext('2d');const hoverEl=document.getElementById('hover');let yaw=-0.7;let pitch=0.45;let zoom=1.0;let dragging=false;let lastX=0;let lastY=0;let hovered=null;let viewportWidth=1280;let viewportHeight=860;const cameraDistance=22000;const focalLength=900;
function resize(){const dpr=window.devicePixelRatio||1;const rect=canvas.getBoundingClientRect();viewportWidth=Math.max(640,Math.round(rect.width||canvas.parentElement?.clientWidth||1280));viewportHeight=Math.max(480,Math.round(rect.height||Math.max(window.innerHeight-120,480)));canvas.width=Math.round(viewportWidth*dpr);canvas.height=Math.round(viewportHeight*dpr);ctx.setTransform(dpr,0,0,dpr,0,0);draw();}
function rotatePoint(p){const cy=Math.cos(yaw),sy=Math.sin(yaw),cx=Math.cos(pitch),sx=Math.sin(pitch);const x1=cy*p.x+sy*p.z;const z1=-sy*p.x+cy*p.z;const y2=cx*p.y-sx*z1;const z2=sx*p.y+cx*z1;return{x:x1,y:y2,z:z2};}
function projectPoint(p){const rotated=rotatePoint(p);const depth=cameraDistance-rotated.z;if(depth<=1)return null;const scale=(focalLength*zoom)/depth;return{x:rotated.x*scale,y:rotated.y*scale,z:rotated.z,depth:depth,scale:scale,rotated:rotated};}
function isOccluded(rotated){const dx=rotated.x,dy=rotated.y,dz=rotated.z-cameraDistance;const a=dx*dx+dy*dy+dz*dz;const b=2*cameraDistance*dz;const c=cameraDistance*cameraDistance-EARTH_RADIUS_KM*EARTH_RADIUS_KM;const disc=b*b-4*a*c;if(disc<=0)return false;const root=Math.sqrt(disc);const t1=(-b-root)/(2*a);const t2=(-b+root)/(2*a);return(t1>1e-5&&t1<1-1e-5)||(t2>1e-5&&t2<1-1e-5);}
function earthScreenRadius(){const center=projectPoint({x:0,y:0,z:0});const edge=projectPoint({x:EARTH_RADIUS_KM,y:0,z:0});if(!center||!edge)return 0;return Math.hypot(edge.x-center.x,edge.y-center.y);}
function drawEarthBody(){const center=projectPoint({x:0,y:0,z:0});if(!center)return;const radius=earthScreenRadius();const sx=centerX+center.x;const sy=centerY-center.y;const glow=ctx.createRadialGradient(sx-radius*0.22,sy-radius*0.35,radius*0.10,sx,sy,radius);glow.addColorStop(0,'rgba(53,108,164,0.96)');glow.addColorStop(0.7,'rgba(19,49,90,0.98)');glow.addColorStop(1,'rgba(7,18,35,0.99)');ctx.fillStyle=glow;ctx.beginPath();ctx.arc(sx,sy,radius,0,Math.PI*2);ctx.fill();}
function drawAxis(label,color,end){const a=projectPoint({x:0,y:0,z:0});const b=projectPoint(end);if(!a||!b)return;ctx.strokeStyle=color;ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(centerX+a.x,centerY-a.y);ctx.lineTo(centerX+b.x,centerY-b.y);ctx.stroke();ctx.fillStyle=color;ctx.font='14px ui-monospace, monospace';ctx.fillText(label,centerX+b.x+6,centerY-b.y-6);}
function sphereLines(radius){const lines=[];for(let lat=-60;lat<=60;lat+=30){const pts=[];const latR=lat*Math.PI/180;for(let lon=-180;lon<=180;lon+=6){const lonR=lon*Math.PI/180;pts.push({x:radius*Math.cos(latR)*Math.cos(lonR),y:radius*Math.cos(latR)*Math.sin(lonR),z:radius*Math.sin(latR)});}lines.push(pts);}for(let lon=0;lon<180;lon+=30){const pts=[];const lonR=lon*Math.PI/180;for(let lat=-90;lat<=90;lat+=4){const latR=lat*Math.PI/180;pts.push({x:radius*Math.cos(latR)*Math.cos(lonR),y:radius*Math.cos(latR)*Math.sin(lonR),z:radius*Math.sin(latR)});}lines.push(pts);}return lines;}const wire=sphereLines(EARTH_RADIUS_KM);
function drawVisibleWire(){ctx.strokeStyle='rgba(112,170,224,0.40)';ctx.lineWidth=1;for(const line of wire){let started=false;ctx.beginPath();for(const point of line){const projected=projectPoint(point);if(!projected||projected.rotated.z<0){started=false;continue;}const sx=centerX+projected.x;const sy=centerY-projected.y;if(!started){ctx.moveTo(sx,sy);started=true;}else{ctx.lineTo(sx,sy);}}ctx.stroke();}}
function drawSatellite(entry,style){const sx=centerX+entry.projected.x;const sy=centerY-entry.projected.y;const alphaBase=Math.max(style.minAlpha,Math.min(style.maxAlpha,0.24+entry.projected.scale*22));ctx.fillStyle=`rgba(${style.rgb},${alphaBase.toFixed(3)})`;ctx.beginPath();ctx.arc(sx,sy,style.radius,0,Math.PI*2);ctx.fill();return{sx,sy};}
let centerX=0,centerY=0;function draw(){const width=viewportWidth;const height=viewportHeight;centerX=width*0.5;centerY=height*0.5;ctx.clearRect(0,0,width,height);ctx.fillStyle='rgba(8,17,28,0.24)';ctx.fillRect(0,0,width,height);ctx.save();const back=[];const front=[];for(const point of POINTS){const projected=projectPoint(point);if(!projected)continue;const entry={point,projected};if(isOccluded(projected.rotated))back.push(entry);else front.push(entry);}back.sort((a,b)=>a.projected.z-b.projected.z);front.sort((a,b)=>a.projected.z-b.projected.z);for(const entry of back){drawSatellite(entry,{rgb:'115,156,201',radius:1.7,minAlpha:0.08,maxAlpha:0.18});}drawEarthBody();drawVisibleWire();drawAxis('X','#f06272',{x:EARTH_RADIUS_KM*1.35,y:0,z:0});drawAxis('Y','#78d572',{x:0,y:EARTH_RADIUS_KM*1.35,z:0});drawAxis('Z','#62b0ff',{x:0,y:0,z:EARTH_RADIUS_KM*1.35});hovered=null;for(const entry of front){const pos=drawSatellite(entry,{rgb:'255,184,77',radius:2.6,minAlpha:0.35,maxAlpha:0.96});const distance=Math.hypot(pointerX-pos.sx,pointerY-pos.sy);if(distance<8&&(hovered===null||distance<hovered.distance))hovered={entry,distance,sx:pos.sx,sy:pos.sy};}if(hovered){ctx.fillStyle='#ffffff';ctx.beginPath();ctx.arc(hovered.sx,hovered.sy,4.4,0,Math.PI*2);ctx.fill();hoverEl.innerHTML=`<strong>${hovered.entry.point.name}</strong><br>ID: ${hovered.entry.point.id}<br>Epoch: ${hovered.entry.point.epoch}<br>ECEF: (${hovered.entry.point.x.toFixed(2)}, ${hovered.entry.point.y.toFixed(2)}, ${hovered.entry.point.z.toFixed(2)}) km<br>Altitude: ${hovered.entry.point.alt.toFixed(2)} km`;}else{hoverEl.textContent='Move the pointer near a point.';}ctx.restore();}
let pointerX=-1e9,pointerY=-1e9;canvas.addEventListener('pointerdown',event=>{dragging=true;lastX=event.clientX;lastY=event.clientY;canvas.setPointerCapture(event.pointerId);});canvas.addEventListener('pointermove',event=>{const rect=canvas.getBoundingClientRect();pointerX=event.clientX-rect.left;pointerY=event.clientY-rect.top;if(dragging){yaw+=(event.clientX-lastX)*0.008;pitch+=(event.clientY-lastY)*0.008;pitch=Math.max(-1.45,Math.min(1.45,pitch));lastX=event.clientX;lastY=event.clientY;}draw();});canvas.addEventListener('pointerup',()=>{dragging=false;});canvas.addEventListener('pointerleave',()=>{dragging=false;pointerX=-1e9;pointerY=-1e9;draw();});canvas.addEventListener('wheel',event=>{event.preventDefault();zoom*=event.deltaY<0?1.08:0.92;zoom=Math.max(0.35,Math.min(zoom,4.5));draw();},{passive:false});window.addEventListener('resize',resize);resize();
</script>
"#,
    )?;
    writeln!(writer, "</body>")?;
    writeln!(writer, "</html>")?;

    writer.flush()
}

fn warn_if_eop_range_mismatch(
    tles: &[TLE],
    eop_records: &[EopRecord],
    render_utc: Option<DateTime<Utc>>,
) {
    let first_mjd = eop_records[0].mjd_utc;
    let last_mjd = eop_records[eop_records.len() - 1].mjd_utc;

    let mut min_sample_mjd = f64::INFINITY;
    let mut max_sample_mjd = f64::NEG_INFINITY;
    for tle in tles {
        if let Ok(samples) = render_samples(tle, render_utc) {
            for (timestamp, _) in samples {
                let mjd = datetime_to_jd(&timestamp) - MJD_OFFSET;
                min_sample_mjd = min_sample_mjd.min(mjd);
                max_sample_mjd = max_sample_mjd.max(mjd);
            }
        }
    }

    if min_sample_mjd < first_mjd || max_sample_mjd > last_mjd {
        println!(
            "Warning: EOP coverage is MJD {:.2}..{:.2}, but requested epochs are MJD {:.2}..{:.2}; nearest available EOP rows will be used outside that range.",
            first_mjd,
            last_mjd,
            min_sample_mjd,
            max_sample_mjd,
        );
    }
}

fn load_viewer_points(path: &Path) -> io::Result<Vec<ViewerPoint>> {
    let mut lines = read_lines(path)?;
    let header = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty ECEF CSV"))??;
    let headers: Vec<&str> = header.split(',').collect();

    let name_idx = column_index(&headers, "name")?;
    let id_idx = column_index(&headers, "object_id")?;
    let epoch_idx = column_index(&headers, "epoch_utc")?;
    let x_idx = column_index(&headers, "ecef_x_km")?;
    let y_idx = column_index(&headers, "ecef_y_km")?;
    let z_idx = column_index(&headers, "ecef_z_km")?;

    let mut points = Vec::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();
        let x_km = parse_csv_field(&fields, x_idx, "ecef_x_km")?;
        let y_km = parse_csv_field(&fields, y_idx, "ecef_y_km")?;
        let z_km = parse_csv_field(&fields, z_idx, "ecef_z_km")?;
        let radius = (x_km * x_km + y_km * y_km + z_km * z_km).sqrt();

        points.push(ViewerPoint {
            name: parse_csv_text_field(&fields, name_idx, "name")?,
            object_id: parse_csv_text_field(&fields, id_idx, "object_id")?,
            epoch_utc: parse_csv_text_field(&fields, epoch_idx, "epoch_utc")?,
            x_km,
            y_km,
            z_km,
            altitude_km: radius - EARTH_RADIUS_KM,
        });
    }

    Ok(points)
}

fn load_eop_records(path: &Path) -> io::Result<Vec<EopRecord>> {
    let mut lines = read_lines(path)?;
    let header = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty EOP CSV"))??;
    let headers: Vec<&str> = header.split(';').collect();

    let mjd_idx = column_index(&headers, "MJD")?;
    let xp_idx = column_index(&headers, "x_pole")?;
    let yp_idx = column_index(&headers, "y_pole")?;
    let ut1_idx = column_index(&headers, "UT1-UTC")?;
    let lod_idx = column_index(&headers, "LOD")?;

    let mut records = Vec::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(';').collect();
        records.push(EopRecord {
            mjd_utc: parse_csv_field(&fields, mjd_idx, "MJD")?,
            xp_arcsec: parse_csv_field(&fields, xp_idx, "x_pole")?,
            yp_arcsec: parse_csv_field(&fields, yp_idx, "y_pole")?,
            ut1_utc_seconds: parse_csv_field(&fields, ut1_idx, "UT1-UTC")?,
            lod_seconds: parse_csv_field(&fields, lod_idx, "LOD")?,
        });
    }

    if records.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no EOP records found in CSV",
        ));
    }

    Ok(records)
}

fn interpolate_eop(records: &[EopRecord], mjd_utc: f64) -> io::Result<EopSample> {
    if mjd_utc <= records[0].mjd_utc {
        return Ok(EopSample {
            xp_rad: records[0].xp_arcsec * ARCSEC_TO_RAD,
            yp_rad: records[0].yp_arcsec * ARCSEC_TO_RAD,
            ut1_utc_seconds: records[0].ut1_utc_seconds,
            lod_seconds: records[0].lod_seconds,
        });
    }

    if mjd_utc >= records[records.len() - 1].mjd_utc {
        let last = records[records.len() - 1];
        return Ok(EopSample {
            xp_rad: last.xp_arcsec * ARCSEC_TO_RAD,
            yp_rad: last.yp_arcsec * ARCSEC_TO_RAD,
            ut1_utc_seconds: last.ut1_utc_seconds,
            lod_seconds: last.lod_seconds,
        });
    }

    for pair in records.windows(2) {
        let start = pair[0];
        let end = pair[1];
        if mjd_utc >= start.mjd_utc && mjd_utc <= end.mjd_utc {
            let span = end.mjd_utc - start.mjd_utc;
            let fraction = if span.abs() < 1e-12 {
                0.0
            } else {
                (mjd_utc - start.mjd_utc) / span
            };

            return Ok(EopSample {
                xp_rad: lerp(start.xp_arcsec, end.xp_arcsec, fraction) * ARCSEC_TO_RAD,
                yp_rad: lerp(start.yp_arcsec, end.yp_arcsec, fraction) * ARCSEC_TO_RAD,
                ut1_utc_seconds: lerp(start.ut1_utc_seconds, end.ut1_utc_seconds, fraction),
                lod_seconds: lerp(start.lod_seconds, end.lod_seconds, fraction),
            });
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("failed to interpolate EOP for MJD {}", mjd_utc),
    ))
}

fn teme_to_ecef(
    r_teme: [f64; 3],
    v_teme: [f64; 3],
    _ttt: f64,
    jdut1: f64,
    lod_seconds: f64,
    xp_rad: f64,
    yp_rad: f64,
) -> ([f64; 3], [f64; 3]) {
    let gmst = gstime(jdut1);
    let st = [
        [gmst.cos(), -gmst.sin(), 0.0],
        [gmst.sin(), gmst.cos(), 0.0],
        [0.0, 0.0, 1.0],
    ];
    let pm = polar_motion_80(xp_rad, yp_rad);
    let theta_sa = 7.29211514670698e-05 * (1.0 - lod_seconds / 86400.0);
    let omega_earth = [0.0, 0.0, theta_sa];

    let r_pef = transpose_mul(&st, &r_teme);
    let r_ecef = transpose_mul(&pm, &r_pef);
    let v_pef = sub_vec(transpose_mul(&st, &v_teme), cross(&omega_earth, &r_pef));
    let v_ecef = transpose_mul(&pm, &v_pef);

    (r_ecef, v_ecef)
}

fn polar_motion_80(xp_rad: f64, yp_rad: f64) -> [[f64; 3]; 3] {
    let cosxp = xp_rad.cos();
    let sinxp = xp_rad.sin();
    let cosyp = yp_rad.cos();
    let sinyp = yp_rad.sin();

    [
        [cosxp, 0.0, -sinxp],
        [sinxp * sinyp, cosyp, cosxp * sinyp],
        [sinxp * cosyp, -sinyp, cosxp * cosyp],
    ]
}

fn transpose_mul(matrix: &[[f64; 3]; 3], vector: &[f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[1][0] * vector[1] + matrix[2][0] * vector[2],
        matrix[0][1] * vector[0] + matrix[1][1] * vector[1] + matrix[2][1] * vector[2],
        matrix[0][2] * vector[0] + matrix[1][2] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn cross(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn sub_vec(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

fn sample_minutes(tle: &TLE) -> Vec<f64> {
    if tle.step_min.abs() < 1e-12 {
        return vec![0.0];
    }

    let mut minutes = Vec::new();
    let mut current = tle.start_min;
    while in_range(current, tle.start_min, tle.stop_min, tle.step_min) {
        minutes.push(current);
        current += tle.step_min;
    }

    if !minutes.iter().any(|value| value.abs() < 1e-12) {
        minutes.insert(0, 0.0);
    }

    minutes
}

fn select_common_render_utc(tles: &[TLE]) -> Option<DateTime<Utc>> {
    if tles.is_empty() || tles.iter().any(|tle| tle.step_min.abs() >= 1e-12) {
        return None;
    }

    let first_epoch = tles[0].epoch;
    if tles.iter().all(|tle| tle.epoch == first_epoch) {
        return None;
    }

    tles.iter().map(|tle| tle.epoch).max()
}

fn render_samples(
    tle: &TLE,
    render_utc: Option<DateTime<Utc>>,
) -> io::Result<Vec<(DateTime<Utc>, f64)>> {
    if let Some(timestamp) = render_utc {
        let delta = timestamp - tle.epoch;
        let mins_after_epoch = duration_to_minutes(delta)?;
        return Ok(vec![(timestamp, mins_after_epoch)]);
    }

    let mut samples = Vec::new();
    for mins_after_epoch in sample_minutes(tle) {
        samples.push((
            tle.epoch + minutes_to_duration(mins_after_epoch)?,
            mins_after_epoch,
        ));
    }
    Ok(samples)
}

fn in_range(value: f64, start: f64, stop: f64, step: f64) -> bool {
    let eps = 1e-9;
    if step > 0.0 {
        value <= stop + eps && value >= start - eps
    } else if step < 0.0 {
        value >= stop - eps && value <= start + eps
    } else {
        false
    }
}

fn datetime_to_jd(timestamp: &DateTime<Utc>) -> f64 {
    let seconds = timestamp.second() as f64 + timestamp.nanosecond() as f64 * 1.0e-9;
    let (jd, jdfrac) = jday(
        timestamp.year(),
        timestamp.month() as i32,
        timestamp.day() as i32,
        timestamp.hour() as i32,
        timestamp.minute() as i32,
        seconds,
    );
    jd + jdfrac
}

fn minutes_to_duration(minutes: f64) -> io::Result<Duration> {
    let nanos = (minutes * 60.0 * 1.0e9).round();
    if !nanos.is_finite() || nanos < i64::MIN as f64 || nanos > i64::MAX as f64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("minutes out of range: {}", minutes),
        ));
    }

    Ok(Duration::nanoseconds(nanos as i64))
}

fn duration_to_minutes(duration: Duration) -> io::Result<f64> {
    let nanos = duration.num_nanoseconds().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "duration out of range for nanosecond conversion",
        )
    })?;
    Ok(nanos as f64 / 60.0 / 1.0e9)
}

fn column_index(headers: &[&str], name: &str) -> io::Result<usize> {
    headers
        .iter()
        .position(|header| *header == name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing column {}", name),
            )
        })
}

fn parse_csv_field(fields: &[&str], index: usize, name: &str) -> io::Result<f64> {
    let raw = fields.get(index).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("missing field {}", name),
        )
    })?;

    raw.trim().parse::<f64>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse {}: {}", name, err),
        )
    })
}

fn parse_csv_text_field(fields: &[&str], index: usize, name: &str) -> io::Result<String> {
    fields
        .get(index)
        .map(|value| value.trim().to_string())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing field {}", name),
            )
        })
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn lerp(start: f64, end: f64, fraction: f64) -> f64 {
    start + (end - start) * fraction
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn js_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
