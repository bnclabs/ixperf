use std::{
    fs,
    io::{self, Read, Seek},
    path,
    str::FromStr,
};

use log::info;
use plotters::{
    chart::SeriesLabelPosition,
    prelude::*,
    style::colors::{BLACK, BLUE, CYAN, GREEN, MAGENTA, RED, WHITE},
    style::RGBColor,
};
use regex::Regex;

use crate::Opt;

struct PlotData {
    title_initial: Vec<toml::Value>,
    title_incrmnt: Vec<toml::Value>,
    title_writers: Vec<toml::Value>,
    title_readers: Vec<toml::Value>,
}

impl PlotData {
    fn render(&self, opt: &Opt) {
        let path_dir = {
            let mut p = path::PathBuf::new();
            p.push(".");
            p.push("plot");
            p
        };
        fs::remove_dir_all(&path_dir).ok();
        fs::create_dir_all(&path_dir).expect("creating the plot dir");

        let x_axis = "Seconds";
        let y_axis1 = "Throughput kilo-ops / Sec";
        let y_axis2 = "Latency in nS";
        let lat = opt.percentile.as_str();
        let plots = [
            (
                "initial-load-throughput.png",
                "initial-load throughput",
                ["load"].to_vec(),
                x_axis,
                y_axis1,
                [Self::get_ops("load", &self.title_initial)].to_vec(),
            ),
            (
                "initial-load-latency.png",
                "initial-load latency 98th percentile",
                ["load"].to_vec(),
                x_axis,
                y_axis2,
                [Self::get_lat_at("load", &self.title_initial, lat)].to_vec(),
            ),
            (
                "incremental-ops-throughput.png",
                "incremental-ops throughput",
                ["set", "delete", "get"].to_vec(),
                x_axis,
                y_axis1,
                [
                    Self::get_ops("set", &self.title_incrmnt),
                    Self::get_ops("delete", &self.title_incrmnt),
                    Self::get_ops("get", &self.title_incrmnt),
                ]
                .to_vec(),
            ),
            (
                "incremental-ops-latency.png",
                "incremental-ops latency 98th percentile",
                ["set", "delete", "get"].to_vec(),
                x_axis,
                y_axis2,
                [
                    Self::get_lat_at("set", &self.title_incrmnt, lat),
                    Self::get_lat_at("delete", &self.title_incrmnt, lat),
                    Self::get_lat_at("get", &self.title_incrmnt, lat),
                ]
                .to_vec(),
            ),
            (
                "incremental-range-throughput.png",
                "incremental-range throughput",
                ["range", "reverse", "range-items", "reverse-items"].to_vec(),
                x_axis,
                y_axis1,
                [
                    Self::get_ops("range", &self.title_incrmnt),
                    Self::get_ops("reverse", &self.title_incrmnt),
                    Self::get_items("range", &self.title_incrmnt),
                    Self::get_items("reverse", &self.title_incrmnt),
                ]
                .to_vec(),
            ),
            (
                "incremental-range-latency.png",
                "incremental-range latency 98th percentile",
                ["range", "reverse"].to_vec(),
                x_axis,
                y_axis2,
                [
                    Self::get_lat_at("range", &self.title_incrmnt, lat),
                    Self::get_lat_at("reverse", &self.title_incrmnt, lat),
                ]
                .to_vec(),
            ),
            (
                "concurrent-rw-ops-throughput.png",
                "concurrent-rw ops throughput",
                ["set", "delete", "get"].to_vec(),
                x_axis,
                y_axis1,
                [
                    Self::get_ops("set", &self.title_writers),
                    Self::get_ops("delete", &self.title_writers),
                    Self::get_ops("get", &self.title_readers),
                ]
                .to_vec(),
            ),
            (
                "concurrent-rw-ops-latency.png",
                "concurrent-rw ops latency 98th percentile",
                ["set", "delete", "get"].to_vec(),
                x_axis,
                y_axis2,
                [
                    Self::get_lat_at("set", &self.title_writers, lat),
                    Self::get_lat_at("delete", &self.title_writers, lat),
                    Self::get_lat_at("get", &self.title_readers, lat),
                ]
                .to_vec(),
            ),
        ];

        // incremental plot, throughput
        for arg in plots.into_iter() {
            let dir = &path_dir.join(arg.0);
            do_render(dir, arg.1, &arg.2, arg.3, arg.4, &arg.5);
        }
    }

    fn get_ops(op_name: &str, vs: &Vec<toml::Value>) -> Vec<u64> {
        let mut out = vec![];
        for v in vs {
            if let Some(table) = v.as_table() {
                if let Some(table) = table.get(op_name) {
                    let v = table["ops"].as_integer().unwrap();
                    out.push((v as u64) / 1000);
                }
            }
        }
        out
    }

    fn get_items(op_name: &str, vs: &Vec<toml::Value>) -> Vec<u64> {
        let mut out = vec![];
        for v in vs {
            if let Some(table) = v.as_table() {
                if let Some(table) = table.get(op_name) {
                    let v = table["items"].as_integer().unwrap();
                    out.push(v as u64);
                }
            }
        }
        out
    }

    fn get_lat_at(op_name: &str, vs: &Vec<toml::Value>, lat: &str) -> Vec<u64> {
        let mut out = vec![];
        for v in vs {
            let table = if let Some(table) = v.as_table() {
                table
            } else {
                continue;
            };
            let t = if let Some(t) = table.get(op_name) {
                t
            } else {
                continue;
            };
            let t = t["latency"]["latencies"].as_table().unwrap();
            out.push(t[lat].as_integer().unwrap() as u64);
        }
        out
    }
}

fn do_render(
    file: &path::PathBuf,
    title: &str,
    names: &Vec<&str>,
    x_desc: &str,
    y_desc: &str,
    valuess: &Vec<Vec<u64>>,
) {
    info!(target: "plot", "plotting throughput for {} at {:?}", title, file);

    let color_for = move |name| match name {
        "load" => BLUE,
        "set" => GREEN,
        "delete" => RED,
        "get" => BLACK,
        "range" => MAGENTA,
        "reverse" => CYAN,
        "range-items" => MAGENTA,
        "reverse-items" => CYAN,
        name => panic!("unreachable {}", name),
    };
    let clrs: Vec<RGBColor> = names.iter().map(|n| color_for(n)).collect();

    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(file)
        .expect("creating file");
    let root = BitMapBackend::new(&file, (1024, 768)).into_drawing_area();
    root.fill(&WHITE).expect("root file");

    let x_maxes: Vec<usize> = valuess.iter().map(|values| values.len()).collect();
    let (xmin, xmax) = (0_u64, x_maxes.into_iter().max().unwrap_or(0) as u64);
    let y_maxes: Vec<u64> = valuess
        .clone()
        .into_iter()
        .map(|v| v.into_iter().max().unwrap_or(0))
        .collect();
    let (ymin, ymax) = (0_u64, y_maxes.into_iter().max().unwrap_or(0));
    let ymax = ymax + (ymax / 5);
    let mut chart = ChartBuilder::on(&root)
        .x_label_area_size(40)
        .y_label_area_size(70)
        .margin(10)
        .caption(&title, ("Arial", 30).into_font())
        .build_ranged(xmin..xmax, ymin..ymax)
        .expect("chard builder");
    chart
        .configure_mesh()
        .line_style_2(&WHITE)
        .label_style(("Arial", 15).into_font())
        .x_desc(x_desc)
        .y_desc(y_desc)
        .axis_desc_style(("Arial", 20).into_font())
        .draw()
        .expect("configure mesh");
    for (i, values) in valuess.into_iter().enumerate() {
        let RGBColor(x, y, z) = clrs[i];
        let clr1 = RGBColor(x, y, z);
        let clr2 = RGBColor(x, y, z);
        chart
            .draw_series(LineSeries::new(
                values
                    .iter()
                    .enumerate()
                    .map(|(j, value)| (j as u64, *value)),
                &clr1,
            ))
            .expect("draw series")
            .label(names[i].to_string())
            .legend(move |(x, y)| Path::new(vec![(x, y), (x + 20, y)], &clr2));
    }
    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .background_style(&RGBColor(255, 255, 255))
        .draw()
        .expect("draw label");
}

#[derive(Debug)]
pub struct PlotFiles(pub Vec<fs::File>);

impl FromStr for PlotFiles {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut files = vec![];
        match s.len() {
            0 => Ok(PlotFiles(files)),
            _ => {
                for file_name in s.split(",") {
                    match fs::OpenOptions::new().read(true).open(file_name) {
                        Ok(file) => files.push(file),
                        Err(err) => return Err(format!("{}", err)),
                    }
                }
                Ok(PlotFiles(files))
            }
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // TODO: clean this up
pub struct PlotTypes(pub Vec<String>);

impl FromStr for PlotTypes {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut types = vec![];
        for typ in s.split(",") {
            match typ {
                "throughput" | "latency" => types.push(typ.to_string()),
                typ => return Err(format!("invalid plot type {}", typ)),
            }
        }
        Ok(PlotTypes(types))
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // TODO: clean this up
pub struct PlotOps(pub Vec<String>);

impl FromStr for PlotOps {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut ops = vec![];
        for op in s.split(",") {
            match op {
                "load" | "set" | "delete" | "get" | "range" | "reverse" => {
                    // something something
                    ops.push(op.to_string())
                }
                op => return Err(format!("invalid plot type {}", op)),
            }
        }
        Ok(PlotOps(ops))
    }
}

pub fn do_plot(opt: Opt) -> Result<(), String> {
    let data = parse_log(&opt)?;
    data.render(&opt);
    Ok(())
}

fn parse_log(opt: &Opt) -> Result<PlotData, String> {
    match &validate_log(&opt) {
        Ok(_) => (),
        Err(_err) if opt.ignore_error => (),
        Err(err) => return Err(err.clone()),
    }

    let re1 = Regex::new(r"\[[0-9]{4}[^\]]*\].*").unwrap();

    let mut title_initial: Vec<toml::Value> = vec![];
    let mut title_incrmnt: Vec<toml::Value> = vec![];
    let mut title_writers: Vec<toml::Value> = vec![];
    let mut title_readers: Vec<toml::Value> = vec![];
    // TODO: make this 128 element array
    let mut writers: Vec<Vec<toml::Value>> = vec![];
    let mut readers: Vec<Vec<toml::Value>> = vec![];

    let merge_thread = |ts: &mut Vec<Vec<toml::Value>>| -> Option<toml::Value> {
        let mut vs: Vec<toml::Value> = ts
            .iter_mut()
            .filter_map(|t| if t.len() > 0 { Some(t.remove(0)) } else { None })
            .collect();
        match vs.len() {
            0 => None,
            1 => Some(vs.remove(0)),
            _ => Some(
                vs[1..]
                    .iter()
                    .fold(vs[0].clone(), |a, v| merge_toml(a, v.clone())),
            ),
        }
    };

    let try_merge_thread = |ts: &mut Vec<Vec<toml::Value>>| -> Option<toml::Value> {
        match ts.iter().any(|t| t.len() == 0) {
            true => None,
            false => merge_thread(ts),
        }
    };

    for mut file in opt.plot.0.iter() {
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();

        let mut log_msgs: Vec<String> = vec![];
        for line in lines {
            if re1.is_match(line) {
                log_msgs.push(line.to_string())
            } else {
                let ln = log_msgs.len() - 1;
                log_msgs[ln].push('\n');
                log_msgs[ln].push_str(line)
            }
        }

        let items: Vec<(String, usize, toml::Value)> = log_msgs
            .into_iter()
            .filter_map(|msg| parse_periodic_stats(msg))
            .collect();
        let max_writers = items
            .iter()
            .filter_map(|x| if x.0 == "writer" { Some(x.1) } else { None })
            .max()
            .unwrap_or(0);
        writers.resize(max_writers + 1, vec![]);
        let max_readers = items
            .iter()
            .filter_map(|x| if x.0 == "reader" { Some(x.1) } else { None })
            .max()
            .unwrap_or(0);
        readers.resize(max_readers + 1, vec![]);
        for (title, thread, value) in items.into_iter() {
            match title.as_str() {
                "initial" => title_initial.push(value),
                "incremental" => title_incrmnt.push(value),
                title if &title[..6] == "writer" => {
                    writers[thread].push(value);
                    try_merge_thread(&mut writers).map(|v| title_writers.push(v));
                }
                title if &title[..6] == "reader" => {
                    readers[thread].push(value);
                    try_merge_thread(&mut readers).map(|v| title_readers.push(v));
                }
                _ => unreachable!(),
            }
        }
        loop {
            match merge_thread(&mut writers) {
                None => break,
                Some(v) => title_writers.push(v),
            }
        }
        loop {
            match merge_thread(&mut readers) {
                None => break,
                Some(v) => title_readers.push(v),
            }
        }
    }
    Ok(PlotData {
        title_initial,
        title_incrmnt,
        title_writers,
        title_readers,
    })
}

fn merge_toml(one: toml::Value, two: toml::Value) -> toml::Value {
    use toml::Value::{Integer, Table};

    match (one, two) {
        (Integer(m), Integer(n)) => toml::Value::Integer(m + n),
        (Table(x), Table(y)) => {
            let mut three = toml::map::Map::new();
            for (name, xv) in x.iter() {
                let v = match y.get(name) {
                    Some(yv) => merge_toml(xv.clone(), yv.clone()),
                    None => xv.clone(),
                };
                three.insert(name.clone(), v);
            }
            toml::Value::Table(three)
        }
        _ => unreachable!(),
    }
}

fn parse_periodic_stats(msg: String) -> Option<(String, usize, toml::Value)> {
    // TODO: move this into lazy_static
    let re1 = Regex::new(r"\[.*\] (.+) periodic-stats.*").unwrap();
    if !re1.is_match(&msg) {
        return None;
    }

    let lines: Vec<&str> = msg.lines().collect();
    let cap = re1.captures_iter(lines[0]).next().unwrap();
    let tp: Vec<String> = cap[1].split("-").map(|x| x.to_string()).collect();
    let sstat = lines[1..].join("\n");
    let value: toml::Value = sstat.parse().expect("failed to parse stats");
    match tp[0].as_str() {
        "initial" => Some((tp[0].clone(), 0, value)),
        "incremental" => Some((tp[0].clone(), 0, value)),
        "reader" | "writer" => {
            let thread = tp[1].parse().unwrap();
            Some((tp[0].clone(), thread, value))
        }
        _ => unreachable!(),
    }
}

fn validate_log(opt: &Opt) -> Result<(), String> {
    let re1 = Regex::new(r"\[.*ERROR.*\] (.+) periodic-stats").unwrap();
    let mut is_err = false;
    for mut file in opt.plot.0.iter() {
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        for line in lines {
            if re1.is_match(line) {
                println!("{}", line);
                is_err = true;
            }
        }
        file.seek(io::SeekFrom::Start(0)).unwrap();
    }
    if is_err {
        Err("log file contains error".to_string())
    } else {
        Ok(())
    }
}
