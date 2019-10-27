use std::{fs, io::Read, path, str::FromStr};

use log::info;
use plotters::{
    prelude::*,
    style::colors::{BLACK, BLUE, CYAN, GREEN, MAGENTA, RED, WHITE},
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
    fn render(&self) {
        let path_dir = {
            let mut p = path::PathBuf::new();
            p.push(".");
            p.push("plot");
            p
        };
        fs::remove_dir_all(&path_dir).ok();
        fs::create_dir_all(&path_dir).expect("creating the plot dir");

        // initial plot, throughput
        let file = path_dir.join("initial-load-throughput.png");
        let values: Vec<u64> = self
            .title_initial
            .iter()
            .filter_map(|v| Self::get_rate("load", v))
            .collect();
        do_render(
            &file,
            "initial-load-throughput",
            vec!["load"],
            "Seconds",
            "Throughput Kilo-ops / Sec",
            vec![values],
        );
        // initial plot, latency
        let file = path_dir.join("initial-load-latency.png");
        let values: Vec<u64> = self
            .title_initial
            .iter()
            .filter_map(|v| Self::get_lat_98("load", v))
            .collect();
        do_render(
            &file,
            "initial-load-latency 98th percentile",
            vec!["load"],
            "Seconds",
            "Latency in uS",
            vec![values],
        );
    }

    fn get_rate(op_name: &str, v: &toml::Value) -> Option<u64> {
        match v.as_table() {
            Some(table) => match table.get(op_name) {
                Some(table) => {
                    let v = table["latency"]["rate"].as_integer().unwrap();
                    Some((v as u64) / 1000)
                }
                None => None,
            },
            None => None,
        }
    }

    fn get_lat_98(op_name: &str, v: &toml::Value) -> Option<u64> {
        match v.as_table() {
            Some(table) => match table.get(op_name) {
                Some(table) => {
                    let v = table["latency"]["latencies"]["98"].as_integer().unwrap();
                    Some(v as u64)
                }
                None => None,
            },
            None => None,
        }
    }
}

fn do_render(
    file: &path::PathBuf,
    title: &str,
    names: Vec<&str>,
    x_desc: &str,
    y_desc: &str,
    values: Vec<Vec<u64>>,
) {
    info!(target: "plot", "plotting throughput for {} at {:?}", title, file);

    let colors = |name| match name {
        "load" => &BLUE,
        "set" => &GREEN,
        "delete" => &RED,
        "get" => &BLACK,
        "range" => &MAGENTA,
        "reverse" => &CYAN,
        _ => unreachable!(),
    };

    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(file)
        .expect("creating file");
    let root = BitMapBackend::new(&file, (1024, 768)).into_drawing_area();
    root.fill(&WHITE).expect("root file");

    for (i, values) in values.into_iter().enumerate() {
        let (xmin, xmax) = (0_u64, values.len() as u64);
        let (ymin, ymax) = (0_u64, values.iter().max().cloned().unwrap_or(0));
        let ymax = ymax + (ymax / 5);
        let mut cc = ChartBuilder::on(&root)
            .x_label_area_size(40)
            .y_label_area_size(60)
            .margin(10)
            .caption(&title, ("Arial", 30).into_font())
            .build_ranged(xmin..xmax, ymin..ymax)
            .expect("chard builder");

        cc.configure_mesh()
            .line_style_2(&WHITE)
            .label_style(("Arial", 15).into_font())
            .x_desc(x_desc)
            .y_desc(y_desc)
            .axis_desc_style(("Arial", 20).into_font())
            .draw()
            .expect("configure mesh");

        cc.draw_series(LineSeries::new(
            values
                .into_iter()
                .enumerate()
                .map(|(j, value)| (j as u64, value)),
            colors(names[i]),
        ))
        .expect("draw series")
        .label(names[i])
        .legend(|(x, y)| Path::new(vec![(x, y), (x + 20, y)], colors(names[i])));
    }
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
    let re1 = Regex::new(r"\[.*\] (.+) periodic-stats (.+)").unwrap();
    let mut title_initial: Vec<toml::Value> = vec![];
    let mut title_incrmnt: Vec<toml::Value> = vec![];
    let mut title_writers: Vec<toml::Value> = vec![];
    let mut title_readers: Vec<toml::Value> = vec![];
    // TODO: make this 128 element array
    let mut writers: Vec<Vec<toml::Value>> = vec![];
    let mut readers: Vec<Vec<toml::Value>> = vec![];

    let merge_thread = |threads: &mut Vec<Vec<toml::Value>>| -> Option<toml::Value> {
        match threads.iter().any(|t| t.len() == 0) {
            true => None,
            false => {
                let vs: Vec<toml::Value> = threads.iter_mut().map(|t| t.remove(0)).collect();
                let value = vs[1..]
                    .iter()
                    .fold(vs[0].clone(), |a, v| merge_toml(a, v.clone()));
                Some(value)
            }
        }
    };

    for mut file in opt.plot.0 {
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        let line_nos: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(i, l)| if re1.is_match(*l) { Some(i) } else { None })
            .collect();
        let items: Vec<(String, usize, toml::Value)> = line_nos
            .into_iter()
            .map(|line_no| parse_periodic_stats(&lines[line_no]))
            .collect();
        let max_writers = items
            .iter()
            .filter_map(|x| if x.0 == "writer" { Some(x.1) } else { None })
            .max()
            .unwrap_or(0);
        writers.resize(max_writers, vec![]);
        let max_readers = items
            .iter()
            .filter_map(|x| if x.0 == "reader" { Some(x.1) } else { None })
            .max()
            .unwrap_or(0);
        readers.resize(max_readers, vec![]);
        for (title, thread, value) in items.into_iter() {
            match title.as_str() {
                "initial" => title_initial.push(value),
                "incremental" => title_incrmnt.push(value),
                title if &title[..6] == "writer" => {
                    writers[thread].push(value);
                    merge_thread(&mut writers).map(|v| title_writers.push(v));
                }
                title if &title[..6] == "readers" => {
                    readers[thread].push(value);
                    merge_thread(&mut readers).map(|v| title_readers.push(v));
                }
                _ => unreachable!(),
            }
        }
    }
    let data = PlotData {
        title_initial,
        title_incrmnt,
        title_writers,
        title_readers,
    };
    data.render();
    Ok(())
}

fn merge_toml(one: toml::Value, two: toml::Value) -> toml::Value {
    use toml::Value::{Integer, Table};

    match (one, two) {
        (Integer(m), Integer(n)) => toml::Value::Integer(m + n),
        (Table(x), Table(y)) => {
            let mut three = toml::map::Map::new();
            for (name, v) in x.iter() {
                let v = merge_toml(v.clone(), y.get(name).unwrap().clone());
                three.insert(name.clone(), v);
            }
            toml::Value::Table(three)
        }
        _ => unreachable!(),
    }
}

fn parse_periodic_stats(line: &str) -> (String, usize, toml::Value) {
    // TODO: move this into lazy_static
    let re1 = Regex::new(r"\[.*\] (.+) periodic-stats (.+)").unwrap();
    let cap = re1.captures_iter(line).next().unwrap();
    let title_parts: Vec<String> = cap[1].split("-").map(|x| x.to_string()).collect();
    let value: toml::Value = cap[2].parse().expect("failed to parse periodic stats");
    match title_parts[0].as_str() {
        "initial" => (title_parts[0].clone(), 0, value),
        "incremental" => (title_parts[0].clone(), 0, value),
        "reader" | "writer" => (
            title_parts[0].clone(),
            title_parts[1].parse().unwrap(),
            value,
        ),
        _ => unreachable!(),
    }
}
