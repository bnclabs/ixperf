use std::{fs, io::Read, path, str::FromStr};

use log::info;
use plotters::{
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
    fn render(&self) {
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
        let y_axis2 = "Latency in uS";
        let plots = [
            (
                "initial-load-throughput.png",
                "initial-load-throughput",
                ["load"].to_vec(),
                x_axis,
                y_axis1,
                [Self::get_rate("load", &self.title_initial)].to_vec(),
            ),
            (
                "initial-load-latency.png",
                "initial-load-latency 98th percentile",
                ["load"].to_vec(),
                x_axis,
                y_axis2,
                [Self::get_lat_98("load", &self.title_initial)].to_vec(),
            ),
            (
                "incremental-ops-throughput.png",
                "incremental-ops-throughput",
                ["set", "delete", "get"].to_vec(),
                x_axis,
                y_axis1,
                [
                    Self::get_rate("set", &self.title_incrmnt),
                    Self::get_rate("delete", &self.title_incrmnt),
                    Self::get_rate("get", &self.title_incrmnt),
                ]
                .to_vec(),
            ),
            (
                "incremental-ops-latency.png",
                "incremental-ops-latency 98th percentile",
                ["set", "delete", "get"].to_vec(),
                x_axis,
                y_axis2,
                [
                    Self::get_lat_98("set", &self.title_incrmnt),
                    Self::get_lat_98("delete", &self.title_incrmnt),
                    Self::get_lat_98("get", &self.title_incrmnt),
                ]
                .to_vec(),
            ),
            (
                "incremental-range-throughput.png",
                "incremental-range-throughput",
                ["range", "reverse", "range-items", "reverse-items"].to_vec(),
                x_axis,
                y_axis1,
                [
                    Self::get_rate("range", &self.title_incrmnt),
                    Self::get_rate("reverse", &self.title_incrmnt),
                    Self::get_items("range", &self.title_incrmnt),
                    Self::get_items("reverse", &self.title_incrmnt),
                ]
                .to_vec(),
            ),
            (
                "incremental-range-latency.png",
                "incremental-range-latency 98th percentile",
                ["range", "reverse"].to_vec(),
                x_axis,
                y_axis2,
                [
                    Self::get_lat_98("range", &self.title_incrmnt),
                    Self::get_lat_98("reverse", &self.title_incrmnt),
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

    fn get_rate(op_name: &str, vs: &Vec<toml::Value>) -> Vec<u64> {
        let mut out = vec![];
        for v in vs {
            if let Some(table) = v.as_table() {
                if let Some(table) = table.get(op_name) {
                    let v = table["latency"]["rate"].as_integer().unwrap();
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

    fn get_lat_98(op_name: &str, vs: &Vec<toml::Value>) -> Vec<u64> {
        let mut out = vec![];
        for v in vs {
            if let Some(table) = v.as_table() {
                if let Some(t) = table.get(op_name) {
                    let t = t["latency"]["latencies"].as_table().unwrap();
                    let v = if t.contains_key("98") {
                        &t["98"]
                    } else if t.contains_key("99") {
                        &t["99"]
                    } else {
                        &t["100"]
                    };
                    out.push(v.as_integer().unwrap() as u64);
                }
            }
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
    let re1 = Regex::new(r"\[.*\] (.+) periodic-stats").unwrap();
    let mut title_initial: Vec<toml::Value> = vec![];
    let mut title_incrmnt: Vec<toml::Value> = vec![];
    let mut title_writers: Vec<toml::Value> = vec![];
    let mut title_readers: Vec<toml::Value> = vec![];
    // TODO: make this 128 element array
    let mut writers: Vec<Vec<toml::Value>> = vec![];
    let mut readers: Vec<Vec<toml::Value>> = vec![];

    let merge_thread = |ts: &mut Vec<Vec<toml::Value>>| -> Option<toml::Value> {
        match ts.iter().any(|t| t.len() == 0) {
            true => None,
            false => {
                let vs: Vec<toml::Value> = ts
                    // something
                    .iter_mut()
                    .map(|t| t.remove(0))
                    .collect();
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
            .map(|line_no| parse_periodic_stats(&lines[line_no..]))
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

fn parse_periodic_stats(lines: &[&str]) -> (String, usize, toml::Value) {
    // TODO: move this into lazy_static
    let re1 = Regex::new(r"\[.*\] (.+) periodic-stats").unwrap();
    let re2 = Regex::new(r"\[.*\] ").unwrap();
    let cap = re1.captures_iter(&lines[0]).next().unwrap();
    let tp: Vec<String> = cap[1].split("-").map(|x| x.to_string()).collect();
    let stat_lines: Vec<String> = lines[1..]
        .iter()
        .take_while(|l| !re2.is_match(l))
        .map(|l| l.to_string())
        .collect();
    let s = stat_lines.join("\n");
    let value: toml::Value = s.parse().expect("failed to parse stats");
    match tp[0].as_str() {
        "initial" => (tp[0].clone(), 0, value),
        "incremental" => (tp[0].clone(), 0, value),
        "reader" | "writer" => (tp[0].clone(), tp[1].parse().unwrap(), value),
        _ => unreachable!(),
    }
}