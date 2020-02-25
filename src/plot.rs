use std::{
    convert::TryInto,
    fs,
    io::{self, Read, Seek},
    path,
    str::FromStr,
};

use chrono::DateTime;
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
    title_initial: Vec<Vec<StatLine>>,
    title_incrmnt: Vec<Vec<StatLine>>,
    title_writers: Vec<Vec<StatLine>>,
    title_readers: Vec<Vec<StatLine>>,
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

        self.render_load_throughput(opt, path_dir.clone());
        self.render_load_latency(opt, path_dir.clone());
        self.render_incr_throughput(opt, path_dir.clone());
        self.render_incr_latency(opt, path_dir.clone());
        self.render_concur_throughput(opt, path_dir.clone());
        self.render_concur_latency(opt, path_dir.clone());
    }

    fn render_load_throughput(&self, _opt: &Opt, path_dir: path::PathBuf) {
        let stats = self.title_initial.clone();
        let x_axis = "Seconds";
        let y_axis = "Throughput kilo-ops / Sec";
        let file = "initial-load-throughput.png";
        let title = "initial-load throughput";
        let names = vec!["load".to_string()];

        let mut ops: Vec<(i64, u64)> = {
            let iter = stats.iter().flatten().filter_map(|s| s.to_ops("load"));
            iter.collect()
        };
        ops.sort_by(|x, y| x.0.cmp(&y.0));

        let y_values = vec![normalize_to_secs(ops)];
        let dir = &path_dir.join(file);
        do_render(dir, title, names, x_axis, y_axis, y_values)
    }

    fn render_load_latency(&self, opt: &Opt, path_dir: path::PathBuf) {
        let p = opt.percentile.as_str();
        let stats = self.title_initial.clone();
        let x_axis = "Seconds";
        let y_axis = "Latency in nS";
        let file = "initial-load-latency.png";
        let title = format!("initial-load latency {} percentile", p);
        let names = vec!["load".to_string()];

        let mut lats: Vec<(i64, u64)> = {
            let iter = stats.iter().flatten();
            iter.filter_map(|s| s.to_latency(opt, "load")).collect()
        };
        lats.sort_by(|x, y| x.0.cmp(&y.0));

        let y_values = vec![normalize_to_secs(lats)];
        let dir = &path_dir.join(file);
        do_render(dir, &title, names, x_axis, y_axis, y_values)
    }

    fn render_incr_throughput(&self, _opt: &Opt, path_dir: path::PathBuf) {
        let stats = self.title_incrmnt.clone();
        let x_axis = "Seconds";
        let y_axis = "Throughput kilo-ops / Sec";
        let file = "initial-incremental-throughput.png";
        let title = "initial-incremental throughput";
        let names = {
            let names = vec!["set", "delete", "get"];
            names.into_iter().map(|s| s.to_string()).collect()
        };

        let mut opss: Vec<Vec<(i64, u64)>> = vec![];
        for op_name in vec!["set", "delete", "get"].into_iter() {
            let iter = stats.iter().flatten().filter_map(|s| s.to_ops(op_name));
            opss.push(iter.collect());
            opss.last_mut().map(|v| v.sort_by(|x, y| x.0.cmp(&y.0)));
        }

        let y_values: Vec<Vec<u64>> = {
            let iter = opss.into_iter().map(|ops| normalize_to_secs(ops));
            iter.collect()
        };
        let dir = &path_dir.join(file);
        do_render(dir, title, names, x_axis, y_axis, y_values)
    }

    fn render_incr_latency(&self, opt: &Opt, path_dir: path::PathBuf) {
        let p = opt.percentile.as_str();
        let stats = self.title_incrmnt.clone();
        let x_axis = "Seconds";
        let y_axis = "Latency in nS";
        let file = "initial-incremental-latency.png";
        let title = format!("initial-load latency {} percentile", p);
        let names = {
            let names = vec!["set", "delete", "get"];
            names.into_iter().map(|s| s.to_string()).collect()
        };

        let mut latss: Vec<Vec<(i64, u64)>> = vec![];
        for op_name in vec!["set", "delete", "get"].into_iter() {
            let lats = {
                let iter = stats.iter().flatten();
                iter.filter_map(|s| s.to_latency(opt, op_name)).collect()
            };
            latss.push(lats);
            latss.last_mut().map(|v| v.sort_by(|x, y| x.0.cmp(&y.0)));
        }

        let y_values: Vec<Vec<u64>> = {
            let iter = latss.into_iter().map(|lats| normalize_to_secs(lats));
            iter.collect()
        };
        let dir = &path_dir.join(file);
        do_render(dir, &title, names, x_axis, y_axis, y_values)
    }

    fn render_concur_throughput(&self, _opt: &Opt, path_dir: path::PathBuf) {
        let x_axis = "Seconds";
        let y_axis = "Throughput kilo-ops / Sec";
        let file = "initial-concurrent-throughput.png";
        let title = "initial-concurrent throughput";

        let (mut names, mut y_values) = {
            let stats = self.title_writers.clone();
            let names: Vec<String> = {
                let names = vec!["set", "delete"];
                names.into_iter().map(|s| s.to_string()).collect()
            };

            let mut opss: Vec<Vec<(i64, u64)>> = vec![];
            for op_name in vec!["set", "delete"].into_iter() {
                let iter = stats.iter().flatten();
                let iter = iter.filter_map(|s| s.to_ops(op_name));
                opss.push(iter.collect());
                opss.last_mut().map(|v| v.sort_by(|x, y| x.0.cmp(&y.0)));
            }
            let y_values: Vec<Vec<u64>> = {
                let iter = opss.into_iter().map(|ops| normalize_to_secs(ops));
                iter.collect()
            };
            (names, y_values)
        };

        let (names_r, y_values_r) = {
            let stats = self.title_readers.clone();
            let names: Vec<String> = {
                let names = vec!["get"];
                names.into_iter().map(|s| s.to_string()).collect()
            };

            let mut opss: Vec<Vec<(i64, u64)>> = vec![];
            for op_name in vec!["get"].into_iter() {
                let iter = stats.iter().flatten();
                let iter = iter.filter_map(|s| s.to_ops(op_name));
                opss.push(iter.collect());
                opss.last_mut().map(|v| v.sort_by(|x, y| x.0.cmp(&y.0)));
            }
            let y_values: Vec<Vec<u64>> = {
                let iter = opss.into_iter().map(|ops| normalize_to_secs(ops));
                iter.collect()
            };
            (names, y_values)
        };

        names.extend_from_slice(&names_r);
        y_values.extend_from_slice(&y_values_r);

        let dir = &path_dir.join(file);
        do_render(dir, title, names, x_axis, y_axis, y_values)
    }

    fn render_concur_latency(&self, opt: &Opt, path_dir: path::PathBuf) {
        let p = opt.percentile.as_str();
        let x_axis = "Seconds";
        let y_axis = "Latency in nS";
        let file = "initial-concurrent-latency.png";
        let title = format!("initial-load latency {} percentile", p);

        let (mut names, mut y_values) = {
            let stats = self.title_writers.clone();
            let names: Vec<String> = {
                let names = vec!["set", "delete"];
                names.into_iter().map(|s| s.to_string()).collect()
            };

            let mut latss: Vec<Vec<(i64, u64)>> = vec![];
            for op_name in vec!["set", "delete"].into_iter() {
                let lats = {
                    let iter = stats.iter().flatten();
                    iter.filter_map(|s| s.to_latency(opt, op_name)).collect()
                };
                latss.push(lats);
                latss.last_mut().map(|v| v.sort_by(|x, y| x.0.cmp(&y.0)));
            }
            let y_values: Vec<Vec<u64>> = {
                let iter = latss.into_iter().map(|lats| normalize_to_secs(lats));
                iter.collect()
            };
            (names, y_values)
        };

        let (names_r, y_values_r) = {
            let stats = self.title_readers.clone();
            let names: Vec<String> = {
                let names = vec!["get"];
                names.into_iter().map(|s| s.to_string()).collect()
            };

            let mut latss: Vec<Vec<(i64, u64)>> = vec![];
            for op_name in vec!["get"].into_iter() {
                let lats = {
                    let iter = stats.iter().flatten();
                    iter.filter_map(|s| s.to_latency(opt, op_name)).collect()
                };
                latss.push(lats);
                latss.last_mut().map(|v| v.sort_by(|x, y| x.0.cmp(&y.0)));
            }
            let y_values: Vec<Vec<u64>> = {
                let iter = latss.into_iter().map(|lats| normalize_to_secs(lats));
                iter.collect()
            };
            (names, y_values)
        };

        names.extend_from_slice(&names_r);
        y_values.extend_from_slice(&y_values_r);

        let dir = &path_dir.join(file);
        do_render(dir, &title, names, x_axis, y_axis, y_values)
    }
}

fn do_render(
    file: &path::PathBuf,
    title: &str,
    names: Vec<String>,
    x_desc: &str,
    y_desc: &str,
    valuess: Vec<Vec<u64>>,
) {
    info!(target: "plot", "plotting throughput for {} at {:?}", title, file);

    let color_for = move |name: &str| match name {
        name if name.contains("load") => BLUE,
        name if name.contains("set") => GREEN,
        name if name.contains("delete") => RED,
        name if name.contains("get") => BLACK,
        name if name.contains("range") => CYAN,
        name if name.contains("reverse") => MAGENTA,
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

    let x_max: u64 = valuess.iter().map(|v| v.len() as u64).max().unwrap_or(0);
    let y_max: u64 = {
        let iter = valuess.clone().into_iter();
        iter.map(|vs| vs.into_iter().max().unwrap_or(0))
            .max()
            .unwrap_or(0)
    };

    let y_max = y_max + (y_max / 5);
    let mut chart = ChartBuilder::on(&root)
        .x_label_area_size(40)
        .y_label_area_size(70)
        .margin(10)
        .caption(&title, ("Arial", 30).into_font())
        .build_ranged(0..x_max, 0..y_max)
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
                values.iter().enumerate().map(|(sec, v)| (sec as u64, *v)),
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
    let lines = log_lines(&opt.plot);

    match &validate_log(&lines) {
        Ok(_) => (),
        Err(_err) if opt.ignore_error => (),
        Err(err) => return Err(err.clone()),
    }

    let re1 = Regex::new(r"\[[0-9]{4}[^\]]*\].*").unwrap();

    let mut log_msgs: Vec<String> = vec![];
    for line in lines {
        if re1.is_match(&line) {
            log_msgs.push(line.to_string())
        } else if log_msgs.len() > 0 {
            let ln = log_msgs.len() - 1;
            log_msgs[ln].push('\n');
            log_msgs[ln].push_str(&line)
        }
    }

    let stat_lines: Vec<StatLine> = log_msgs
        .into_iter()
        .filter_map(|msg| parse_periodic_stats(msg))
        .collect();

    let mut stats: Vec<Vec<Vec<StatLine>>> = vec![];
    for mode in vec!["initial", "incremental", "reader", "writer"].into_iter() {
        let mut stat_mode = vec![];
        for thread in 0.. {
            let s: Vec<StatLine> = stat_lines
                .iter()
                .filter_map(|s| s.filter_mt(mode, thread))
                .collect();
            if s.len() > 0 {
                stat_mode.push(s)
            } else {
                break;
            }
        }
        stats.push(stat_mode);
    }

    Ok(PlotData {
        title_initial: stats.remove(0),
        title_incrmnt: stats.remove(0),
        title_writers: stats.remove(0),
        title_readers: stats.remove(0),
    })
}

fn parse_periodic_stats(msg: String) -> Option<StatLine> {
    let re1 = Regex::new(r"\[([^ ]+) .*\] (.+) periodic-stats.*").unwrap();
    if !re1.is_match(&msg) {
        return None;
    }

    let lines: Vec<&str> = msg.lines().collect();
    let cap = re1.captures_iter(lines[0]).next().unwrap();

    let tp: Vec<String> = cap[2].split("-").map(|x| x.to_string()).collect();
    let a = tp.first().as_ref().map(|s| s.as_str());
    let z = tp.last().as_ref().map(|s| s.as_str());
    let (mode, thread): (&'static str, usize) = match (a, z) {
        (Some("initial"), Some("initial")) => ("initial", 0),
        (Some("initial"), Some(thread)) => ("initial", thread.parse().unwrap()),
        (Some("incremental"), Some("incremental")) => ("incremental", 0),
        (Some("reader"), Some("reader")) => ("reader", 0),
        (Some("reader"), Some(thread)) => ("reader", thread.parse().unwrap()),
        (Some("writer"), Some("writer")) => ("writer", 0),
        (Some("writer"), Some(thread)) => ("writer", thread.parse().unwrap()),
        _ => unreachable!(),
    };

    let millis: i64 = {
        let dt = DateTime::parse_from_rfc3339(&cap[1]).unwrap();
        dt.timestamp_millis()
    };

    let value: toml::Value = {
        let sstat = lines[1..].join("\n");
        sstat.parse().expect("failed to parse stats")
    };

    Some(StatLine {
        mode,
        thread,
        millis,
        value,
    })
}

fn validate_log(lines: &[String]) -> Result<(), String> {
    let re1 = Regex::new(r"\[.*ERROR.*\]").unwrap();
    let mut is_err = false;

    for line in lines {
        if re1.is_match(line) {
            println!("{}", line);
            is_err = true;
        }
    }

    if is_err {
        Err("log file contains error".to_string())
    } else {
        Ok(())
    }
}

fn log_lines(files: &PlotFiles) -> Vec<String> {
    let mut lines = vec![];
    for mut file in files.0.iter() {
        let mut buf = vec![];
        let s: Vec<&str> = {
            file.read_to_end(&mut buf).unwrap();
            std::str::from_utf8(&buf).unwrap().lines().collect()
        };
        let ls: Vec<String> = s.into_iter().map(|l| l.to_string()).collect();
        lines.extend_from_slice(&ls);
        file.seek(io::SeekFrom::Start(0)).unwrap();
    }

    lines
}

#[derive(Clone)]
struct StatLine {
    mode: &'static str,
    thread: usize,
    millis: i64,
    value: toml::Value,
}

impl StatLine {
    fn filter_mt(&self, mode: &'static str, n: usize) -> Option<StatLine> {
        if self.mode == mode && self.thread == n {
            Some(self.clone())
        } else {
            None
        }
    }

    fn to_ops(&self, op_name: &str) -> Option<(i64, u64)> {
        match self.value.as_table() {
            Some(table) => match table.get(op_name) {
                Some(table) => {
                    let ops = table["ops"].as_integer().unwrap();
                    Some((self.millis, ops.try_into().unwrap()))
                }
                None => None,
            },
            None => None,
        }
    }

    fn to_latency(&self, opt: &Opt, op_name: &str) -> Option<(i64, u64)> {
        let p = opt.percentile.as_str();
        let lat = match self.value.as_table() {
            Some(table) => match table.get(op_name) {
                Some(table) => match table["latency"]["latencies"].get(p) {
                    Some(value) => value.as_integer().unwrap(),
                    None => {
                        let value = &table["latency"]["latencies"];
                        let table = value.as_table().unwrap();
                        let sum: i64 = {
                            let iter = table.iter();
                            let iter = iter.map(|(_, v)| v.as_integer().unwrap());
                            iter.collect::<Vec<i64>>().iter().sum()
                        };
                        sum / (table.len() as i64)
                    }
                },
                None => unreachable!(),
            },
            None => unreachable!(),
        };

        Some((self.millis, lat.try_into().unwrap()))
    }
}

fn normalize_to_secs(mut items: Vec<(i64, u64)>) -> Vec<u64> {
    if items.len() == 0 {
        vec![]
    } else if items.len() == 1 {
        vec![items.remove(0).1]
    } else {
        let items = {
            let (_, v1) = items[0].clone();
            let mut acc = vec![(0, v1)];
            let iter = items[..].to_vec().into_iter();
            for ((t1, _), (t2, v)) in iter.zip(items[1..].to_vec().into_iter()) {
                acc.push(((t2 - t1), v))
            }
            acc
        };
        let mut items = {
            let mut acc = vec![(0, 0)];
            for (t1, v1) in items.into_iter() {
                match acc.remove(acc.len() - 1) {
                    (t0, v0) if t0 + t1 <= 1000 => acc.push((t0 + t1, v0 + v1)),
                    (t0, v0) => {
                        acc.push((t0, v0));
                        acc.push((t1, v1));
                    }
                }
            }
            acc
        };

        let mut acc = vec![items.remove(0).1];
        let (mut rem_t, mut rem_v) = (0, 0);
        for (mut t, mut v) in items.into_iter() {
            assert!(t > 1000, "{}", t);
            assert!(t < 1000 * 100, "{}", t);

            t += rem_t;
            v += rem_v;
            while t >= 1000 {
                let r = 1000.0 / (t as f64);
                acc.push(((v as f64) * r) as u64);
                t = t - 1000;
                v = v - acc[acc.len() - 1];
            }
            rem_t = t;
            rem_v = v;
        }

        acc
    }
}
