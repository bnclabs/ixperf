use std::{fs, io::Read, path, str::FromStr, time};

use log::info;
use plotters::{
    prelude::*,
    style::colors::{BLACK, BLUE, CYAN, GREEN, MAGENTA, RED, WHITE, YELLOW},
};
use regex::Regex;

use crate::Opt;

fn latency(
    path: path::PathBuf,
    title: String,
    mut values: Vec<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("plotting latency graph {}", title);

    let root = BitMapBackend::new(&path, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    let (xmin, xmax) = (0_u64, values.len() as u64);
    let (ymin, ymax) = (0_u64, values.iter().max().cloned().unwrap_or(0));
    let mut scatter_ctx = ChartBuilder::on(&root)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .margin(10)
        .caption(&title, ("Arial", 30).into_font())
        .build_ranged(xmin..xmax, ymin..ymax)?;
    scatter_ctx
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .label_style(("Arial", 15).into_font())
        .x_desc("N")
        .y_desc("Millisecond")
        .axis_desc_style(("Arial", 20).into_font())
        .draw()?;
    scatter_ctx.draw_series(
        values
            .iter()
            .enumerate()
            .map(|(i, l)| Circle::new((i as u64, *l), 2, RED.filled())),
    )?;

    values.sort();
    let off = (values.len() as f64 * 0.99) as usize;
    let p99 = time::Duration::from_nanos(values[off] * 1000);
    println!("99th percentile latency: {:?}", p99);
    Ok(())
}

fn throughput(
    path: path::PathBuf,
    title: String,
    sessions: Vec<String>,
    names: Vec<String>,
    valuess: Vec<Vec<u64>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("plotting throughput graph for {}", title);
    let colors = [&RED, &BLUE, &CYAN, &GREEN, &MAGENTA, &BLACK, &YELLOW];

    for (i, mut values) in valuess.into_iter().enumerate() {
        values.insert(0, 0);
        let root = BitMapBackend::new(&path, (1024, 768)).into_drawing_area();
        root.fill(&WHITE)?;

        let (xmin, xmax) = (0_u64, values.len() as u64);
        let (ymin, ymax) = (0_u64, values.iter().max().cloned().unwrap_or(0));
        let ymax = ymax + (ymax / 5);
        let mut cc = ChartBuilder::on(&root)
            .x_label_area_size(40)
            .y_label_area_size(60)
            .margin(10)
            .caption(&title, ("Arial", 30).into_font())
            .build_ranged(xmin..xmax, ymin..ymax)?;

        cc.configure_mesh()
            .line_style_2(&WHITE)
            .label_style(("Arial", 15).into_font())
            .x_desc("Seconds")
            .y_desc("Throughput Ops/sec")
            .axis_desc_style(("Arial", 20).into_font())
            .draw()?;

        let name = if sessions.len() == 1 {
            names[i].clone()
        } else {
            format!("{}:{}", sessions[i], names[i])
        };

        cc.draw_series(LineSeries::new(
            values
                .into_iter()
                .enumerate()
                .map(|(j, value)| (j as u64, value)),
            colors[i],
        ))?
        .label(name)
        .legend(|(x, y)| Path::new(vec![(x, y), (x + 20, y)], colors[i]));
    }

    Ok(())
}

#[derive(Debug)]
pub struct PlotFiles(pub Vec<fs::File>);

impl FromStr for PlotFiles {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut files = vec![];
        for file_name in s.split(",") {
            match fs::OpenOptions::new().read(true).open(file_name) {
                Ok(file) => files.push(file),
                Err(err) => return Err(format!("invalid file: {}", err)),
            }
        }
        Ok(PlotFiles(files))
    }
}

#[derive(Debug, Clone)]
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

pub fn do_plot(mut opt: Opt) -> Result<(), String> {
    let re1 = Regex::new(r"\[.*\] (.+) periodic-stats").unwrap();
    let mut title_initial: Vec<toml::Value> = vec![];
    let mut title_incrmnt: Vec<toml::Value> = vec![];
    // TODO: make this 128 element array
    let mut title_writers: [Option<Vec<toml::Value>>; 32] = Default::default();
    let mut title_readers: [Option<Vec<toml::Value>>; 32] = Default::default();

    for mut file in opt.plot.0 {
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        let starts =
            lines
                .iter()
                .enumerate()
                .filter_map(|(i, l)| if re1.is_match(*l) { Some(i) } else { None });
        let items: Vec<(String, usize, toml::Value)> = starts
            .into_iter()
            .map(|start| parse_periodic_stats(&lines[start..]))
            .collect();
        for (title, thread, value) in items.into_iter() {
            match title.as_str() {
                "initial" => title_initial.push(value),
                "incremental" => title_initial.push(value),
                title if &title[..6] == "writer" => {
                    let item = match title_writers[thread].take() {
                        Some(mut vs) => {
                            vs.push(value);
                            Some(vs)
                        }
                        None => Some(vec![value]),
                    };
                    title_writers[thread] = item;
                }
                title if &title[..6] == "readers" => {
                    let item = match title_writers[thread].take() {
                        Some(mut vs) => {
                            vs.push(value);
                            Some(vs)
                        }
                        None => Some(vec![value]),
                    };
                    title_writers[thread] = item;
                }
                _ => unreachable!(),
            }
        }

        let writers: Vec<Vec<toml::Value>> = title_writers
            .into_iter()
            .filter_map(|x| x.clone())
            .collect();
        let writers = match writers.len() {
            0 => vec![],
            1 => writers[0].clone(),
            _ => {
                let mut outs = writers[0].clone();
                for writer in writers[1..].iter() {
                    for (i, w) in writer.into_iter().enumerate() {
                        if i >= outs.len() {
                            outs.push(w.clone())
                        } else {
                            outs[i] = merge_toml(outs[i].clone(), w.clone())
                        }
                    }
                }
                outs
            }
        };

        let readers: Vec<Vec<toml::Value>> = title_readers
            .into_iter()
            .filter_map(|x| x.clone())
            .collect();
        let readers = match readers.len() {
            0 => vec![],
            1 => readers[0].clone(),
            _ => {
                let mut outs = readers[0].clone();
                for reader in readers[1..].iter() {
                    for (i, r) in reader.into_iter().enumerate() {
                        if i >= outs.len() {
                            outs.push(r.clone())
                        } else {
                            outs[i] = merge_toml(outs[i].clone(), r.clone())
                        }
                    }
                }
                outs
            }
        };
    }
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
    let re2 = Regex::new(r"\[.*\]").unwrap();
    let cap = re1.captures_iter(lines[0]).next().unwrap();
    let title = cap[1].to_string();
    let title_parts: Vec<String> = title.split("-").map(|x| x.to_string()).collect();
    let values: Vec<toml::Value> = lines[1..]
        .iter()
        .take_while(|line| !re1.is_match(line) && !re2.is_match(line))
        .map(|line| {
            let value: toml::Value = line.parse().unwrap();
            value
        })
        .collect();
    let mut mv = toml::map::Map::new();
    for value in values.iter() {
        for (name, v) in value.as_table().unwrap().iter() {
            mv.insert(name.clone(), v.clone());
        }
    }
    match title_parts.len() {
        1 => (title_parts[0].clone(), 0, toml::Value::Table(mv)),
        2 => (
            title_parts[0].clone(),
            title_parts[1].parse().unwrap(),
            toml::Value::Table(mv),
        ),
        _ => unreachable!(),
    }
}
