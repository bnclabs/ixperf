use std::{path, time};

use log::info;
use plotters::{
    prelude::*,
    style::colors::{BLACK, BLUE, CYAN, GREEN, MAGENTA, RED, WHITE, YELLOW},
};
use regex::Regex;

use crate::Opt;

struct PlotFiles(Vec<fs::File>);

impl FromStr for PlotFiles {
    type Err = String;

    fn from_str(s: &str) -> PlotFiles {
        let files = vec![];
        for file_name in s.split(",") {
            match OpenOptions::new().read(true).open(file_name) {
                Ok(file) => files.push(file),
                Err(err) => return format!("invalid plot file {}", file_name),
            }
        }
        PlotFiles(files)
    }
}

struct PlotTypes(Vec<String>);

impl FromStr for PlotTypes {
    type Err = String;

    fn from_str(s: &str) -> PlotTypes {
        let types = vec![];
        for typ in s.split(",") {
            match typ {
                "throughput" | "latency" => types.push(typ.to_string()),
                Err(err) => return format!("invalid plot type {}", typ),
            }
        }
        PlotTypes(types)
    }
}

struct PlotOps(Vec<String>);

impl FromStr for PlotOps {
    type Err = String;

    fn from_str(s: &str) -> PlotOps {
        let ops = vec![];
        for op in s.split(",") {
            match op {
                "load" | "set" | "delete" | "get" | "range" | "reverse" => {
                    // something something
                    ops.push(op.to_string())
                }
                Err(err) => return format!("invalid plot type {}", op),
            }
        }
        PlotOps(ops)
    }
}

pub fn do_plot(opt: Opt) -> Result<(), String> {
    let re1 = Regex::new(r"\[.*\] periodic-stats").unwrap();
    let re2 = Regex::new(r"\[.*\]").unwrap();
    for file in opt.plot {
        let buf = vec![];
        file.read_to_end(buf).unwrap();
        let s = buf.from_utf8().unwrap();
        let lines: Vec<&str> = s.lines().collect();
        let starts = lines
            .iter()
            .enumerate()
            .filter(|(i, l)| if re1.is_match(*l) { Some(i) } else { None });
        starts
            .into_iter()
            .map(|start| parse_periodic_stats(lines[start..]));
    }
}

fn parse_periodic_stats(lines: &Vec<&str>) -> Vec<toml::Value> {
    lines
        .iter()
        .take_while(|line| !re1.is_match(line) && re2.is_match(line))
        .map(|line| {
            let value: toml::Value = line.parse().unwrap();
            value
        })
        .collect()
}

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
