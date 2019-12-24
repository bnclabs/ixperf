#![feature(result_map_or_else)]
#![feature(test)]

mod generator;
mod latency;
mod mod_btree_map;
mod mod_llrb;
mod mod_rdms;
mod plot;
// TODO mod mod_lmdb;
mod stats;
mod utils;

use std::{
    convert::{TryFrom, TryInto},
    ffi,
    io::Write,
};

use env_logger;
use jemallocator;
use log::{self, error, info};
use rand::random;
use structopt::StructOpt;
use toml;

// TODO: try valgrid after injecting a memory leak in mvcc.
// TODO: check for unreachable!() and panic!() macros and make it more
// user friendly.

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
pub struct Opt {
    #[structopt(long = "profile", default_value = "")]
    profile: String,

    #[structopt(long = "seed", default_value = "0")]
    seed: u128,

    #[structopt(long = "plot", default_value = "")]
    plot: plot::PlotFiles,

    #[structopt(long = "ignore-error")]
    ignore_error: bool,

    #[structopt(long = "percentile", default_value = "99")]
    percentile: String,

    #[structopt(short = "v", long = "verbose")]
    verbose: bool,
}

impl TryFrom<Opt> for Profile {
    type Error = String;
    fn try_from(opt: Opt) -> Result<Profile, String> {
        let mut p: Profile = match opt.profile.as_str() {
            "" => Err(format!("please provide a profile file")),
            profile => match std::fs::read(profile) {
                Ok(text) => {
                    let text = std::str::from_utf8(&text).unwrap();
                    let toml_value = match text.parse::<toml::Value>() {
                        Ok(value) => Ok(value),
                        Err(err) => Err(format!("{:}", err)),
                    }?;
                    Ok(TryFrom::try_from(toml_value)?)
                }
                Err(err) => Err(format!("{:?}", err)),
            },
        }?;
        p.verbose = opt.verbose;
        let seed = std::cmp::max(p.g.seed, opt.seed);
        p.g.seed = match seed {
            n if n > 0 => seed,
            n if n == 0 => random(),
            n => n,
        };
        Ok(p)
    }
}

fn init_logging() {
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .target(env_logger::Target::Stdout)
        .format(|buf, record| {
            let mut level_style = buf.default_level_style(record.level());
            let color = match record.level() {
                log::Level::Error => env_logger::fmt::Color::Red,
                log::Level::Warn => env_logger::fmt::Color::Yellow,
                log::Level::Info => env_logger::fmt::Color::White,
                log::Level::Debug => env_logger::fmt::Color::Cyan,
                log::Level::Trace => env_logger::fmt::Color::Green,
            };
            level_style.set_color(color);
            if record.level() == log::Level::Info {
                level_style.set_bold(true);
            }
            writeln!(
                buf,
                "[{} {} {}] {}",
                level_style.value(buf.timestamp_millis()),
                level_style.value(record.level()),
                level_style.value(record.target()),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info)
        .init();
}

fn main() {
    init_logging();

    let opts = Opt::from_args();
    if opts.plot.0.len() > 0 {
        match plot::do_plot(opts) {
            Ok(_) => (),
            Err(err) => error!(target: "main  ", "plot-failed: {}", err),
        }
        std::process::exit(1);
    };

    let p: Profile = match opts.try_into() {
        Ok(p) => p,
        Err(err) => {
            error!(target: "main  ", "invalid args/profile: {}", err);
            std::process::exit(1);
        }
    };
    info!(target: "main  ", "starting with seed = {}", p.g.seed);

    // TODO - enable this via feature gating.
    // use cpuprofiler::PROFILER;
    // PROFILER.lock().unwrap().start("./ixperf.prof").unwrap();

    let res = match p.index.as_str() {
        "llrb-index" => mod_llrb::do_llrb_index("ixperf", p),
        "btree-map" => mod_btree_map::do_btree_map("ixperf", p),
        "rdms" => mod_rdms::do_rdms_index(p),
        _ => Err(format!("unsupported index-type {}", p.index)),
    };
    match res {
        Err(err) => error!(target: "main  ", "ixperf failed: {}", err),
        _ => (),
    };

    // PROFILER.lock().unwrap().stop().unwrap();
}

//
#[derive(Clone, Default)]
pub struct Profile {
    pub path: ffi::OsString,

    pub index: String,
    pub key_type: String,
    pub val_type: String,
    pub verbose: bool,

    pub key_footprint: usize,
    pub value_footprint: usize,

    pub g: generator::GenOptions,
    pub rdms: mod_rdms::RdmsOpt,
    pub rdms_llrb: mod_rdms::LlrbOpt,
    pub rdms_mvcc: mod_rdms::MvccOpt,
    pub rdms_robt: mod_rdms::RobtOpt,
    pub rdms_llrb_shards: mod_rdms::ShardedLlrbOpt,
}

impl TryFrom<toml::Value> for Profile {
    type Error = String;
    fn try_from(value: toml::Value) -> Result<Profile, String> {
        let mut p: Profile = Default::default();
        let section = &value["ixperf"];
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "index" => p.index = utils::toml_to_string(value),
                "key_type" => p.key_type = utils::toml_to_string(value),
                "value_type" => p.val_type = utils::toml_to_string(value),
                _ => return Err(format!("invalid option {}", name)),
            }
        }
        p.rdms = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.g = {
            let mut g: generator::GenOptions = TryFrom::try_from(value.clone())?;
            g.initial = p.rdms.initial;
            g
        };
        p.rdms_llrb = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_mvcc = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_robt = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_llrb_shards = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        Ok(p)
    }
}

#[cfg(test)]
mod jealloc_bench;
