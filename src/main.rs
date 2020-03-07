#![feature(test)]

#[cfg(feature = "cpuprofile")]
use cpuprofiler::PROFILER;

use env_logger;
use jemallocator;
use log::{self, error, info};
use rand::random;
use structopt::StructOpt;
use toml;

use std::{
    convert::{TryFrom, TryInto},
    fs,
    io::Write,
    thread, time,
};

mod generator;
mod latency;
mod mod_btree_map;
mod mod_llrb;
mod mod_lmdb;
mod mod_rdms;
mod mod_rdms_llrb;
mod mod_rdms_mvcc;
mod mod_rdms_robt;
mod mod_rdms_shllrb;
mod plot;
mod stats;
mod utils;

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
            profile => match fs::read(profile) {
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
    match do_main() {
        Ok(_) => (),
        Err(err) => error!(target: "main  ", "failure: {}", err),
    }
}

fn do_main() -> Result<(), String> {
    init_logging();

    let opts = Opt::from_args();
    if opts.plot.0.len() > 0 {
        let opts = Opt::from_args();
        plot::do_plot(opts)?;
        std::process::exit(0);
    };

    thread::spawn(|| system_stats());

    let p: Profile = opts.try_into()?;

    info!(target: "main  ", "starting with seed = {}", p.g.seed);

    #[cfg(feature = "cpuprofile")]
    {
        let file_name = "./ixperf.prof";
        {
            fs::remove_file(file_name).map_err(|e| format!("{:?}", e))?;
            fs::File::create(file_name).map_err(|e| format!("{:?}", e))?;
        }
        PROFILER.lock().unwrap().start(file_name).unwrap();
    }

    let res = match p.index.as_str() {
        "llrb-index" => mod_llrb::perf("ixperf", p),
        "btree-map" => mod_btree_map::perf("ixperf", p),
        "lmdb" => mod_lmdb::perf(p),
        "rdms" => mod_rdms::do_rdms_index(p),
        _ => Err(format!("unsupported index-type {}", p.index)),
    };
    match res {
        Err(err) => error!(target: "main  ", "ixperf failed: {}", err),
        _ => (),
    };

    #[cfg(feature = "cpuprofile")]
    {
        PROFILER.lock().unwrap().stop().unwrap()
    }

    Ok(())
}

#[derive(Clone, Default)]
pub struct Profile {
    pub index: String,
    pub key_type: String,
    pub val_type: String,
    pub verbose: bool,

    pub key_footprint: usize,
    pub value_footprint: usize,

    pub g: generator::GenOptions,
    pub lmdb: mod_lmdb::LmdbOpt,
    pub rdms: mod_rdms::RdmsOpt,
    pub rdms_llrb: mod_rdms_llrb::LlrbOpt,
    pub rdms_mvcc: mod_rdms_mvcc::MvccOpt,
    pub rdms_robt: mod_rdms_robt::RobtOpt,
    pub rdms_shllrb: mod_rdms_shllrb::ShllrbOpt,
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

        p.g = {
            let mut g: generator::GenOptions = TryFrom::try_from(value.clone())?;
            g.initial = p.rdms.initial;
            g
        };

        p.lmdb = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_llrb = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_mvcc = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_robt = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_shllrb = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        Ok(p)
    }
}

fn system_stats() {
    use sysinfo::{ProcessorExt, System, SystemExt};

    let mut sys = System::new();

    loop {
        thread::sleep(time::Duration::from_secs(1));
        sys.refresh_system();

        let mut cpu_load = 0_f32;
        for cpu in sys.get_processor_list() {
            cpu_load += cpu.get_cpu_usage();
        }
        let cpu_load = (cpu_load * 100_f32) as u64;
        let mem_rss = sys.get_used_memory() / 1024;

        let line = format!("system = {{ cpu_load={}, mem_rss={} }}", cpu_load, mem_rss);
        info!(target: "ixperf", "system periodic-stats\n{}", line);
    }
}

#[cfg(test)]
mod jealloc_bench;
