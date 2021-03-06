#![feature(test)]

#[cfg(feature = "cpuprofile")]
use cpuprofiler::PROFILER;

use simplelog;
use jemallocator;
use log::{self, debug, error};
use rand::random;
use structopt::StructOpt;
use toml;

use std::{convert::TryFrom, io, path, fs, thread, time};

mod generator;
mod latency;
mod mod_btree_map;
mod mod_llrb;
mod mod_lmdb;
mod mod_rdms;
mod mod_rdms_dgm;
mod mod_rdms_llrb;
mod mod_rdms_mvcc;
mod mod_rdms_robt;
mod mod_rdms_shllrb;
mod mod_rdms_shrobt;
mod mod_wal;
mod mod_xorfilter;
mod plot;
mod stats;
#[macro_use]
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

    #[structopt(long = "ignore-error", help = "Ignore log errors while plotting")]
    ignore_error: bool,

    #[structopt(long = "percentile", default_value = "99")]
    percentile: String,

    #[structopt(long = "log-file", default_value="")]
    log_file: String,

    #[structopt(short = "v", long = "verbose")]
    verbose: bool,

    #[structopt(long = "trace")]
    trace: bool,

    #[structopt(long = "stats")]
    stats: bool,
}

fn main() {
    match do_main() {
        Ok(_) => (),
        Err(err) => error!(target: "main  ", "{}", err),
    }
}

fn do_main() -> Result<(), String> {
    let opts = Opt::from_args();
    init_logger(&opts)?;

    if opts.plot.0.len() > 0 {
        let opts = Opt::from_args();
        plot::do_plot(opts)?;
        std::process::exit(0);
    };

    thread::spawn(|| system_stats());

    let p: Profile = Profile::new()?;

    debug!(target: "main  ", "starting with seed = {}", p.g.seed);

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
        "xorfilter" => mod_xorfilter::perf(p),
        "rdms" => mod_rdms::do_rdms_index(p),
        "wal" => mod_wal::perf("ixperf", p),
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

pub struct Profile {
    pub index: String,
    pub key_type: String,
    pub val_type: String,
    pub cmd_opts: Opt,

    pub key_footprint: usize,
    pub value_footprint: usize,

    pub g: generator::GenOptions,
    pub lmdb: mod_lmdb::LmdbOpt,
    pub rdms: mod_rdms::RdmsOpt,
    pub rdms_llrb: mod_rdms_llrb::LlrbOpt,
    pub rdms_mvcc: mod_rdms_mvcc::MvccOpt,
    pub rdms_robt: mod_rdms_robt::RobtOpt,
    pub rdms_shrobt: mod_rdms_shrobt::ShrobtOpt,
    pub rdms_shllrb: mod_rdms_shllrb::ShllrbOpt,
    pub rdms_dgm: mod_rdms_dgm::DgmOpt,
    pub wal: mod_wal::WalOpt,
}

impl Default for Profile {
    fn default() -> Profile {
        Profile {
            index: Default::default(),
            key_type: Default::default(),
            val_type: Default::default(),
            cmd_opts: Opt::from_args(),

            key_footprint: Default::default(),
            value_footprint: Default::default(),

            g: Default::default(),
            lmdb: Default::default(),
            rdms: Default::default(),
            rdms_llrb: Default::default(),
            rdms_mvcc: Default::default(),
            rdms_robt: Default::default(),
            rdms_shrobt: Default::default(),
            rdms_shllrb: Default::default(),
            rdms_dgm: Default::default(),
            wal: Default::default(),
        }
    }
}

impl Clone for Profile {
    fn clone(&self) -> Profile {
        Profile {
            index: self.index.clone(),
            key_type: self.key_type.clone(),
            val_type: self.val_type.clone(),
            cmd_opts: Opt::from_args(),

            key_footprint: self.key_footprint,
            value_footprint: self.value_footprint,

            g: self.g.clone(),
            lmdb: self.lmdb.clone(),
            rdms: self.rdms.clone(),
            rdms_llrb: self.rdms_llrb.clone(),
            rdms_mvcc: self.rdms_mvcc.clone(),
            rdms_robt: self.rdms_robt.clone(),
            rdms_shrobt: self.rdms_shrobt.clone(),
            rdms_shllrb: self.rdms_shllrb.clone(),
            rdms_dgm: self.rdms_dgm.clone(),
            wal: self.wal.clone(),
        }
    }
}

impl Profile {
    fn new() -> Result<Profile, String> {
        let opt = Opt::from_args();
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
        let seed = std::cmp::max(p.g.seed, opt.seed);
        p.g.seed = match seed {
            n if n > 0 => seed,
            n if n == 0 => random(),
            n => n,
        };
        p.cmd_opts = opt;
        Ok(p)
    }
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
        p.rdms_shrobt = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_shllrb = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.rdms_dgm = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        p.wal = TryFrom::try_from(value.clone())
            .ok()
            .unwrap_or(Default::default());
        Ok(p)
    }
}

fn system_stats() {
    use sysinfo::{ProcessExt, System, SystemExt};

    let opts = Opt::from_args();
    let mut sys = System::new();

    loop {
        thread::sleep(time::Duration::from_secs(1));
        sys.refresh_processes();
        for (_pid, p) in sys.get_processes() {
            if p.name() != "ixperf" {
                continue;
            }
            let cpu = p.cpu_usage();
            let memory = p.memory() / 1024;

            let line = format!(
                //
                "system = {{ cpu_load={:.2}, mem_rss={} }}", cpu, memory
            );
            stats!(opts, "ixperf", "system periodic-stats\n{}", line);
            break;
        }
    }
}

fn init_logger(opts: &Opt) -> Result<(), String> {
    let level_filter = if opts.trace {
        simplelog::LevelFilter::Trace
    } else if opts.verbose {
        simplelog::LevelFilter::Debug
    } else {
        simplelog::LevelFilter::Info
    };

    let mut config = simplelog::ConfigBuilder::new();
    config
        .set_location_level(simplelog::LevelFilter::Off)
        .set_target_level(simplelog::LevelFilter::Off)
        .set_thread_mode(simplelog::ThreadLogMode::Both)
        .set_thread_level(simplelog::LevelFilter::Error)
        .set_time_to_local(true)
        .set_time_format("[%Y-%m-%dT%H:%M:%S%.3fZ]".to_string());

    if opts.log_file.len() > 0 {
        let p = path::Path::new(&opts.log_file);
        let log_file = if p.is_relative() {
            let mut cwd = std::env::current_dir().map_err(|e| e.to_string())?;
            cwd.push(&p);
            cwd.into_os_string()
        } else {
            p.as_os_str().to_os_string()
        };
        let fs = fs::File::create(&log_file).map_err(|e| e.to_string())?;
        simplelog::WriteLogger::init(
            level_filter,
            config.build(),
            fs
        )
    } else {
        simplelog::WriteLogger::init(
            level_filter,
            config.build(),
            io::stdout()
        )
    }
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod jealloc_bench;
