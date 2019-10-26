mod generator;
mod latency;
mod mod_llrb;
mod plot;
// TODO mod mod_lmdb;
// TODO mod mod_rdms_llrb;
// TODO mod mod_rdms_mvcc;
// TODO mod mod_rdms_robt;
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

    #[structopt(long = "plot", default_value = "ixperf.log")]
    plot: plot::PlotFiles,

    #[structopt(long = "plot-types", default_value = "throughput,latency")]
    plot_type: plot::PlotTypes,

    #[structopt(long = "plot-ops", default_value = "load,set,deleted,get")]
    plot_op: plot::PlotOps,

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
        "llrb-index" => mod_llrb::do_llrb_index(p),
        //"rdms-llrb" => do_rdms_llrb(p),
        //"rdms-mvcc" => do_rdms_mvcc(p),
        //"rdms-robt" => do_rdms_robt(p),
        _ => Err(format!("unsupported index-type {}", p.index)),
    };
    match res {
        Err(err) => error!(target: "main  ", "ixperf failed: {}", err),
        _ => (),
    };

    // PROFILER.lock().unwrap().stop().unwrap();
}

//
//fn do_rdms_llrb(p: Profile) {
//    match (p.key_type.as_str(), p.val_type.as_str()) {
//        ("i32", "i32") => mod_rdms_llrb::perf::<i32, i32>(p),
//        // ("i32", "array") => mod_rdms_llrb::perf::<i32, [u8; 32]>(p),
//        ("i32", "bytes") => mod_rdms_llrb::perf::<i32, Vec<u8>>(p),
//        ("i64", "i64") => mod_rdms_llrb::perf::<i64, i64>(p),
//        // ("i64", "array") => mod_rdms_llrb::perf::<i64, [u8; 32]>(p),
//        ("i64", "bytes") => mod_rdms_llrb::perf::<i64, Vec<u8>>(p),
//        // ("array", "array") => mod_rdms_llrb::perf::<[u8; 32], [u8; 32]>(p),
//        // ("array", "bytes") => mod_rdms_llrb::perf::<[u8; 32], Vec<u8>>(p),
//        ("bytes", "bytes") => mod_rdms_llrb::perf::<Vec<u8>, Vec<u8>>(p),
//        _ => panic!("unsupported key/value types {}/{}", p.key_type, p.val_type),
//    }
//}
//
//fn do_rdms_mvcc(p: Profile) {
//    match (p.key_type.as_str(), p.val_type.as_str()) {
//        ("i32", "i32") => mod_rdms_mvcc::perf::<i32, i32>(p),
//        // ("i32", "array") => mod_rdms_mvcc::perf::<i32, [u8; 32]>(p),
//        ("i32", "bytes") => mod_rdms_mvcc::perf::<i32, Vec<u8>>(p),
//        ("i64", "i64") => mod_rdms_mvcc::perf::<i64, i64>(p),
//        // ("i64", "array") => mod_rdms_mvcc::perf::<i64, [u8; 32]>(p),
//        ("i64", "bytes") => mod_rdms_mvcc::perf::<i64, Vec<u8>>(p),
//        // ("array", "array") => mod_rdms_mvcc::perf::<[u8; 32], [u8; 32]>(p),
//        // ("array", "bytes") => mod_rdms_mvcc::perf::<[u8; 32], Vec<u8>>(p),
//        ("bytes", "bytes") => mod_rdms_mvcc::perf::<Vec<u8>, Vec<u8>>(p),
//        _ => panic!("unsupported key/value types {}/{}", p.key_type, p.val_type),
//    }
//}
//
//fn do_rdms_robt(p: Profile) {
//    match (p.key_type.as_str(), p.val_type.as_str()) {
//        ("i32", "i32") => mod_rdms_robt::perf::<i32, i32>(p),
//        // ("i32", "array") => mod_rdms_robt::perf::<i32, [u8; 32]>(p),
//        ("i32", "bytes") => mod_rdms_robt::perf::<i32, Vec<u8>>(p),
//        ("i64", "i64") => mod_rdms_robt::perf::<i64, i64>(p),
//        // ("i64", "array") => mod_rdms_robt::perf::<i64, [u8; 32]>(p),
//        ("i64", "bytes") => mod_rdms_robt::perf::<i64, Vec<u8>>(p),
//        // ("array", "array") => mod_rdms_robt::perf::<[u8; 32], [u8; 32]>(p),
//        // ("array", "bytes") => mod_rdms_robt::perf::<[u8; 32], Vec<u8>>(p),
//        ("bytes", "bytes") => mod_rdms_robt::perf::<Vec<u8>, Vec<u8>>(p),
//        _ => panic!("unsupported key/value types {}/{}", p.key_type, p.val_type),
//    }
//}

#[derive(Clone, Default)]
pub struct Profile {
    pub path: ffi::OsString,

    pub index: String,
    pub key_type: String,
    pub val_type: String,
    pub verbose: bool,
    pub g: generator::GenOptions,
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
        p.g = TryFrom::try_from(value.clone())?;
        Ok(p)
    }
}

//impl From<toml::Value> for Profile {
//    fn from(value: toml::Value) -> Profile {
//        let mut p: Profile = Default::default();
//
//        // common profile
//        let section = &value["ixperf"];
//        for (name, value) in section.as_table().unwrap().iter() {
//            match name.as_str() {
//                "index" => p.index = value.as_str().unwrap().to_string(),
//                "key_type" => p.key_type = value.as_str().unwrap().to_string(),
//                "value_type" => p.val_type = value.as_str().unwrap().to_string(),
//                "key_size" => {
//                    p.key_size = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "value_size" => {
//                    p.val_size = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "seed" => {
//                    p.seed = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "loads" => {
//                    p.loads = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "sets" => {
//                    p.sets = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "deletes" => {
//                    p.deletes = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "gets" => {
//                    p.gets = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "iters" => {
//                    p.iters = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "ranges" => {
//                    p.ranges = value.as_integer().unwrap().try_into().unwrap();
//                }
//                "reverses" => {
//                    p.g.reverses = value.as_integer().unwrap().try_into().unwrap();
//                }
//                _ => panic!("invalid profile parameter {}", name),
//            }
//        }
//
//        match p.index.as_str() {
//            "llrb-index" => (),
//            "rdms-llrb" => {
//                let section = &value["rdms-llrb"];
//                for (name, value) in section.as_table().unwrap().iter() {
//                    match name.as_str() {
//                        "lsm" => p.lsm = value.as_bool().unwrap(),
//                        "readers" => {
//                            let v = value.as_integer().unwrap();
//                            p.readers = v.try_into().unwrap();
//                        }
//                        "writers" => {
//                            let v = value.as_integer().unwrap();
//                            p.writers = v.try_into().unwrap();
//                        }
//                        _ => panic!("invalid profile parameter {}", name),
//                    }
//                }
//            }
//            "rdms-mvcc" => {
//                let section = &value["rdms-mvcc"];
//                for (name, value) in section.as_table().unwrap().iter() {
//                    match name.as_str() {
//                        "lsm" => p.lsm = value.as_bool().unwrap(),
//                        "readers" => {
//                            let v = value.as_integer().unwrap();
//                            p.readers = v.try_into().unwrap();
//                        }
//                        "writers" => {
//                            let v = value.as_integer().unwrap();
//                            p.writers = v.try_into().unwrap();
//                        }
//                        _ => panic!("invalid profile parameter {}", name),
//                    }
//                }
//            }
//            "rdms-robt" => {
//                let section = &value["rdms-robt"];
//                for (name, value) in section.as_table().unwrap().iter() {
//                    match name.as_str() {
//                        "readers" => {
//                            let v = value.as_integer().unwrap();
//                            p.readers = v.try_into().unwrap();
//                        }
//                        "path" => {
//                            p.path = {
//                                let v = value.as_str().unwrap();
//                                let path: &ffi::OsStr = v.as_ref();
//                                path.to_os_string()
//                            }
//                        }
//                        _ => panic!("invalid profile parameter {}", name),
//                    }
//                }
//            }
//            _ => panic!("invalid index {}", p.index),
//        }
//        p
//    }
//}
