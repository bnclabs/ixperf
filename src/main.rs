mod generator;
mod latency;
mod mod_bogn_llrb;
mod mod_llrb;
//mod mod_lmdb;
mod stats;

use std::{convert::TryInto, ffi};

use rand::random;
use structopt::StructOpt;
use toml;

const LOG_BATCH: usize = 1_000_000;

#[derive(Debug, StructOpt, Clone)]
pub struct Opt {
    #[structopt(long = "profile", default_value = "")]
    profile: String,

    #[structopt(long = "seed", default_value = "0")]
    seed: u128,
}

fn main() {
    let p: Profile = Opt::from_args().into();

    println!("starting with seed = {}", p.seed);

    match p.index.as_str() {
        "llrb" => do_llrb_index(p),
        "bogn-llrb" => do_bogn_llrb(p),
        _ => panic!("unsupported index-type {}", p.index),
    }
}

fn do_llrb_index(p: Profile) {
    match (p.key_type.as_str(), p.val_type.as_str()) {
        ("i32", "i32") => mod_llrb::perf::<i32, i32>(p),
        ("i32", "array") => mod_llrb::perf::<i32, [u8; 32]>(p),
        ("i32", "bytes") => mod_llrb::perf::<i32, Vec<u8>>(p),
        ("i64", "i64") => mod_llrb::perf::<i64, i64>(p),
        ("i64", "array") => mod_llrb::perf::<i64, [u8; 32]>(p),
        ("i64", "bytes") => mod_llrb::perf::<i64, Vec<u8>>(p),
        ("array", "array") => mod_llrb::perf::<[u8; 32], [u8; 32]>(p),
        ("array", "bytes") => mod_llrb::perf::<[u8; 32], Vec<u8>>(p),
        ("bytes", "bytes") => mod_llrb::perf::<Vec<u8>, Vec<u8>>(p),
        _ => panic!("unsupported key/value types {}/{}", p.key_type, p.val_type),
    }
}

fn do_bogn_llrb(p: Profile) {
    match (p.key_type.as_str(), p.val_type.as_str()) {
        ("i32", "i32") => mod_bogn_llrb::perf::<i32, i32>(p),
        // ("i32", "array") => mod_bogn_llrb::perf::<i32, [u8; 32]>(p),
        ("i32", "bytes") => mod_bogn_llrb::perf::<i32, Vec<u8>>(p),
        ("i64", "i64") => mod_bogn_llrb::perf::<i64, i64>(p),
        // ("i64", "array") => mod_bogn_llrb::perf::<i64, [u8; 32]>(p),
        ("i64", "bytes") => mod_bogn_llrb::perf::<i64, Vec<u8>>(p),
        // ("array", "array") => mod_bogn_llrb::perf::<[u8; 32], [u8; 32]>(p),
        // ("array", "bytes") => mod_bogn_llrb::perf::<[u8; 32], Vec<u8>>(p),
        ("bytes", "bytes") => mod_bogn_llrb::perf::<Vec<u8>, Vec<u8>>(p),
        _ => panic!("unsupported key/value types {}/{}", p.key_type, p.val_type),
    }
}

#[derive(Clone)]
pub struct Profile {
    pub index: String,
    pub key_type: String,
    pub val_type: String,
    pub path: ffi::OsString,
    pub key_size: usize,
    pub val_size: usize,
    pub json: bool,
    pub lsm: bool,
    pub seed: u128,
    pub readers: usize,
    pub writers: usize,
    pub loads: usize,
    pub sets: usize,
    pub deletes: usize,
    pub gets: usize,
    pub iters: usize,
    pub ranges: usize,
    pub revrs: usize,
}

impl Profile {
    pub fn read_ops(&self) -> usize {
        self.gets + self.iters + self.ranges + self.revrs
    }

    pub fn write_ops(&self) -> usize {
        self.sets + self.deletes
    }

    pub fn periodic_log(&self, ostats: &stats::Ops, fin: bool) {
        if self.json {
            println!("{}", ostats.json());
        } else {
            ostats.pretty_print("", fin);
        }
    }
}

impl Default for Profile {
    fn default() -> Profile {
        let path = {
            let mut path = std::env::temp_dir();
            path.push("ixperf");
            path.push("default");
            path.into_os_string()
        };
        let seed: u128 = random();
        Profile {
            index: "llrb".to_string(),
            key_type: "i64".to_string(),
            val_type: "i64".to_string(),
            path,
            key_size: 64,
            val_size: 64,
            json: false,
            lsm: false,
            seed,
            readers: 0,
            writers: 0,
            loads: 1_000_000,
            sets: 0,
            deletes: 0,
            gets: 0,
            iters: 0,
            ranges: 0,
            revrs: 0,
        }
    }
}

impl From<toml::Value> for Profile {
    fn from(value: toml::Value) -> Profile {
        let mut p: Profile = Default::default();
        for (name, value) in value.as_table().unwrap().iter() {
            match name.as_str() {
                "index" => p.index = value.as_str().unwrap().to_string(),
                "key_type" => p.key_type = value.as_str().unwrap().to_string(),
                "value_type" => p.val_type = value.as_str().unwrap().to_string(),
                "path" => {
                    p.path = {
                        let path: &ffi::OsStr = value.as_str().unwrap().as_ref();
                        path.to_os_string()
                    }
                }
                "key_size" => {
                    p.key_size = value.as_integer().unwrap().try_into().unwrap();
                }
                "value_size" => {
                    p.val_size = value.as_integer().unwrap().try_into().unwrap();
                }
                "json" => p.json = value.as_bool().unwrap(),
                "lsm" => p.lsm = value.as_bool().unwrap(),
                "seed" => {
                    p.seed = value.as_integer().unwrap().try_into().unwrap();
                }
                "readers" => {
                    p.readers = value.as_integer().unwrap().try_into().unwrap();
                }
                "writers" => {
                    p.writers = value.as_integer().unwrap().try_into().unwrap();
                }
                "loads" => {
                    p.loads = value.as_integer().unwrap().try_into().unwrap();
                }
                "sets" => {
                    p.sets = value.as_integer().unwrap().try_into().unwrap();
                }
                "deletes" => {
                    p.deletes = value.as_integer().unwrap().try_into().unwrap();
                }
                "gets" => {
                    p.gets = value.as_integer().unwrap().try_into().unwrap();
                }
                "iters" => {
                    p.iters = value.as_integer().unwrap().try_into().unwrap();
                }
                "ranges" => {
                    p.ranges = value.as_integer().unwrap().try_into().unwrap();
                }
                "revrs" => {
                    p.revrs = value.as_integer().unwrap().try_into().unwrap();
                }
                _ => panic!("invalid profile parameter {}", name),
            }
        }
        p
    }
}

impl From<Opt> for Profile {
    fn from(opt: Opt) -> Profile {
        let mut p: Profile = if opt.profile == "" {
            Default::default()
        } else {
            match std::fs::read(opt.profile) {
                Ok(text) => {
                    let text = std::str::from_utf8(&text).unwrap();
                    let toml_value: toml::Value = text.parse().unwrap();
                    toml_value.into()
                }
                Err(err) => panic!(err),
            }
        };
        if opt.seed > 0 {
            p.seed = opt.seed;
        }
        p
    }
}
