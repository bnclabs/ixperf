mod generator;
mod latency;
mod mod_llrb;
// TODO mod mod_lmdb;
// TODO mod mod_rdms_llrb;
// TODO mod mod_rdms_mvcc;
// TODO mod mod_rdms_robt;
mod stats;

use std::{convert::TryInto, ffi};

use jemallocator;
use rand::random;
use structopt::StructOpt;
use toml;

//  TODO: try valgrid after injecting a memory leak in mvcc.

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const LOG_BATCH: usize = 1_000_000;

#[derive(Debug, StructOpt, Clone)]
pub struct Opt {
    #[structopt(long = "profile", default_value = "")]
    profile: String,

    #[structopt(long = "seed", default_value = "0")]
    seed: u128,
}

impl From<Opt> for Profile {
    fn from(opt: Opt) -> Profile {
        if opt.profile == "" {
            panic!("please provide a profile file"); // TODO: exit(1)
        }
        let p: Profile = match std::fs::read(opt.profile) {
            Ok(text) => {
                let text = std::str::from_utf8(&text).unwrap();
                let toml_value: toml::Value = text.parse().unwrap();
                toml_value.into()
            }
            Err(err) => panic!(err), // TODO: exit(1)
        };
        p.g.seed = if opt.g.seed > 0 {
            opt.seed
        } else if p.g.seed == 0 {
            random()
        } else {
            p.gseed
        };
        p
    }
}

fn main() {
    let p: Profile = Opt::from_args().into();

    println!("starting with seed = {}", p.seed);

    // TODO - enable this via feature gating.
    // use cpuprofiler::PROFILER;
    // PROFILER.lock().unwrap().start("./ixperf.prof").unwrap();

    match p.index.as_str() {
        "llrb-index" => do_llrb_index(p),
        //"rdms-llrb" => do_rdms_llrb(p),
        //"rdms-mvcc" => do_rdms_mvcc(p),
        //"rdms-robt" => do_rdms_robt(p),
        _ => panic!("unsupported index-type {}", p.index),
    }

    // PROFILER.lock().unwrap().stop().unwrap();
}

//fn do_llrb_index(p: Profile) {
//    match (p.key_type.as_str(), p.val_type.as_str()) {
//        ("i32", "i32") => mod_llrb::perf::<i32, i32>(p),
//        ("i32", "array") => mod_llrb::perf::<i32, [u8; 32]>(p),
//        ("i32", "bytes") => mod_llrb::perf::<i32, Vec<u8>>(p),
//        ("i64", "i64") => mod_llrb::perf::<i64, i64>(p),
//        ("i64", "array") => mod_llrb::perf::<i64, [u8; 32]>(p),
//        ("i64", "bytes") => mod_llrb::perf::<i64, Vec<u8>>(p),
//        ("array", "array") => mod_llrb::perf::<[u8; 32], [u8; 32]>(p),
//        ("array", "bytes") => mod_llrb::perf::<[u8; 32], Vec<u8>>(p),
//        ("bytes", "bytes") => mod_llrb::perf::<Vec<u8>, Vec<u8>>(p),
//        _ => panic!("unsupported key/value types {}/{}", p.key_type, p.val_type),
//    }
//}
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
    pub g: GenOptions,
}

//impl Profile {
//    pub fn periodic_log(&self, prefix: &str, ostats: &stats::Ops, fin: bool) {
//        if self.json {
//            println!("{}{}", prefix, ostats.json());
//        } else {
//            ostats.pretty_print(prefix, fin);
//        }
//    }
//}

impl From<toml::Value> for Profile {
    fn from(value: toml::Value) -> Profile {
        let mut p: Profile = Default::default();
        let section = &value["ixperf"];
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "index" => p.index = util::toml_to_string(value),
                "key_type" => p.key_type = util::toml_to_string(value),
                "value_type" => p.val_type = util::toml_to_string(value),
            }
        }
        p.g = value.clone().into();
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
//                "revrs" => {
//                    p.revrs = value.as_integer().unwrap().try_into().unwrap();
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
