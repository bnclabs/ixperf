use log::{debug, info};

use rdms::{self, core::Serialize, dlog, wal};

use std::{
    collections::hash_map::RandomState,
    convert::{TryFrom, TryInto},
    ffi,
    hash::{BuildHasher, Hash},
    thread,
    time::{Duration, SystemTime},
};

use crate::generator::{Cmd, IncrementalWrite, RandomKV};
use crate::stats;
use crate::Profile;

#[derive(Default, Clone)]
pub struct WalOpt {
    dir: ffi::OsString,
    name: String,
    writers: usize,
    nshards: usize,
    journal_limit: usize,
    batch_size: usize,
    nosync: bool,
    build_hasher: String,
}

impl TryFrom<toml::Value> for WalOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut opt: WalOpt = Default::default();

        let section = match &value.get("rdms-wal") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "dir" => {
                    let dir: &ffi::OsStr = value.as_str().unwrap().as_ref();
                    opt.dir = dir.to_os_string();
                }
                "name" => opt.name = value.as_str().unwrap().to_string(),
                "writers" => opt.writers = value.as_integer().unwrap().try_into().unwrap(),
                "nshards" => opt.nshards = value.as_integer().unwrap().try_into().unwrap(),
                "journal_limit" => {
                    opt.journal_limit = value.as_integer().unwrap().try_into().unwrap()
                }
                "batch_size" => opt.batch_size = value.as_integer().unwrap().try_into().unwrap(),
                "nosync" => opt.nosync = value.as_bool().unwrap(),
                "build_hasher" => opt.build_hasher = value.as_str().unwrap().to_string(),
                _ => panic!("invalid profile parameter {}", name),
            }
        }
        Ok(opt)
    }
}

impl WalOpt {
    fn new<K, V, H>(&self, name: &str, build_hasher: H) -> wal::Wal<K, V, H>
    where
        K: 'static + Clone + Default + Send + Sync + Ord + Hash + Serialize + RandomKV,
        V: 'static + Clone + Default + Send + Sync + Serialize + RandomKV,
        H: 'static + Send + Clone + BuildHasher,
    {
        let dl = dlog::Dlog::create(
            self.dir.clone(),
            name.to_string(),
            self.nshards,
            self.journal_limit,
            self.batch_size,
            self.nosync,
        )
        .unwrap();
        wal::Wal::from_dlog(dl, build_hasher)
    }
}

pub(crate) fn perf(name: &str, p: Profile) -> Result<(), String> {
    match (
        p.key_type.as_str(),
        p.val_type.as_str(),
        p.wal.build_hasher.as_str(),
    ) {
        ("i32", "i32", "random_state") => do_perf::<i32, i32, _>(name, p, RandomState::new()),
        ("i32", "i64", "random_state") => do_perf::<i32, i64, _>(name, p, RandomState::new()),
        ("i32", "array", "random_state") => {
            do_perf::<i32, [u8; 20], _>(name, p, RandomState::new())
        }
        ("i32", "bytes", "random_state") => do_perf::<i32, Vec<u8>, _>(name, p, RandomState::new()),
        ("i64", "i64", "random_state") => do_perf::<i64, i64, _>(name, p, RandomState::new()),
        ("i64", "array", "random_state") => {
            do_perf::<i64, [u8; 20], _>(name, p, RandomState::new())
        }
        ("i64", "bytes", "random_state") => do_perf::<i64, Vec<u8>, _>(name, p, RandomState::new()),
        ("array", "array", "random_state") => {
            do_perf::<[u8; 20], [u8; 20], _>(name, p, RandomState::new())
        }
        ("array", "bytes", "random_state") => {
            do_perf::<[u8; 20], Vec<u8>, _>(name, p, RandomState::new())
        }
        ("bytes", "bytes", "random_state") => {
            do_perf::<Vec<u8>, Vec<u8>, _>(name, p, RandomState::new())
        }
        _ => Err(format!(
            "unsupported key/value types {}/{}",
            p.key_type, p.val_type
        ))?,
    };

    Ok(())
}

pub(crate) fn do_perf<K, V, H>(name: &str, p: Profile, build_hasher: H) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Hash + Serialize + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Serialize + RandomKV,
    H: 'static + Send + Clone + BuildHasher,
{
    let mut wl = p.wal.new::<K, V, H>(name, build_hasher);

    let mut w_threads = vec![];
    for i in 0..p.wal.writers {
        let w = wl.to_writer().unwrap();
        let pr = p.clone();
        w_threads.push(thread::spawn(move || do_write(i, w, pr)));
    }
    let mut fstats = stats::Ops::new();
    for t in w_threads {
        fstats.merge(&t.join().unwrap());
    }
    stats!(&p.cmd_opts, "ixperf", "all-writers stats\n{:?}", fstats);

    fstats
}

fn do_write<K, V, H>(id: usize, mut w: wal::Writer<K, V, H>, mut p: Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Hash + Serialize + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Serialize + RandomKV,
    H: 'static + Send + Clone + BuildHasher,
{
    p.g.seed += (id * 100) as u128; // change the seed

    if p.g.write_ops() == 0 {
        return stats::Ops::new();
    }

    let mut fstats = stats::Ops::new();
    let elapsed = {
        let start = SystemTime::now();

        let mut lstats = stats::Ops::new();
        let gen = IncrementalWrite::<K, V>::new(p.g.clone());
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Set { key, value } => {
                    lstats.set.sample_start(false);
                    w.set(key, value.clone()).unwrap();
                    lstats.set.sample_end(0);
                }
                Cmd::Delete { key } => {
                    lstats.delete.sample_start(false);
                    w.delete(&key).unwrap();
                    lstats.delete.sample_end(0);
                }
                _ => unreachable!(),
            };
            if lstats.is_sec_elapsed() {
                stats!(
                    &p.cmd_opts,
                    "ixperf",
                    "writer-{} periodic-stats\n{}",
                    id,
                    lstats
                );
                fstats.merge(&lstats);
                lstats = stats::Ops::new();
            }
        }
        fstats.merge(&lstats);
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    stats!(&p.cmd_opts, "ixperf", "writer-{} stats\n{:?}", id, fstats);
    info!(
        target: "ixperf", "writer-{} w_ops:{} elapsed:{:?}",
        id, p.g.write_ops(), elapsed
    );

    fstats
}
