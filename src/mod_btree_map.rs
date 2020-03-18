use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

use log::{debug, info};

use crate::generator::{Cmd, IncrementalLoad, InitialLoad, RandomKV};
use crate::stats;
use crate::Profile;

pub fn perf(name: &str, p: Profile) -> Result<(), String> {
    match (p.key_type.as_str(), p.val_type.as_str()) {
        ("i32", "i32") => Ok(do_perf::<i32, i32>(name, p)),
        ("i32", "i64") => Ok(do_perf::<i32, i64>(name, p)),
        ("i32", "array") => Ok(do_perf::<i32, [u8; 20]>(name, p)),
        ("i32", "bytes") => Ok(do_perf::<i32, Vec<u8>>(name, p)),
        ("i64", "i64") => Ok(do_perf::<i64, i64>(name, p)),
        ("i64", "array") => Ok(do_perf::<i64, [u8; 20]>(name, p)),
        ("i64", "bytes") => Ok(do_perf::<i64, Vec<u8>>(name, p)),
        ("array", "array") => Ok(do_perf::<[u8; 20], [u8; 20]>(name, p)),
        ("array", "bytes") => Ok(do_perf::<[u8; 20], Vec<u8>>(name, p)),
        ("bytes", "bytes") => Ok(do_perf::<Vec<u8>, Vec<u8>>(name, p)),
        _ => Err(format!(
            "unsupported key/value types {}/{}",
            p.key_type, p.val_type
        )),
    }
}

fn do_perf<K, V>(_name: &str, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    debug!(
        target: "ixperf",
        "intial load for type <{},{}>", p.key_type, p.val_type
    );

    let mut map: BTreeMap<K, V> = BTreeMap::new();
    do_initial_load(&mut map, &p);
    do_incremental(&mut map, &p);
}

fn do_initial_load<K, V>(map: &mut BTreeMap<K, V>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let load_ops = p.g.loads;
    if load_ops == 0 {
        return;
    }

    let mut fstats = stats::Ops::new();
    let elapsed = {
        let start = SystemTime::now();

        let mut lstats = stats::Ops::new();
        let gen = InitialLoad::<K, V>::new(p.g.clone());
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Load { key, value } => {
                    lstats.load.sample_start(false);
                    let items = map.insert(key, value).map_or(0, |_| 1);
                    lstats.load.sample_end(items);
                }
                _ => unreachable!(),
            };
            if p.cmd_opts.verbose && lstats.is_sec_elapsed() {
                stats!(&p.cmd_opts, "ixperf", "initial periodic-stats\n{}", lstats);
                fstats.merge(&lstats);
                lstats = stats::Ops::new();
            }
        }
        fstats.merge(&lstats);
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    stats!(&p.cmd_opts, "ixperf", "initial stats\n{:?}", fstats);
    info!(
        target: "ixperf",
        "initial-load load_ops:{} map.len:{} elapsed:{:?}",
        load_ops, map.len(), elapsed
    );
}

fn do_incremental<K, V>(index: &mut BTreeMap<K, V>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    if (p.g.read_ops() + p.g.write_ops()) == 0 {
        return;
    }

    let mut fstats = stats::Ops::new();
    let elapsed = {
        let start = SystemTime::now();
        let mut lstats = stats::Ops::new();
        let gen = IncrementalLoad::<K, V>::new(p.g.clone());
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Set { key, value } => {
                    lstats.set.sample_start(false);
                    let n = index.insert(key, value.clone()).map_or(0, |_| 1);
                    lstats.set.sample_end(n);
                }
                Cmd::Delete { key } => {
                    lstats.delete.sample_start(false);
                    let items = index.remove(&key).map_or(1, |_| 0);
                    lstats.delete.sample_end(items);
                }
                Cmd::Get { key } => {
                    lstats.get.sample_start(false);
                    let items = index.get(&key).map_or(1, |_| 0);
                    lstats.get.sample_end(items);
                }
                _ => unreachable!(),
            };
            if p.cmd_opts.verbose && lstats.is_sec_elapsed() {
                stats!(
                    &p.cmd_opts,
                    "ixperf",
                    "incremental periodic-stats\n{}",
                    lstats
                );
                fstats.merge(&lstats);
                lstats = stats::Ops::new();
            }
        }
        fstats.merge(&lstats);
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    stats!(&p.cmd_opts, "ixperf", "incremental stats\n{:?}", fstats);
    info!(
        target: "ixperf",
        "incremental-load r_ops:{} w_ops:{}, map.len:{} elapsed:{:?}",
        p.g.read_ops(), p.g.write_ops(), index.len(), elapsed
    );
}
