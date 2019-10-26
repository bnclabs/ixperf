use std::time::{Duration, SystemTime};

use llrb_index::Llrb;
use log::info;

use crate::generator::{Cmd, IncrementalLoad, InitialLoad, RandomKV};
use crate::stats;
use crate::Profile;

pub fn do_llrb_index(p: Profile) -> Result<(), String> {
    match (p.key_type.as_str(), p.val_type.as_str()) {
        ("i32", "i32") => Ok(perf::<i32, i32>(p)),
        ("i32", "array") => Ok(perf::<i32, [u8; 32]>(p)),
        ("i32", "bytes") => Ok(perf::<i32, Vec<u8>>(p)),
        ("i64", "i64") => Ok(perf::<i64, i64>(p)),
        ("i64", "array") => Ok(perf::<i64, [u8; 32]>(p)),
        ("i64", "bytes") => Ok(perf::<i64, Vec<u8>>(p)),
        ("array", "array") => Ok(perf::<[u8; 32], [u8; 32]>(p)),
        ("array", "bytes") => Ok(perf::<[u8; 32], Vec<u8>>(p)),
        ("bytes", "bytes") => Ok(perf::<Vec<u8>, Vec<u8>>(p)),
        _ => Err(format!(
            "unsupported key/value types {}/{}",
            p.key_type, p.val_type
        )),
    }
}

fn perf<K, V>(p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let mut index: Llrb<K, V> = Llrb::new("ixperf");
    info!(
        target: "llrbix",
        "node overhead for llrb: {}", index.stats().node_size()
    );

    let start = SystemTime::now();
    do_initial_load(&mut index, &p);
    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    info!(
        target: "llrbix",
        "initial-load completed {} items in {:?}", index.len(), dur
    );

    do_incremental(&mut index, &p);
    validate(index, p);
}

fn do_initial_load<K, V>(index: &mut Llrb<K, V>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    if p.g.loads == 0 {
        return;
    }

    info!(
        target: "llrbix",
        "INITIAL LOAD for type <{},{}>", p.key_type, p.val_type
    );
    let mut full_stats = stats::Ops::new();
    let mut local_stats = stats::Ops::new();
    let gen = InitialLoad::<K, V>::new(p.g.clone());
    let mut start = SystemTime::now();
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => {
                local_stats.load.sample_start();
                let items = index.set(key, value).map_or(0, |_| 1);
                local_stats.load.sample_end(items);
            }
            _ => unreachable!(),
        };
        if start.elapsed().unwrap().as_nanos() > 1_000_000_000 {
            info!(target: "llrbix", "periodic-stats {}", local_stats);
            full_stats.merge(&local_stats);
            local_stats = stats::Ops::new();
            start = SystemTime::now();
        }
    }
    info!(target: "llrbix", "stats \n{:?}\n", full_stats);
}

fn do_incremental<K, V>(index: &mut Llrb<K, V>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    if (p.g.read_ops() + p.g.write_ops()) == 0 {
        return;
    }

    info!(
        target: "llrbix",
        "INCREMENTAL LOAD for type <{},{}>", p.key_type, p.val_type
    );
    let mut full_stats = stats::Ops::new();
    let mut local_stats = stats::Ops::new();
    let gen = IncrementalLoad::<K, V>::new(p.g.clone());
    let mut start = SystemTime::now();
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                local_stats.set.sample_start();
                let n = index.set(key, value.clone()).map_or(0, |_| 1);
                local_stats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                local_stats.delete.sample_start();
                let items = index.delete(&key).map_or(1, |_| 0);
                local_stats.delete.sample_end(items);
            }
            Cmd::Get { key } => {
                local_stats.get.sample_start();
                let items = index.get(&key).map_or(1, |_| 0);
                local_stats.get.sample_end(items);
            }
            Cmd::Iter => {
                let iter = index.iter();
                local_stats.iter.sample_start();
                local_stats.iter.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Range { low, high } => {
                let iter = index.range((low, high));
                local_stats.range.sample_start();
                local_stats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Reverse { low, high } => {
                let iter = index.reverse((low, high));
                local_stats.reverse.sample_start();
                local_stats
                    .reverse
                    .sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            _ => unreachable!(),
        };
        if start.elapsed().unwrap().as_nanos() > 1_000_000_000 {
            info!(target: "llrbix", "periodic-stats {}", local_stats);
            full_stats.merge(&local_stats);
            local_stats = stats::Ops::new();
            start = SystemTime::now();
        }
    }

    info!(target: "llrbix", "stats - {:?}", local_stats);
}

fn validate<K, V>(index: Llrb<K, V>, _p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    // TODO: validate the statitics
    match index.validate() {
        Ok(_) => (),
        Err(err) => panic!(err),
    }
}
