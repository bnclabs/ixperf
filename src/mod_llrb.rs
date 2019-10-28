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

    let (start, mut iter_count) = (SystemTime::now(), 0);
    if p.g.iters {
        for _ in index.iter() {
            iter_count += 1
        }
        assert_eq!(iter_count, index.len());
    }
    let iter_dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);

    do_incremental(&mut index, &p);

    if p.g.iters {
        info!(
            target: "llrbix",
            "llrb took {:?} to iter over {} items", iter_dur, iter_count
        );
    }

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
    let mut fstats = stats::Ops::new();
    let mut lstats = stats::Ops::new();
    let gen = InitialLoad::<K, V>::new(p.g.clone());
    let mut start = SystemTime::now();
    for (_i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => {
                lstats.load.sample_start(false);
                let items = index.set(key, value).map_or(0, |_| 1);
                lstats.load.sample_end(items);
            }
            _ => unreachable!(),
        };
        if start.elapsed().unwrap().as_nanos() > 1_000_000_000 && p.verbose {
            info!(target: "llrbix", "initial periodic-stats\n{}", lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
            start = SystemTime::now();
        }
    }
    info!(target: "llrbix", "initial stats\n{:?}\n", fstats);
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
    let mut fstats = stats::Ops::new();
    let mut lstats = stats::Ops::new();
    let gen = IncrementalLoad::<K, V>::new(p.g.clone());
    let mut start = SystemTime::now();
    for (_i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                lstats.set.sample_start(false);
                let n = index.set(key, value.clone()).map_or(0, |_| 1);
                lstats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                lstats.delete.sample_start(false);
                let items = index.delete(&key).map_or(1, |_| 0);
                lstats.delete.sample_end(items);
            }
            Cmd::Get { key } => {
                lstats.get.sample_start(false);
                let items = index.get(&key).map_or(1, |_| 0);
                lstats.get.sample_end(items);
            }
            Cmd::Range { low, high } => {
                let iter = index.range((low, high));
                lstats.range.sample_start(true);
                lstats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Reverse { low, high } => {
                let iter = index.reverse((low, high));
                lstats.reverse.sample_start(true);
                lstats.reverse.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            _ => unreachable!(),
        };
        if start.elapsed().unwrap().as_nanos() > 1_000_000_000 && p.verbose {
            info!(target: "llrbix", "incremental periodic-stats\n{}", lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
            start = SystemTime::now();
        }
    }

    info!(target: "llrbix", "incremental stats\n{:?}", lstats);
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
