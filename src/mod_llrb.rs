use std::time::{Duration, SystemTime};

use llrb_index::Llrb;

use crate::generator::{Cmd, IncrementalLoad, InitialLoad, RandomKV};
use crate::stats;
use crate::Profile;

pub fn perf<K, V>(p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let mut index: Llrb<K, V> = Llrb::new("ixperf");
    println!("node overhead for llrb: {}", index.stats().node_size());

    let start = SystemTime::now();
    do_initial_load(&mut index, &p);
    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    println!("initial-load {} items in {:?}", index.len(), dur);

    do_incremental(&mut index, &p);
    validate(index, p);
}

fn do_initial_load<K, V>(index: &mut Llrb<K, V>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    if p.loads == 0 {
        return;
    }

    let mut ostats = stats::Ops::new();
    println!(
        "\n==== INITIAL LOAD for type <{},{}> ====",
        p.key_type, p.val_type
    );
    let gen = InitialLoad::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => {
                ostats.load.sample_start();
                let items = index.set(key, value).map_or(0, |_| 1);
                ostats.load.sample_end(items);
            }
            _ => unreachable!(),
        };
        if ((i + 1) % crate::LOG_BATCH) == 0 {
            p.periodic_log("initial-load ", &ostats, false /*fin*/);
        }
    }
    p.periodic_log("initial-load ", &ostats, true /*fin*/);
}

fn do_incremental<K, V>(index: &mut Llrb<K, V>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    if (p.read_ops() + p.write_ops()) == 0 {
        return;
    }

    let mut ostats = stats::Ops::new();
    println!(
        "\n==== INCREMENTAL LOAD for type <{},{}> ====",
        p.key_type, p.val_type
    );
    let start = SystemTime::now();
    let gen = IncrementalLoad::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                ostats.set.sample_start();
                let n = index.set(key, value.clone()).map_or(0, |_| 1);
                ostats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                ostats.delete.sample_start();
                let items = index.delete(&key).map_or(1, |_| 0);
                ostats.delete.sample_end(items);
            }
            Cmd::Get { key } => {
                ostats.get.sample_start();
                let items = index.get(&key).map_or(1, |_| 0);
                ostats.get.sample_end(items);
            }
            Cmd::Iter => {
                let iter = index.iter();
                ostats.iter.sample_start();
                ostats.iter.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Range { low, high } => {
                let iter = index.range((low, high));
                ostats.range.sample_start();
                ostats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Reverse { low, high } => {
                let iter = index.reverse((low, high));
                ostats.reverse.sample_start();
                ostats.reverse.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            _ => unreachable!(),
        };
        if ((i + 1) % crate::LOG_BATCH) == 0 {
            p.periodic_log("incremental-load ", &ostats, false /*fin*/);
        }
    }

    p.periodic_log("incremental-load ", &ostats, true /*fin*/);
    let (elapsed, len) = (start.elapsed().unwrap(), index.len());
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!(
        "incremental-load {} in {:?}, index-len: {}",
        ostats.total_ops(),
        dur,
        len
    );
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
