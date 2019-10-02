use std::time::{Duration, SystemTime};

use bogn::llrb::Llrb;
use bogn::{Diff, Footprint, Reader, Writer};

use crate::generator::{Cmd, IncrementalLoad, InitialLoad, RandomKV};
use crate::stats;
use crate::Profile;

pub fn perf<K, V>(p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    let mut index: Box<Llrb<K, V>> = if p.lsm {
        Llrb::new_lsm("ixperf")
    } else {
        Llrb::new("ixperf")
    };
    let mut ostats = stats::Ops::new();

    let node_overhead = index.stats().to_node_size();
    println!("node overhead for bogn-llrb: {}", node_overhead);

    let start = SystemTime::now();
    do_initial_load(&mut index, &mut ostats, &p);
    p.periodic_log(&ostats, true /*fin*/);
    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    println!("initial-load {} items in {:?}", index.len(), dur);

    let start = SystemTime::now();
    do_incremental(&mut index, &mut ostats, &p);
    p.periodic_log(&ostats, true /*fin*/);
    let (elapsed, len) = (start.elapsed().unwrap(), index.len());
    let ops = ostats.total_ops();
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("incremental-load {} in {:?}, index-len: {}", ops, dur, len);
}

fn do_initial_load<K, V>(
    index: &mut Box<Llrb<K, V>>, // index
    ostats: &mut stats::Ops,
    p: &Profile,
) where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    let (kt, vt) = (&p.key_type, &p.val_type);
    println!("\n==== INITIAL LOAD for type <{},{}> ====", kt, vt);
    let gen = InitialLoad::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => {
                ostats.load.latency.start();
                if let Ok(Some(_)) = index.set(key, value) {
                    ostats.load.items += 1;
                }
                ostats.load.latency.stop();
                ostats.load.count += 1;
            }
            _ => unreachable!(),
        };
        if (i % crate::LOG_BATCH) == 0 {
            p.periodic_log(&ostats, false /*fin*/);
        }
    }
}

fn do_incremental<K, V>(
    index: &mut Box<Llrb<K, V>>, // index
    ostats: &mut stats::Ops,
    p: &Profile,
) where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    let (kt, vt) = (&p.key_type, &p.val_type);
    println!("\n==== INCREMENTAL LOAD for type <{},{}> ====", kt, vt);
    let gen = IncrementalLoad::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                ostats.set.latency.start();
                if let Ok(Some(_)) = index.set(key, value.clone()) {
                    ostats.set.items += 1;
                }
                ostats.set.latency.stop();
                ostats.set.count += 1;
            }
            Cmd::Delete { key } => {
                ostats.delete.latency.start();
                if let Err(_) = index.delete(&key) {
                    ostats.delete.items += 1;
                }
                ostats.delete.latency.stop();
                ostats.delete.count += 1;
            }
            Cmd::Get { key } => {
                ostats.get.latency.start();
                if let Err(_) = index.get(&key) {
                    ostats.get.items += 1;
                }
                ostats.get.latency.stop();
                ostats.get.count += 1;
            }
            Cmd::Iter => {
                let iter = index.iter().unwrap();
                ostats.iter.latency.start();
                iter.for_each(|_| ostats.iter.items += 1);
                ostats.iter.latency.stop();
                ostats.iter.count += 1;
            }
            Cmd::Range { low, high } => {
                let iter = index.range((low, high)).unwrap();
                ostats.range.latency.start();
                iter.for_each(|_| ostats.range.items += 1);
                ostats.range.latency.stop();
                ostats.range.count += 1;
            }
            Cmd::Reverse { low, high } => {
                let iter = index.reverse((low, high)).unwrap();
                ostats.reverse.latency.start();
                iter.for_each(|_| ostats.reverse.items += 1);
                ostats.reverse.latency.stop();
                ostats.reverse.count += 1;
            }
            _ => unreachable!(),
        };
        if (i % crate::LOG_BATCH) == 0 {
            p.periodic_log(&ostats, false /*fin*/);
        }
    }
}
