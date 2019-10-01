use std::time::{Duration, SystemTime};

use llrb_index::Llrb;

use crate::generator::{Cmd, IncrementalLoad, InitialLoad, RandomKV};
use crate::stats;
use crate::Opt;

pub fn perf<K, V>(opt: Opt)
where
    K: 'static + Clone + Default + Send + Sync + Ord + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let mut index: Llrb<K, V> = Llrb::new("ixperf");
    let mut ostats = stats::Ops::new();
    println!("node overhead for llrb: {}", index.stats().node_size());

    println!("\n==== INITIAL LOAD for type {} ====", opt.typ);

    let gen = InitialLoad::<K, V>::new(opt.clone());

    let start = SystemTime::now();
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => {
                ostats.load.latency.start();
                if index.set(key, value).is_some() {
                    ostats.load.items += 1;
                }
                ostats.load.latency.stop();
                ostats.load.count += 1;
            }
            _ => unreachable!(),
        };
        if (i % crate::LOG_BATCH) == 0 {
            opt.periodic_log(&ostats, false /*fin*/);
        }
    }
    opt.periodic_log(&ostats, true /*fin*/);

    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    println!("initial-load {} items in {:?}", index.len(), dur);

    println!("\n==== INCREMENTAL LOAD for type {} ====", opt.typ);

    let gen = IncrementalLoad::<K, V>::new(opt.clone());

    let start = SystemTime::now();
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                ostats.set.latency.start();
                if index.set(key, value.clone()).is_some() {
                    ostats.set.items += 1;
                }
                ostats.set.latency.stop();
                ostats.set.count += 1;
            }
            Cmd::Delete { key } => {
                ostats.delete.latency.start();
                if index.delete(&key).is_none() {
                    ostats.delete.items += 1;
                }
                ostats.delete.latency.stop();
                ostats.delete.count += 1;
            }
            Cmd::Get { key } => {
                ostats.get.latency.start();
                if index.get(&key).is_none() {
                    ostats.get.items += 1;
                }
                ostats.get.latency.stop();
                ostats.get.count += 1;
            }
            Cmd::Iter => {
                let iter = index.iter();
                ostats.iter.latency.start();
                iter.for_each(|_| ostats.iter.items += 1);
                ostats.iter.latency.stop();
                ostats.iter.count += 1;
            }
            Cmd::Range { low, high } => {
                let iter = index.range((low, high));
                ostats.range.latency.start();
                iter.for_each(|_| ostats.range.items += 1);
                ostats.range.latency.stop();
                ostats.range.count += 1;
            }
            Cmd::Reverse { low, high } => {
                let iter = index.reverse((low, high));
                ostats.reverse.latency.start();
                iter.for_each(|_| ostats.reverse.items += 1);
                ostats.reverse.latency.stop();
                ostats.reverse.count += 1;
            }
            _ => unreachable!(),
        };
        if (i % crate::LOG_BATCH) == 0 {
            opt.periodic_log(&ostats, false /*fin*/);
        }
    }

    opt.periodic_log(&ostats, true /*fin*/);

    let (elapsed, len) = (start.elapsed().unwrap(), index.len());
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!(
        "incremental-load {} in {:?}, index-len: {}",
        ostats.total_ops(),
        dur,
        len
    );
}
