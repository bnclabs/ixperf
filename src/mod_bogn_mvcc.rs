use std::{
    thread,
    time::{Duration, SystemTime},
};

use bogn::mvcc::{Mvcc, MvccReader, MvccWriter};
use bogn::{Diff, Footprint, Index, Reader, Writer};

use crate::generator::{
    Cmd, IncrementalLoad, IncrementalRead, IncrementalWrite, InitialLoad, RandomKV,
};
use crate::stats;
use crate::Profile;

pub fn perf<K, V>(p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    <V as Diff>::D: Send + Sync,
{
    let mut index: Box<Mvcc<K, V>> = if p.lsm {
        Mvcc::new_lsm("ixperf")
    } else {
        Mvcc::new("ixperf")
    };

    let node_overhead = index.stats().to_node_size();
    println!("node overhead for bogn-mvcc: {}", node_overhead);

    let start = SystemTime::now();
    do_initial_load(&mut index, &p);
    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    println!("initial-load {} items in {:?}", index.len(), dur);

    if p.threads() == 0 {
        do_incremental(&mut index, &p);
    } else {
        let mut threads = vec![];
        for _i in 0..p.readers {
            let r = index.to_reader().unwrap();
            let pr = p.clone();
            threads.push(thread::spawn(|| do_read(r, pr)));
        }
        for _i in 0..p.writers {
            let w = index.to_writer().unwrap();
            let pr = p.clone();
            threads.push(thread::spawn(|| do_write(w, pr)));
        }
        for t in threads {
            t.join().unwrap()
        }
    }
}

fn do_initial_load<K, V>(index: &mut Box<Mvcc<K, V>>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    if p.loads == 0 {
        return;
    }

    let mut ostats = stats::Ops::new();

    let (kt, vt) = (&p.key_type, &p.val_type);
    println!("\n==== INITIAL LOAD for type <{},{}> ====", kt, vt);
    let gen = InitialLoad::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => {
                ostats.load.sample_start();
                let items = index.set(key, value).unwrap().map_or(0, |_| 1);
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

fn do_incremental<K, V>(index: &mut Box<Mvcc<K, V>>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    if (p.read_ops() + p.write_ops()) == 0 {
        return;
    }

    let mut ostats = stats::Ops::new();
    let start = SystemTime::now();

    let (kt, vt) = (&p.key_type, &p.val_type);
    println!("\n==== INCREMENTAL LOAD for type <{},{}> ====", kt, vt);
    let gen = IncrementalLoad::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                ostats.set.sample_start();
                let n = index.set(key, value.clone()).unwrap().map_or(0, |_| 1);
                ostats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                ostats.delete.sample_start();
                let items = index.delete(&key).ok().map_or(1, |_| 0);
                ostats.delete.sample_end(items);
            }
            Cmd::Get { key } => {
                ostats.get.sample_start();
                let items = index.get(&key).ok().map_or(1, |_| 0);
                ostats.get.sample_end(items);
            }
            Cmd::Iter => {
                let iter = index.iter().unwrap();
                ostats.iter.sample_start();
                ostats.iter.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Range { low, high } => {
                let iter = index.range((low, high)).unwrap();
                ostats.range.sample_start();
                ostats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Reverse { low, high } => {
                let iter = index.reverse((low, high)).unwrap();
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
    let ops = ostats.total_ops();
    let (elapsed, len) = (start.elapsed().unwrap(), index.len());
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("incremental-load {} in {:?}, index-len: {}", ops, dur, len);
}

fn do_read<K, V>(r: MvccReader<K, V>, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    if p.read_ops() == 0 {
        return;
    }

    let mut ostats = stats::Ops::new();
    let start = SystemTime::now();

    let (kt, vt) = (&p.key_type, &p.val_type);
    println!("\n==== INCREMENTAL Read for type <{},{}> ====", kt, vt);
    let gen = IncrementalRead::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Get { key } => {
                ostats.get.sample_start();
                let items = r.get(&key).ok().map_or(1, |_| 0);
                ostats.get.sample_end(items);
            }
            Cmd::Iter => {
                let iter = r.iter().unwrap();
                ostats.iter.sample_start();
                ostats.iter.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Range { low, high } => {
                let iter = r.range((low, high)).unwrap();
                ostats.range.sample_start();
                ostats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Reverse { low, high } => {
                let iter = r.reverse((low, high)).unwrap();
                ostats.reverse.sample_start();
                ostats.reverse.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            _ => unreachable!(),
        };
        if ((i + 1) % crate::LOG_BATCH) == 0 {
            p.periodic_log("incremental-read ", &ostats, false /*fin*/);
        }
    }

    p.periodic_log("incremental-read ", &ostats, true /*fin*/);
    let ops = ostats.total_ops();
    let elapsed = start.elapsed().unwrap();
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("incremental-read {} in {:?}", ops, dur);
}

fn do_write<K, V>(mut w: MvccWriter<K, V>, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    if p.write_ops() == 0 {
        return;
    }

    let mut ostats = stats::Ops::new();
    let start = SystemTime::now();

    let (kt, vt) = (&p.key_type, &p.val_type);
    println!("\n==== INCREMENTAL Write for type <{},{}> ====", kt, vt);
    let gen = IncrementalWrite::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                ostats.set.sample_start();
                let n = w.set(key, value.clone()).unwrap().map_or(0, |_| 1);
                ostats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                ostats.delete.sample_start();
                let items = w.delete(&key).ok().map_or(1, |_| 0);
                ostats.delete.sample_end(items);
            }
            _ => unreachable!(),
        };
        if ((i + 1) % crate::LOG_BATCH) == 0 {
            p.periodic_log("incremental-write ", &ostats, false /*fin*/);
        }
    }

    p.periodic_log("incremental-write ", &ostats, true /*fin*/);
    let ops = ostats.total_ops();
    let elapsed = start.elapsed().unwrap();
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("incremental-write {} in {:?}", ops, dur);
}
