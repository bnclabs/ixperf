use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime};

use llrb_index::Llrb;

use crate::generator::RandomKV;
use crate::generator::{init_generators, read_generator, write_generator};
use crate::opts::{Cmd, Opt};
use crate::stats;

pub fn perf<T>(opt: Opt)
where
    T: RandomKV + Clone + Ord + Default + Send + 'static,
{
    let mut index: Llrb<T, T> = Llrb::new("ixperf");

    println!("\n==== INITIAL LOAD for type {} ====", opt.typ);
    println!("node overhead for llrb: {}", index.stats().node_size());

    let optt = opt.clone();
    let (tx, rx) = mpsc::sync_channel(1000);
    let generator = thread::spawn(move || init_generators(optt, tx));
    do_init(opt.clone(), &mut index, rx);
    generator.join().unwrap();

    println!("\n==== INCREMENTAL LOAD for type {} ====", opt.typ);
    let (optr, optw) = (opt.clone(), opt.clone());

    let (tx_r, rx) = mpsc::sync_channel(1000);
    let tx_w = mpsc::SyncSender::clone(&tx_r);
    let generator_r = thread::spawn(move || read_generator(1, optr, tx_r));
    let generator_w = thread::spawn(move || write_generator(optw, tx_w));
    do_incr(opt.clone(), &mut index, rx);
    generator_r.join().unwrap();
    generator_w.join().unwrap();
}

fn do_init<T>(opt: Opt, index: &mut Llrb<T, T>, rx: mpsc::Receiver<Cmd<T>>)
where
    T: RandomKV + Clone + Ord,
{
    let mut op_stats = stats::Ops::new();
    let (start, mut opcount) = (SystemTime::now(), 0);
    for cmd in rx {
        match cmd {
            Cmd::Load { key, value } => {
                op_stats.load.latency.start();
                if index.set(key, value).is_some() {
                    op_stats.load.items += 1;
                }
                op_stats.load.latency.stop();
                op_stats.load.count += 1;
            }
            _ => unreachable!(),
        };
        opcount += 1;
        if (opcount % crate::LOG_BATCH) == 0 {
            opt.periodic_log(&op_stats, false /*fin*/);
        }
    }
    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);

    opt.periodic_log(&op_stats, true);
    println!("init ops {} items in {:?}", index.len(), dur);
}

fn do_incr<T>(opt: Opt, index: &mut Llrb<T, T>, rx: mpsc::Receiver<Cmd<T>>)
where
    T: RandomKV + Clone + Ord,
{
    let mut op_stats = stats::Ops::new();
    let (start, mut opcount) = (SystemTime::now(), 0);
    for cmd in rx {
        match cmd {
            Cmd::Set { key, value } => {
                op_stats.set.latency.start();
                if index.set(key, value.clone()).is_some() {
                    op_stats.set.items += 1;
                }
                op_stats.set.latency.stop();
                op_stats.set.count += 1;
            }
            Cmd::Delete { key } => {
                op_stats.delete.latency.start();
                if index.delete(&key).is_none() {
                    op_stats.delete.items += 1;
                }
                op_stats.delete.latency.stop();
                op_stats.delete.count += 1;
            }
            Cmd::Get { key } => {
                op_stats.get.latency.start();
                if index.get(&key).is_none() {
                    op_stats.get.items += 1;
                }
                op_stats.get.latency.stop();
                op_stats.get.count += 1;
            }
            Cmd::Iter => {
                let iter = index.iter();
                op_stats.iter.latency.start();
                iter.for_each(|_| op_stats.iter.items += 1);
                op_stats.iter.latency.stop();
                op_stats.iter.count += 1;
            }
            Cmd::Range { low, high } => {
                let iter = index.range(low, high);
                op_stats.range.latency.start();
                iter.for_each(|_| op_stats.range.items += 1);
                op_stats.range.latency.stop();
                op_stats.range.count += 1;
            }
            Cmd::Reverse { low, high } => {
                let iter = index.range(low, high).rev();
                op_stats.reverse.latency.start();
                iter.for_each(|_| op_stats.reverse.items += 1);
                op_stats.reverse.latency.stop();
                op_stats.reverse.count += 1;
            }
            _ => unreachable!(),
        };
        opcount += 1;
        if (opcount % crate::LOG_BATCH) == 0 {
            opt.periodic_log(&op_stats, false);
        }
    }
    let (elapsed, len) = (start.elapsed().unwrap(), index.len());
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    opt.periodic_log(&op_stats, true);
    println!("incr ops {} in {:?}, index-len: {}", opcount, dur, len);
}
