use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime};

use llrb_index::Llrb;

use crate::generator::{init_generators, read_generator, write_generator};
use crate::opts::{Cmd, Opt};
use crate::stats;

pub fn perf(opt: Opt) {
    println!("\n==== INITIAL LOAD ====");
    let mut index = Llrb::new("ixperf");
    let refn: Arc<Llrb<Vec<u8>, Vec<u8>>> = Arc::new(Llrb::new("reference"));
    let (opt1, opt2) = (opt.clone(), opt.clone());

    println!("node overhead for llrb: {}", index.stats().node_size());

    let (tx_idx, rx_idx) = mpsc::sync_channel(1000);
    let (tx_ref, rx_ref) = mpsc::sync_channel(1000);

    let generator = thread::spawn(move || init_generators(opt1, tx_idx, tx_ref));

    let refn1 = Arc::clone(&refn);
    let reference = thread::spawn(move || {
        let refn1 = unsafe {
            (Arc::into_raw(refn1) as *mut Llrb<Vec<u8>, Vec<u8>>)
                .as_mut()
                .unwrap()
        };
        for item in rx_ref {
            let value: Vec<u8> = vec![];
            refn1.set(item, value);
        }
        let _refn1 = unsafe { Arc::from_raw(refn1) };
    });

    let dur = do_initial(opt2, &mut index, rx_idx);

    generator.join().unwrap();
    reference.join().unwrap();

    println!("loaded ({},{}) items in {:?}", index.len(), refn.len(), dur);

    println!("\n==== INCREMENTAL LOAD ====");
    let refn1 = if let Ok(refn) = Arc::try_unwrap(refn) {
        refn
    } else {
        unreachable!();
    };
    let refn2 = refn1.clone();
    let (opt1, opt2, opt3) = (opt.clone(), opt.clone(), opt.clone());

    let (tx_r, rx) = mpsc::sync_channel(1000);
    let tx_w = mpsc::SyncSender::clone(&tx_r);
    let generator_r = thread::spawn(move || read_generator(1, opt1, tx_r, refn1));
    let generator_w = thread::spawn(move || write_generator(opt2, tx_w, refn2));

    do_incremental(opt3, &mut index, rx);
    generator_r.join().unwrap();
    generator_w.join().unwrap();
}

fn do_initial(opt: Opt, index: &mut Llrb<Vec<u8>, Vec<u8>>, rx: mpsc::Receiver<Cmd>) -> Duration {
    let mut op_stats = stats::Ops::new();
    let start = SystemTime::now();

    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);

    let mut opcount = 0;
    for cmd in rx {
        opcount += 1;
        match cmd {
            Cmd::Load { key } => {
                op_stats.load.latency.start();
                let value = index.set(key, value.clone());
                op_stats.load.latency.stop();
                op_stats.load.count += 1;
                if value.is_some() {
                    op_stats.load.items += 1;
                }
            }
            _ => unreachable!(),
        };
        if (opcount % crate::LOG_BATCH) == 0 {
            opt.periodic_log(&op_stats, false /*fin*/);
        }
    }

    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);

    opt.periodic_log(&op_stats, true);

    dur
}

fn do_incremental(opt: Opt, index: &mut Llrb<Vec<u8>, Vec<u8>>, rx: mpsc::Receiver<Cmd>) {
    let mut op_stats = stats::Ops::new();
    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);
    let mut opcount = 0;

    let start = SystemTime::now();
    for cmd in rx {
        opcount += 1;
        match cmd {
            Cmd::Create { key } => {
                op_stats.create.latency.start();
                index.create(key, value.clone());
                op_stats.create.latency.stop();
                op_stats.create.count += 1;
            }
            Cmd::Set { key } => {
                op_stats.set.latency.start();
                index.set(key, value.clone());
                op_stats.set.latency.stop();
                op_stats.set.count += 1;
            }
            Cmd::Delete { key } => {
                op_stats.delete.latency.start();
                index.delete(&key);
                op_stats.delete.latency.stop();
                op_stats.delete.count += 1;
            }
            Cmd::Get { key } => {
                op_stats.get.latency.start();
                index.get(&key);
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
        if (opcount % crate::LOG_BATCH) == 0 {
            opt.periodic_log(&op_stats, false);
        }
    }

    let (elapsed, len) = (start.elapsed().unwrap(), index.len());
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("incr ops {} in {:?}, index-len: {}", opcount, dur, len);

    opt.periodic_log(&op_stats, false);
}
