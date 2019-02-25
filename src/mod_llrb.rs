use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime};

use llrb_index::Llrb;

use crate::generator::{init_generators, read_generator, write_generator};
use crate::latency::Latency;
use crate::opts::{Cmd, Opt};

pub fn perf(opt: Opt) {
    println!("\n==== INITIAL LOAD ====");
    let mut index = Llrb::new("ixperf");
    let refn = Arc::new(Llrb::new("reference"));
    let (opt1, opt2) = (opt.clone(), opt.clone());

    let (tx_idx, rx_idx) = mpsc::channel();
    let (tx_ref, rx_ref) = mpsc::channel();

    let generator = thread::spawn(move || init_generators(opt1, tx_idx, tx_ref));

    let refn1 = Arc::clone(&refn);
    let reference = thread::spawn(move || {
        let refn1 = unsafe {
            (Arc::into_raw(refn1) as *mut Llrb<[u8; 16], Vec<u8>>)
                .as_mut()
                .unwrap()
        };
        for item in rx_ref {
            let value: Vec<u8> = vec![];
            refn1.set(item, value);
        }
        let _refn1 = unsafe { Arc::from_raw(refn1) };
    });

    do_initial(opt2, &mut index, rx_idx);

    generator.join().unwrap();
    reference.join().unwrap();

    println!("\n==== INCREMENTAL LOAD ====");
    let refn1 = if let Ok(refn) = Arc::try_unwrap(refn) {
        refn
    } else {
        unreachable!();
    };
    let refn2 = refn1.clone();
    let (opt1, opt2, opt3) = (opt.clone(), opt.clone(), opt.clone());

    let (tx_r, rx) = mpsc::channel();
    let tx_w = mpsc::Sender::clone(&tx_r);
    let generator_r = thread::spawn(move || read_generator(1, opt1, tx_r, refn1));
    let generator_w = thread::spawn(move || write_generator(opt2, tx_w, refn2));

    do_incremental(opt3, &mut index, rx);
    generator_r.join().unwrap();
    generator_w.join().unwrap();
}

fn do_initial(opt: Opt, index: &mut Llrb<[u8; 16], Vec<u8>>, rx: mpsc::Receiver<Cmd>) {
    let mut latency = Latency::new();
    let start = SystemTime::now();

    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);

    for cmd in rx {
        match cmd {
            Cmd::Load { key } => {
                latency.start();
                index.set(key, value.clone());
                latency.stop();
            }
            _ => unreachable!(),
        };
    }

    let (elapsed, len) = (start.elapsed().unwrap(), index.count());
    let rate = len / ((elapsed.as_nanos() / 1000_000_000) as usize);
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("loaded {} items in {:?} @ {} ops/sec", len, dur, rate);
    latency.print_latency("    ");
}

fn do_incremental(opt: Opt, index: &mut Llrb<[u8; 16], Vec<u8>>, rx: mpsc::Receiver<Cmd>) {
    thread::sleep(Duration::from_millis(1000));

    let mut op_stats = init_stats();
    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);
    let mut ops = 0;

    let start = SystemTime::now();
    for cmd in rx {
        ops += 1;
        match cmd {
            Cmd::Create { key } => {
                op_stats[0].latency.start();
                index.create(key, value.clone());
                op_stats[0].latency.stop();
                op_stats[0].count += 1;
            }
            Cmd::Set { key } => {
                op_stats[1].latency.start();
                index.set(key, value.clone());
                op_stats[1].latency.stop();
                op_stats[1].count += 1;
            }
            Cmd::Delete { key } => {
                op_stats[2].latency.start();
                index.delete(&key);
                op_stats[2].latency.stop();
                op_stats[2].count += 1;
            }
            Cmd::Get { key } => {
                op_stats[3].latency.start();
                index.get(&key);
                op_stats[3].latency.stop();
                op_stats[3].count += 1;
            }
            Cmd::Iter => {
                let iter = index.iter();
                op_stats[4].latency.start();
                iter.for_each(|_| op_stats[4].items += 1);
                op_stats[4].latency.stop();
                op_stats[4].count += 1;
            }
            Cmd::Range { low, high } => {
                let iter = index.range(low, high);
                op_stats[5].latency.start();
                iter.for_each(|_| op_stats[5].items += 1);
                op_stats[5].latency.stop();
                op_stats[5].count += 1;
            }
            Cmd::Reverse { low, high } => {
                let iter = index.range(low, high).rev();
                op_stats[6].latency.start();
                iter.for_each(|_| op_stats[6].items += 1);
                op_stats[6].latency.stop();
                op_stats[6].count += 1;
            }
            _ => unreachable!(),
        };
    }

    let (elapsed, len) = (start.elapsed().unwrap(), index.count());
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("incr ops {} in {:?}, index-len: {}", ops, dur, len);

    for op_stat in op_stats.iter() {
        if op_stat.count == 0 {
            continue;
        }
        match op_stat.name.as_str() {
            "create" | "set" | "delete" | "get" => {
                println!("{} ops {}", op_stat.name, op_stat.count);
            }
            "iter" | "range" | "reverse" => {
                println!(
                    "{} ops {}, items: {}",
                    op_stat.name, op_stat.count, op_stat.items
                );
                let dur = Duration::from_nanos(
                    (op_stat.latency.average() * op_stat.latency.count()) / op_stat.items,
                );
                println!("    average latency per item: {:?}", dur);
            }
            _ => unreachable!(),
        }
        op_stat.latency.print_latency("    ");
    }
}

struct OpStat {
    name: String,
    latency: Latency,
    count: u64,
    items: u64,
}

fn init_stats() -> [OpStat; 7] {
    let (count, items) = (0, 0);
    [
        OpStat {
            name: "create".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "set".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "delete".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "get".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "iter".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "range".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "reverse".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
    ]
}
