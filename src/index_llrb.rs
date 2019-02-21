use std::sync::mpsc;
use std::thread;
use std::time::SystemTime;

use llrb_index::Llrb;

use crate::generator::{init_generators, read_generator, write_generator};
use crate::latency::Latency;
use crate::opts::{Cmd, Opt};

pub fn perf(opt: Opt) {
    println!("\n==== INITIAL LOAD ====");
    let mut index = Llrb::new("ixperf");
    let mut refn = Llrb::new("reference");
    let (opt1, opt2) = (opt.clone(), opt.clone());

    let (tx_idx, rx_idx) = mpsc::channel();
    let (tx_ref, rx_ref) = mpsc::channel();
    let generator = thread::spawn(move || init_generators(opt1, tx_idx, tx_ref));
    let runner = thread::spawn(|| do_initial(opt2, &mut index, rx_idx));
    for item in rx_ref {
        let value: Vec<u8> = vec![];
        refn.set(item, value);
    }
    generator.join().unwrap();
    runner.join().unwrap();

    println!("\n==== INCREMENTAL LOAD ====");
    let refn1 = refn.clone();
    let (opt1, opt2) = (opt.clone(), opt.clone());

    let (tx_r, rx) = mpsc::channel();
    let tx_w = mpsc::Sender::clone(&tx_r);
    let generator_r = thread::spawn(move || read_generator(1, opt1, tx_r, refn));
    let generator_w = thread::spawn(move || write_generator(opt2, tx_w, refn1));

    let runner = thread::spawn(|| do_incremental(opt2, &mut index, rx));
    generator_r.join().unwrap();
    generator_w.join().unwrap();
    runner.join().unwrap();
}

fn do_initial(opt: Opt, index: &mut Llrb<Vec<u8>, Vec<u8>>, rx: mpsc::Receiver<Cmd>) {
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
    let rate = len / ((elapsed.as_nanos() / 1000_000_000) as u64);
    println!("loaded {} items in {:?} @ {} ops/sec", len, elapsed, rate);
    latency.print_latency("    ");
}

fn do_incremental(opt: Opt, index: &mut Llrb<Vec<u8>, Vec<u8>>, rx: mpsc::Receiver<Cmd>) {
    let op_names = ["create", "set", "delete", "get", "iter", "range", "reverse"];
    let mut latencies: Vec<Latency> = vec![
        Latency::new(),
        Latency::new(),
        Latency::new(),
        Latency::new(),
        Latency::new(),
        Latency::new(),
        Latency::new(),
    ];
    let mut counts: Vec<usize> = vec![0, 0, 0, 0, 0, 0, 0];
    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);
    let mut ops = 0;

    let start = SystemTime::now();
    for cmd in rx {
        ops += 1;
        match cmd {
            Cmd::Create { key } => {
                latencies[0].start();
                index.create(key, value.clone());
                latencies[0].stop();
                counts[0] += 1;
            }
            Cmd::Set { key } => {
                latencies[1].start();
                index.set(key, value.clone());
                latencies[1].stop();
                counts[1] += 1;
            }
            Cmd::Delete { key } => {
                latencies[2].start();
                index.delete(&key);
                latencies[2].stop();
                counts[2] += 1;
            }
            Cmd::Get { key } => {
                latencies[3].start();
                index.get(&key);
                latencies[3].stop();
                counts[3] += 1;
            }
            Cmd::Iter => {
                let iter = index.iter();
                iter.for_each(|_| {
                    latencies[4].start();
                    counts[4] += 1;
                    latencies[4].stop();
                });
            }
            Cmd::Range { low, high } => {
                let iter = index.range(low, high);
                iter.for_each(|_| {
                    latencies[5].start();
                    counts[5] += 1;
                    latencies[5].stop();
                });
            }
            Cmd::Reverse { low, high } => {
                let iter = index.range(low, high).rev();
                iter.for_each(|_| {
                    latencies[6].start();
                    counts[6] += 1;
                    latencies[6].stop();
                });
            }
            _ => unreachable!(),
        };
    }

    let (elapsed, len) = (start.elapsed().unwrap(), index.count());
    println!("incr ops {}, {:?} index-len: {}", ops, elapsed, len);

    for (i, op_name) in op_names.iter().enumerate() {
        if counts[i] == 0 {
            continue;
        }
        println!("{} ops {}", op_name, counts[i]);
        latencies[i].print_latency("    ");
    }
}
