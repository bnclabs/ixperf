use std::sync::mpsc;
use std::thread;
use std::time::SystemTime;

use llrb_index::Llrb;

use crate::generator::{init_generators, read_generator};
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
    let runner = thread::spawn(move || do_initial(opt2, &mut index, rx_idx));
    for item in rx_ref {
        let value: Vec<u8> = vec![];
        refn.set(item, value);
    }
    generator.join().unwrap();
    runner.join().unwrap();

    println!("\n==== INCREMENTAL LOAD ====");

    // incremental load
}

fn do_initial(opt: Opt, index: &mut Llrb<Vec<u8>, Vec<u8>>, rx: mpsc::Receiver<Cmd>) {
    let mut latency = Latency::new();
    let start = SystemTime::now();

    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);

    for cmd in rx {
        latency.start();
        match cmd {
            Cmd::Load { key } => index.set(key, value.clone()),
            _ => unreachable!(),
        };
        latency.stop();
    }

    let (elapsed, len) = (start.elapsed().unwrap(), index.count());
    let rate = len / ((elapsed.as_nanos() / 1000_000_000) as u64);
    println!("loaded {}, items in {:?} @ {} ops/sec", len, elapsed, rate);
    latency.print_latency(elapsed.as_nanos());
}

//fn do_incremental(_opt: Opt, index: &mut Llrb<Vec<u8>, Vec<u8>>, rx: mpsc::Receiver<Cmd>) {}
