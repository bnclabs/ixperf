use std::sync::mpsc;
use std::time::SystemTime;

use llrb_index::Llrb;

use crate::opts::{Cmd, Opt};

pub(crate) fn do_initial_u64(
    _opt: &Opt,
    mut index: Llrb<u64, u64>,
    rx: mpsc::Receiver<Cmd<u64>>,
) -> Llrb<u64, u64> {
    use crate::latency::Latency;

    let mut latency = Latency::new();

    let start = SystemTime::now();
    for cmd in rx {
        latency.start();
        match cmd {
            Cmd::Load { key, value } => index.set(key, value),
        };
        latency.stop();
    }
    let elapsed = start.elapsed().unwrap();
    let len = index.count();
    let rate = len / ((elapsed.as_nanos() / 1000_000_000) as u64);
    println!("loaded {}, items in {:?} @ {}/sec", len, elapsed, rate);
    let (min, max) = latency.stats();
    let avg = (elapsed.as_nanos() as u64) / len;
    println!("latency (min, max, avg): {:?}", (min, max, avg));
    println!("latency percentiles: {:?}", latency.percentiles());

    index
}

//pub(crate) fn do_create_u64(
//    _opt: &Opt,
//    mut omap: SharedOrdMap<u64, u64>,
//    rx: mpsc::Receiver<Cmd<u64>>,
//) -> SharedOrdMap<u64, u64> {
//    // just do it !!
//    for cmd in rx {
//        match cmd {
//            Cmd::Load { key, value } => omap.insert(key, value),
//        };
//    }
//    omap
//}
