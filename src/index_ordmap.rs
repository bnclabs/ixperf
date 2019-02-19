use std::sync::atomic::{AtomicPtr, Ordering::Relaxed};
use std::sync::mpsc;
use std::thread;
use std::time::SystemTime;

use im::ordmap::OrdMap;

use crate::generator::init_generators;
use crate::opts::{Cmd, Opt};

pub fn perf(opt: Opt) {
    if opt.load > 0 {
        let (tx_idx, rx_idx) = mpsc::channel();
        let (tx_ref, rx_ref) = mpsc::channel();
        let newopt = opt.clone();
        let loader = thread::spawn(move || init_generators(newopt, tx_idx, tx_ref));
        do_initial(&opt, SharedOrdMap::new(), rx_idx);
        for _item in rx_ref {
            // do nothing
        }
        loader.join().unwrap();
    }
}

struct SharedOrdMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    index: AtomicPtr<OrdMap<K, V>>,
}

impl<K, V> SharedOrdMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new() -> SharedOrdMap<K, V> {
        SharedOrdMap {
            index: AtomicPtr::new(Box::leak(Box::new(OrdMap::new()))),
        }
    }

    fn load(&self) -> Box<OrdMap<K, V>> {
        let index = unsafe { self.index.load(Relaxed).as_mut().unwrap() };
        unsafe { Box::from_raw(index) }
    }

    fn store(&self, index: Box<OrdMap<K, V>>) {
        self.index.store(Box::leak(index), Relaxed);
    }
}

fn do_initial(
    opt: &Opt,
    omap: SharedOrdMap<Vec<u8>, Vec<u8>>,
    rx: mpsc::Receiver<Cmd>,
) -> SharedOrdMap<Vec<u8>, Vec<u8>> {
    use crate::latency::Latency;

    let mut index = omap.load();
    let mut latency = Latency::new();

    let start = SystemTime::now();
    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);
    for cmd in rx {
        latency.start();
        match cmd {
            Cmd::Load { key } => index = Box::new(index.update(key, value.clone())),
            _ => unreachable!(),
        };
        latency.stop();
    }
    let elapsed = start.elapsed().unwrap();
    let len = index.len();
    let rate = len / ((elapsed.as_nanos() / 1000_000_000) as usize);
    println!("loaded {}, items in {:?} @ {}/sec", len, elapsed, rate);
    let (min, max) = latency.stats();
    let avg = (elapsed.as_nanos() as usize) / len;
    println!("latency (min, max, avg): {:?}", (min, max, avg));
    println!("latency percentiles: {:?}", latency.percentiles());

    omap.store(index);

    omap
}
