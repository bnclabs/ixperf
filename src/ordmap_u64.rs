use std::sync::atomic::{AtomicPtr, Ordering::Relaxed};
use std::sync::mpsc;
use std::time::SystemTime;

use im::ordmap::OrdMap;

use crate::opts::{Cmd, Opt};

pub(crate) struct SharedOrdMap<K, V>
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
    pub(crate) fn new() -> SharedOrdMap<K, V> {
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

pub(crate) fn do_initial_u64(
    _opt: &Opt,
    omap: SharedOrdMap<u64, u64>,
    rx: mpsc::Receiver<Cmd<u64>>,
) -> SharedOrdMap<u64, u64> {
    use crate::latency::Latency;

    let mut index = omap.load();
    let mut latency = Latency::new();

    let start = SystemTime::now();
    for cmd in rx {
        latency.start();
        match cmd {
            Cmd::Load { key, value } => index = Box::new(index.update(key, value)),
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
