use std::sync::mpsc;
use std::thread;
use std::time::SystemTime;

use im::ordmap::OrdMap;
use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::lib::Opt;

pub(crate) fn initial_index(opt: &Opt) {
    if opt.cmdopt.load == 0 {
        println!("no initial load for this index !!");
        return;
    }

    let (tx, rx) = mpsc::channel();
    let opt = opt.clone();
    let loader = thread::spawn(move || feed_loader(opt, tx));

    let mut omap = OrdMap::new();

    let start = SystemTime::now();
    for (key, value) in rx {
        omap.insert(key, value);
    }
    println!(
        "loaded {}, items in {:?}",
        omap.len(),
        start.elapsed().unwrap()
    );

    loader.join().unwrap();
}

fn feed_loader(opt: Opt, tx: mpsc::Sender<(u64, u64)>) {
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.cmdopt.seed.to_le_bytes());
    for _ in 0..opt.cmdopt.load {
        tx.send((rng.gen(), rng.gen())).unwrap();
    }
    let elapsed = start.elapsed().unwrap();
    println!("generated {} items in {:?}", opt.cmdopt.load, elapsed);
}
