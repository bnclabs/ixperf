use std::ops::Bound;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::opts::{Cmd, Opt};

pub fn init_generators(opt: Opt, tx: mpsc::SyncSender<Cmd>) {
    if opt.init_load() == 0 {
        return;
    }

    let mut hs: Vec<JoinHandle<()>> = vec![];
    let n = opt.init_load() / crate::NUM_GENERATORS;
    for id in 0..crate::NUM_GENERATORS {
        let tx1 = mpsc::SyncSender::clone(&tx);
        let newopt = opt.clone();
        let h = thread::spawn(move || init_generator(id + 1, n, newopt, tx1));
        hs.push(h);
    }
    for h in hs.into_iter() {
        h.join().unwrap();
    }
}

fn init_generator(id: usize, n: usize, opt: Opt, tx: mpsc::SyncSender<Cmd>) {
    let start = SystemTime::now();
    let seed = opt.seed + ((n / id) as u128);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let mut key_print = Vec::with_capacity(opt.keysize);
    key_print.resize(opt.keysize, b'0');
    for _ in 0..n {
        let cmd = Cmd::Load {
            key: opt.gen_key(&mut rng),
        };
        tx.send(cmd).unwrap();
    }
    let elapsed = start.elapsed().unwrap();
    println!("init-gen{}: {} items in {:?}", id, n, elapsed);
}

pub fn read_generator(id: usize, opt: Opt, tx: mpsc::SyncSender<Cmd>) {
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed((opt.seed + 1).to_le_bytes());

    let (mut gets, mut iters, mut ranges, mut revrs) = (opt.gets, opt.iters, opt.ranges, opt.revrs);
    let mut total = gets + iters + ranges + revrs;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            let key = opt.gen_key(&mut rng);
            Cmd::Get { key }
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::Iter
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            let low = opt.gen_key(&mut rng);
            let high = opt.gen_key(&mut rng);
            let (low, high) = random_low_high(low, high, &mut rng);
            Cmd::Range { low, high }
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            let low = opt.gen_key(&mut rng);
            let high = opt.gen_key(&mut rng);
            let (low, high) = random_low_high(low, high, &mut rng);
            Cmd::Reverse { low, high }
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs;
    }

    let elapsed = start.elapsed().unwrap();
    println!("read-gen{}: {} items in {:?}", id, opt.read_load(), elapsed);
}

pub fn write_generator(opt: Opt, tx: mpsc::SyncSender<Cmd>) {
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed((opt.seed + 2).to_le_bytes());

    let (mut sets, mut deletes) = (opt.sets, opt.deletes);
    let mut total = sets + deletes;

    if total == 0 {
        return;
    }

    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < sets {
            sets -= 1;
            Cmd::Set {
                key: opt.gen_key(&mut rng),
            }
        } else if r < (sets + deletes) {
            deletes -= 1;
            Cmd::Delete {
                key: opt.gen_key(&mut rng),
            }
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = sets + deletes;
    }

    let elapsed = start.elapsed().unwrap();
    println!("write-gen: {} items in {:?}", opt.write_load(), elapsed);
}

fn random_low_high(
    low: Vec<u8>,
    high: Vec<u8>,
    rng: &mut SmallRng,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let low = match rng.gen::<u8>() % 3 {
        0 => Bound::Included(low),
        1 => Bound::Excluded(low),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    let high = match rng.gen::<u8>() % 3 {
        0 => Bound::Included(high),
        1 => Bound::Excluded(high),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    //println!("low_high {:?} {:?}", low, high);
    (low, high)
}
