use std::ops::Bound;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use llrb_index::Llrb;
use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::opts::{Cmd, Opt};

pub fn init_generators(opt: Opt, tx_idx: mpsc::Sender<Cmd>, tx_ref: mpsc::Sender<Vec<u8>>) {
    if opt.init_load() == 0 {
        return;
    }

    let mut hs: Vec<JoinHandle<()>> = vec![];
    let n = opt.init_load() / crate::NUM_GENERATORS;
    for id in 0..crate::NUM_GENERATORS {
        let tx_idx1 = mpsc::Sender::clone(&tx_idx);
        let tx_ref1 = mpsc::Sender::clone(&tx_ref);
        let newopt = opt.clone();
        let h = thread::spawn(move || init_generator(id + 1, n, newopt, tx_idx1, tx_ref1));
        hs.push(h);
    }
    for h in hs.into_iter() {
        h.join().unwrap();
    }
}

fn init_generator(
    id: usize,
    n: usize,
    opt: Opt,
    tx_idx: mpsc::Sender<Cmd>,
    tx_ref: mpsc::Sender<Vec<u8>>,
) {
    let start = SystemTime::now();
    let seed = opt.seed + ((n / id) as u128);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    for _ in 0..n {
        let key = opt.gen_key(&mut rng);
        let cmd = Cmd::Load { key: key.clone() };
        tx_idx.send(cmd).unwrap();
        tx_ref.send(key).unwrap();
    }
    let elapsed = start.elapsed().unwrap();
    println!("init-gen({}): {} items in {:?}", id, n, elapsed);
}

pub fn read_generator(id: i32, opt: Opt, tx: mpsc::Sender<Cmd>, mut refn: Llrb<Vec<u8>, Vec<u8>>) {
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.seed.to_le_bytes());

    let (mut gets, mut iters, mut ranges, mut revrs) = (opt.gets, opt.iters, opt.ranges, opt.revrs);
    let mut total = gets + iters + ranges + revrs;
    let mut creates = opt.creates;
    let empty_value: Vec<u8> = vec![];
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        if r < creates {
            creates -= 1; // tail the writer if configured with creates.
            let key = opt.gen_key(&mut rng);
            refn.set(key, empty_value.clone());
        }

        let cmd = if r < gets {
            gets -= 1;
            let (key, _value) = refn.random(&mut rng).unwrap();
            Cmd::Get { key }
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::Iter
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            let (low, _value) = refn.random(&mut rng).unwrap();
            let (high, _value) = refn.random(&mut rng).unwrap();
            let (low, high) = random_low_high(low, high, &mut rng);
            Cmd::Range { low, high }
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            let (low, _value) = refn.random(&mut rng).unwrap();
            let (high, _value) = refn.random(&mut rng).unwrap();
            let (low, high) = random_low_high(low, high, &mut rng);
            Cmd::Reverse { low, high }
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs;
    }

    let elapsed = start.elapsed().unwrap();
    println!(
        "read-gen({}): {} items in {:?}",
        id,
        opt.read_load(),
        elapsed
    );
}

pub fn write_generator(opt: Opt, tx: mpsc::Sender<Cmd>, mut refn: Llrb<Vec<u8>, Vec<u8>>) {
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.seed.to_le_bytes());

    let (mut creates, mut sets, mut deletes) = (opt.creates, opt.sets, opt.deletes);
    let mut total = creates + sets + deletes;
    let empty_value: Vec<u8> = vec![];
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < creates {
            creates -= 1;
            let key = opt.gen_key(&mut rng);
            refn.set(key.clone(), empty_value.clone());
            Cmd::Create { key }
        } else if r < (creates + sets) {
            sets -= 1;
            let (key, _value) = refn.random(&mut rng).unwrap();
            Cmd::Set { key }
        } else if r < (creates + sets + deletes) {
            deletes -= 1;
            let (key, _value) = refn.random(&mut rng).unwrap();
            Cmd::Delete { key }
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = creates + sets + deletes;
    }

    let elapsed = start.elapsed().unwrap();
    println!("write-gen: {} items in {:?}", opt.write_load(), elapsed);
}

fn random_low_high(
    low: Vec<u8>,
    high: Vec<u8>,
    rng: &mut SmallRng,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let low = match rng.gen::<i32>() % 3 {
        0 => Bound::Included(low),
        1 => Bound::Excluded(low),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    let high = match rng.gen::<i32>() % 3 {
        0 => Bound::Included(high),
        1 => Bound::Excluded(high),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    };
    //println!("low_high {:?} {:?}", low, high);
    (low, high)
}
