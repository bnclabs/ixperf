use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::opts::{Cmd, Opt};

pub trait RandomKV {
    fn generate_key(&self, rng: &mut SmallRng, opt: &Opt) -> Self;
    fn generate_value(&self, rng: &mut SmallRng, opt: &Opt) -> Self;
}

impl RandomKV for u32 {
    fn generate_key(&self, rng: &mut SmallRng, _opt: &Opt) -> u32 {
        rng.gen()
    }

    fn generate_value(&self, rng: &mut SmallRng, _opt: &Opt) -> u32 {
        rng.gen()
    }
}

impl RandomKV for u64 {
    fn generate_key(&self, rng: &mut SmallRng, _opt: &Opt) -> u64 {
        rng.gen()
    }

    fn generate_value(&self, rng: &mut SmallRng, _opt: &Opt) -> u64 {
        rng.gen()
    }
}

impl RandomKV for [u8; 32] {
    fn generate_key(&self, rng: &mut SmallRng, _opt: &Opt) -> [u8; 32] {
        let mut arr = [0_u8; 32];
        (0..32).for_each(|i| arr[i] = rng.gen());
        arr
    }

    fn generate_value(&self, rng: &mut SmallRng, _opt: &Opt) -> [u8; 32] {
        let mut arr = [0_u8; 32];
        (0..32).for_each(|i| arr[i] = rng.gen());
        arr
    }
}

impl RandomKV for Vec<u8> {
    fn generate_key(&self, rng: &mut SmallRng, opt: &Opt) -> Vec<u8> {
        let mut key = Vec::with_capacity(opt.keysize);
        key.resize(opt.keysize, b'0');

        let keynum = rng.gen::<u64>().to_string().into_bytes();
        let start = opt.keysize.saturating_sub(keynum.len());
        let till = if opt.keysize < keynum.len() {
            opt.keysize
        } else {
            keynum.len()
        };
        key[start..].copy_from_slice(&keynum[..till]);
        key
    }

    fn generate_value(&self, _rng: &mut SmallRng, opt: &Opt) -> Vec<u8> {
        let mut value = Vec::with_capacity(opt.valsize);
        value.resize(opt.valsize, b'0');
        value
    }
}

pub fn init_generators<K>(opt: Opt, tx: mpsc::SyncSender<Cmd<K>>)
where
    K: RandomKV + Clone + Default + Send + 'static,
{
    if opt.load > 0 {
        let mut hs: Vec<JoinHandle<()>> = vec![];
        for id in 0..crate::NUM_GENERATORS {
            let txx = mpsc::SyncSender::clone(&tx);
            let newopt = opt.clone();
            let h = thread::spawn(move || init_generator(id + 1, newopt, txx));
            hs.push(h);
        }
        hs.into_iter().for_each(|h| h.join().unwrap());
    } else {
        println!("no initial load ...")
    }
}

fn init_generator<K>(id: usize, opt: Opt, tx: mpsc::SyncSender<Cmd<K>>)
where
    K: RandomKV + Clone + Default,
{
    let (start, n) = (SystemTime::now(), opt.load / crate::NUM_GENERATORS);
    let seed = opt.seed + ((n / id) as u128);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let k: K = Default::default();

    (0..n).for_each(|_| {
        tx.send(Cmd::generate_load(&mut rng, &opt, &k)).unwrap();
    });

    let elapsed = start.elapsed().unwrap();
    println!("-->> load gen: ({}) {} items in {:?}", id, n, elapsed);
}

pub fn read_generator<K>(id: usize, opt: Opt, tx: mpsc::SyncSender<Cmd<K>>)
where
    K: RandomKV + Clone + Default,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed((opt.seed + 1).to_le_bytes());
    let k: K = Default::default();

    let (mut gets, mut iters) = (opt.gets, opt.iters);
    let (mut ranges, mut revrs) = (opt.ranges, opt.revrs);
    let mut total = gets + iters + ranges + revrs;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::generate_get(&mut rng, &opt, &k)
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::generate_iter(&mut rng, &opt, &k)
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            Cmd::generate_range(&mut rng, &opt, &k)
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            Cmd::generate_reverse(&mut rng, &opt, &k)
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs;
    }

    let (n, elapsed) = (opt.read_load(), start.elapsed().unwrap());
    println!("-->> read gen: ({}) {} items in {:?}", id, n, elapsed);
}

pub fn write_generator<K>(opt: Opt, tx: mpsc::SyncSender<Cmd<K>>)
where
    K: RandomKV + Clone + Default,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed((opt.seed + 2).to_le_bytes());
    let k: K = Default::default();

    let (mut sets, mut deletes) = (opt.sets, opt.deletes);
    let mut total = sets + deletes;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < sets {
            sets -= 1;
            Cmd::generate_set(&mut rng, &opt, &k)
        } else if r < (sets + deletes) {
            deletes -= 1;
            Cmd::generate_delete(&mut rng, &opt, &k)
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = sets + deletes;
    }

    let (n, elapsed) = (opt.write_load(), start.elapsed().unwrap());
    println!("-->> writ gen: {} items in {:?}", n, elapsed);
}
