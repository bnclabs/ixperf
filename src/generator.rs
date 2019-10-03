use std::mem;
use std::ops::Bound;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::Profile;

pub struct InitialLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    _p: Profile,
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> InitialLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(_p: Profile) -> InitialLoad<K, V> {
        let (tx, rx) = mpsc::channel();
        let _thread = {
            let opt1 = _p.clone();
            thread::spawn(move || initial_load(opt1, tx))
        };
        InitialLoad { _p, _thread, rx }
    }
}

impl<K, V> Iterator for InitialLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

fn initial_load<K, V>(p: Profile, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(p.seed.to_le_bytes());

    for _i in 0..p.loads {
        tx.send(Cmd::gen_load(&mut rng, &p)).unwrap();
    }

    let elapsed = start.elapsed().unwrap();
    println!(
        "gen--> initial_load(): {:10} items in {:?}",
        p.loads, elapsed
    );
}

pub struct IncrementalRead<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    _p: Profile,
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalRead<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(_p: Profile) -> IncrementalRead<K, V> {
        let (tx, rx) = mpsc::channel();
        let _thread = {
            let opt1 = _p.clone();
            thread::spawn(move || incremental_read(opt1, tx))
        };
        IncrementalRead { _p, _thread, rx }
    }
}

impl<K, V> Iterator for IncrementalRead<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

fn incremental_read<K, V>(p: Profile, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(p.seed.to_le_bytes());

    let (mut gets, mut iters) = (p.gets, p.iters);
    let (mut ranges, mut revrs) = (p.ranges, p.revrs);
    let mut total = gets + iters + ranges + revrs;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &p)
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::gen_iter(&mut rng, &p)
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &p)
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            Cmd::gen_reverse(&mut rng, &p)
        } else {
            unreachable!();
        };
        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs;
    }

    let total = p.gets + p.iters + p.ranges + p.revrs;
    let elapsed = start.elapsed().unwrap();
    println!(
        "gen--> incremental_read(): {:10} reads in {:?}",
        total, elapsed
    );
}

pub struct IncrementalWrite<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    _p: Profile,
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalWrite<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(_p: Profile) -> IncrementalWrite<K, V> {
        let (tx, rx) = mpsc::channel();
        let _thread = {
            let opt1 = _p.clone();
            thread::spawn(move || incremental_write(opt1, tx))
        };
        IncrementalWrite { _p, _thread, rx }
    }
}

impl<K, V> Iterator for IncrementalWrite<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

fn incremental_write<K, V>(p: Profile, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(p.seed.to_le_bytes());

    let (mut sets, mut dels) = (p.sets, p.deletes);
    let mut total = sets + dels;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < sets {
            sets -= 1;
            Cmd::gen_set(&mut rng, &p)
        } else if r < (sets + dels) {
            dels -= 1;
            Cmd::gen_del(&mut rng, &p)
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = sets + dels;
    }

    let total = p.gets + p.iters + p.ranges + p.revrs;
    let elapsed = start.elapsed().unwrap();
    println!(
        "gen--> incremental_write(): {:10} reads in {:?}",
        total, elapsed
    );
}

pub struct IncrementalLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    _p: Profile,
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(_p: Profile) -> IncrementalLoad<K, V> {
        let (tx, rx) = mpsc::channel();
        let _thread = {
            let opt1 = _p.clone();
            thread::spawn(move || incremental_load(opt1, tx))
        };
        IncrementalLoad { _p, _thread, rx }
    }
}

impl<K, V> Iterator for IncrementalLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

fn incremental_load<K, V>(p: Profile, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(p.seed.to_le_bytes());

    let (mut gets, mut iters) = (p.gets, p.iters);
    let (mut ranges, mut revrs) = (p.ranges, p.revrs);
    let (mut sets, mut dels) = (p.sets, p.deletes);
    let mut total = gets + iters + ranges + revrs + sets + dels;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &p)
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::gen_iter(&mut rng, &p)
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &p)
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            Cmd::gen_reverse(&mut rng, &p)
        } else if r < (gets + iters + ranges + revrs + sets) {
            sets -= 1;
            Cmd::gen_set(&mut rng, &p)
        } else if r < (gets + iters + ranges + revrs + sets + dels) {
            dels -= 1;
            Cmd::gen_del(&mut rng, &p)
        } else {
            unreachable!();
        };
        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs + sets + dels;
    }

    let total = p.gets + p.iters + p.ranges + p.revrs // reads
    + p.sets + p.deletes; // writes
    let elapsed = start.elapsed().unwrap();
    println!(
        "gen--> incremental_load(): {:10} reads in {:?}",
        total, elapsed
    );
}

pub enum Cmd<K, V> {
    Load { key: K, value: V },
    Set { key: K, value: V },
    Delete { key: K },
    Get { key: K },
    Iter,
    Range { low: Bound<K>, high: Bound<K> },
    Reverse { low: Bound<K>, high: Bound<K> },
}

impl<K, V> Cmd<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn gen_load(rng: &mut SmallRng, p: &Profile) -> Cmd<K, V> {
        let (key, value): (K, V) = unsafe { (mem::zeroed(), mem::zeroed()) };
        Cmd::Load {
            key: key.gen_key(rng, p),
            value: value.gen_val(rng, p),
        }
    }

    pub fn gen_set(rng: &mut SmallRng, p: &Profile) -> Cmd<K, V> {
        let (key, value): (K, V) = unsafe { (mem::zeroed(), mem::zeroed()) };
        Cmd::Set {
            key: key.gen_key(rng, p),
            value: value.gen_val(rng, p),
        }
    }

    pub fn gen_del(rng: &mut SmallRng, p: &Profile) -> Cmd<K, V> {
        let key: K = unsafe { mem::zeroed() };
        Cmd::Delete {
            key: key.gen_key(rng, p),
        }
    }

    pub fn gen_get(rng: &mut SmallRng, p: &Profile) -> Cmd<K, V> {
        let key: K = unsafe { mem::zeroed() };
        Cmd::Get {
            key: key.gen_key(rng, p),
        }
    }

    pub fn gen_iter(_rng: &mut SmallRng, _p: &Profile) -> Cmd<K, V> {
        Cmd::Iter
    }

    pub fn gen_range(rng: &mut SmallRng, p: &Profile) -> Cmd<K, V> {
        let low = bounded_key::<K>(rng, p);
        let high = bounded_key::<K>(rng, p);
        Cmd::Range { low, high }
    }

    pub fn gen_reverse(rng: &mut SmallRng, p: &Profile) -> Cmd<K, V> {
        let low = bounded_key::<K>(rng, p);
        let high = bounded_key::<K>(rng, p);
        Cmd::Reverse { low, high }
    }
}

pub trait RandomKV {
    fn gen_key(&self, rng: &mut SmallRng, p: &Profile) -> Self;
    fn gen_val(&self, rng: &mut SmallRng, p: &Profile) -> Self;
}

impl RandomKV for i32 {
    fn gen_key(&self, rng: &mut SmallRng, _p: &Profile) -> i32 {
        i32::abs(rng.gen())
    }

    fn gen_val(&self, rng: &mut SmallRng, _p: &Profile) -> i32 {
        i32::abs(rng.gen())
    }
}

impl RandomKV for i64 {
    fn gen_key(&self, rng: &mut SmallRng, _p: &Profile) -> i64 {
        i64::abs(rng.gen())
    }

    fn gen_val(&self, rng: &mut SmallRng, _p: &Profile) -> i64 {
        i64::abs(rng.gen())
    }
}

impl RandomKV for [u8; 32] {
    fn gen_key(&self, rng: &mut SmallRng, _p: &Profile) -> [u8; 32] {
        let num = i64::abs(rng.gen());
        let mut arr = [0_u8; 32];
        let src = format!("{:032}", num).as_bytes().to_vec();
        arr.copy_from_slice(&src);
        arr
    }

    fn gen_val(&self, _rng: &mut SmallRng, _p: &Profile) -> [u8; 32] {
        let arr = [0xAB_u8; 32];
        arr
    }
}

impl RandomKV for Vec<u8> {
    fn gen_key(&self, rng: &mut SmallRng, p: &Profile) -> Vec<u8> {
        let mut key = Vec::with_capacity(p.key_size);
        key.resize(p.key_size, b'0');

        let num = i64::abs(rng.gen());
        let src = format!("{:0width$}", num, width = p.key_size);
        src.as_bytes().to_vec()
    }

    fn gen_val(&self, _rng: &mut SmallRng, p: &Profile) -> Vec<u8> {
        let mut value = Vec::with_capacity(p.val_size);
        value.resize(p.val_size, 0xAB_u8);
        value
    }
}

fn bounded_key<K>(rng: &mut SmallRng, p: &Profile) -> Bound<K>
where
    K: RandomKV,
{
    let key: K = unsafe { mem::zeroed() };
    let key = key.gen_key(rng, p);
    match rng.gen::<u8>() % 3 {
        0 => Bound::Included(key),
        1 => Bound::Excluded(key),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    }
}
