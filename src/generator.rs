use std::mem;
use std::ops::Bound;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::Opt;

pub struct InitialLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    opt: Opt,
    thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> InitialLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(opt: Opt) -> InitialLoad<K, V> {
        let (tx, rx) = mpsc::channel();
        let thread = {
            let opt1 = opt.clone();
            thread::spawn(move || initial_load(opt1, tx))
        };
        InitialLoad { opt, thread, rx }
    }

    fn close(self) -> Result<(), String> {
        for _cmd in self.rx {
            // drain remaining load commands here
        }
        self.thread.join().map_err(|e| format!("{:?}", e))
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

fn initial_load<K, V>(opt: Opt, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.seed.to_le_bytes());

    for _i in 0..opt.load {
        tx.send(Cmd::gen_load(&mut rng, &opt)).unwrap();
    }

    let elapsed = start.elapsed().unwrap();
    println!("--> initial_load(): {:10} items in {:?}", opt.load, elapsed);
}

pub struct IncrementalRead<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    opt: Opt,
    thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalRead<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(opt: Opt) -> IncrementalRead<K, V> {
        let (tx, rx) = mpsc::channel();
        let thread = {
            let opt1 = opt.clone();
            thread::spawn(move || incremental_read(opt1, tx))
        };
        IncrementalRead { opt, thread, rx }
    }

    fn close(self) -> Result<(), String> {
        for _cmd in self.rx {
            // drain remaining load commands here
        }
        self.thread.join().map_err(|e| format!("{:?}", e))
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

fn incremental_read<K, V>(opt: Opt, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.seed.to_le_bytes());

    let (mut gets, mut iters) = (opt.gets, opt.iters);
    let (mut ranges, mut revrs) = (opt.ranges, opt.revrs);
    let mut total = gets + iters + ranges + revrs;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &opt)
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::gen_iter(&mut rng, &opt)
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &opt)
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            Cmd::gen_reverse(&mut rng, &opt)
        } else {
            unreachable!();
        };
        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs;
    }

    let total = opt.gets + opt.iters + opt.ranges + opt.revrs;
    let elapsed = start.elapsed().unwrap();
    println!(
        "--> incremental_read(): {:10} reads in {:?}",
        total, elapsed
    );
}

pub struct IncrementalWrite<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    opt: Opt,
    thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalWrite<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(opt: Opt) -> IncrementalWrite<K, V> {
        let (tx, rx) = mpsc::channel();
        let thread = {
            let opt1 = opt.clone();
            thread::spawn(move || incremental_write(opt1, tx))
        };
        IncrementalWrite { opt, thread, rx }
    }

    fn close(self) -> Result<(), String> {
        for _cmd in self.rx {
            // drain remaining load commands here
        }
        self.thread.join().map_err(|e| format!("{:?}", e))
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

fn incremental_write<K, V>(opt: Opt, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.seed.to_le_bytes());

    let (mut sets, mut dels) = (opt.sets, opt.deletes);
    let mut total = sets + dels;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < sets {
            sets -= 1;
            Cmd::gen_set(&mut rng, &opt)
        } else if r < (sets + dels) {
            dels -= 1;
            Cmd::gen_del(&mut rng, &opt)
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = sets + dels;
    }

    let total = opt.gets + opt.iters + opt.ranges + opt.revrs;
    let elapsed = start.elapsed().unwrap();
    println!(
        "--> incremental_write(): {:10} reads in {:?}",
        total, elapsed
    );
}

pub struct IncrementalLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    opt: Opt,
    thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(opt: Opt) -> IncrementalLoad<K, V> {
        let (tx, rx) = mpsc::channel();
        let thread = {
            let opt1 = opt.clone();
            thread::spawn(move || incremental_load(opt1, tx))
        };
        IncrementalLoad { opt, thread, rx }
    }

    fn close(self) -> Result<(), String> {
        for _cmd in self.rx {
            // drain remaining load commands here
        }
        self.thread.join().map_err(|e| format!("{:?}", e))
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

fn incremental_load<K, V>(opt: Opt, tx: mpsc::Sender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.seed.to_le_bytes());

    let (mut gets, mut iters) = (opt.gets, opt.iters);
    let (mut ranges, mut revrs) = (opt.ranges, opt.revrs);
    let (mut sets, mut dels) = (opt.sets, opt.deletes);
    let mut total = gets + iters + ranges + revrs + sets + dels;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &opt)
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::gen_iter(&mut rng, &opt)
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &opt)
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            Cmd::gen_reverse(&mut rng, &opt)
        } else if r < (gets + iters + ranges + revrs + sets) {
            sets -= 1;
            Cmd::gen_set(&mut rng, &opt)
        } else if r < (gets + iters + ranges + revrs + sets + dels) {
            dels -= 1;
            Cmd::gen_del(&mut rng, &opt)
        } else {
            unreachable!();
        };
        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs + sets + dels;
    }

    let total = opt.gets + opt.iters + opt.ranges + opt.revrs // reads
    + opt.sets + opt.deletes; // writes
    let elapsed = start.elapsed().unwrap();
    println!(
        "--> incremental_load(): {:10} reads in {:?}",
        total, elapsed
    );
}

pub enum Cmd<K, V> {
    Load { key: K, value: V },
    Set { key: K, value: V },
    SetCas { key: K, value: V },
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
    pub fn gen_load(rng: &mut SmallRng, opt: &Opt) -> Cmd<K, V> {
        let (key, value): (K, V) = unsafe { (mem::zeroed(), mem::zeroed()) };
        Cmd::Load {
            key: key.gen_key(rng, opt),
            value: value.gen_val(rng, opt),
        }
    }

    pub fn gen_set(rng: &mut SmallRng, opt: &Opt) -> Cmd<K, V> {
        let (key, value): (K, V) = unsafe { (mem::zeroed(), mem::zeroed()) };
        Cmd::Set {
            key: key.gen_key(rng, opt),
            value: value.gen_val(rng, opt),
        }
    }

    pub fn gen_set_cas(rng: &mut SmallRng, opt: &Opt) -> Cmd<K, V> {
        let (key, value): (K, V) = unsafe { (mem::zeroed(), mem::zeroed()) };
        Cmd::SetCas {
            key: key.gen_key(rng, opt),
            value: value.gen_val(rng, opt),
        }
    }

    pub fn gen_del(rng: &mut SmallRng, opt: &Opt) -> Cmd<K, V> {
        let key: K = unsafe { mem::zeroed() };
        Cmd::Delete {
            key: key.gen_key(rng, opt),
        }
    }

    pub fn gen_get(rng: &mut SmallRng, opt: &Opt) -> Cmd<K, V> {
        let key: K = unsafe { mem::zeroed() };
        Cmd::Get {
            key: key.gen_key(rng, opt),
        }
    }

    pub fn gen_iter(_rng: &mut SmallRng, _opt: &Opt) -> Cmd<K, V> {
        Cmd::Iter
    }

    pub fn gen_range(rng: &mut SmallRng, opt: &Opt) -> Cmd<K, V> {
        let (low, high) = (bounded_key::<K>(rng, opt), bounded_key::<K>(rng, opt));
        Cmd::Range { low, high }
    }

    pub fn gen_reverse(rng: &mut SmallRng, opt: &Opt) -> Cmd<K, V> {
        let (low, high) = (bounded_key::<K>(rng, opt), bounded_key::<K>(rng, opt));
        Cmd::Reverse { low, high }
    }
}

pub trait RandomKV {
    fn gen_key(&self, rng: &mut SmallRng, opt: &Opt) -> Self;
    fn gen_val(&self, rng: &mut SmallRng, opt: &Opt) -> Self;
}

impl RandomKV for i32 {
    fn gen_key(&self, rng: &mut SmallRng, _opt: &Opt) -> i32 {
        i32::abs(rng.gen())
    }

    fn gen_val(&self, rng: &mut SmallRng, _opt: &Opt) -> i32 {
        i32::abs(rng.gen())
    }
}

impl RandomKV for i64 {
    fn gen_key(&self, rng: &mut SmallRng, _opt: &Opt) -> i64 {
        i64::abs(rng.gen())
    }

    fn gen_val(&self, rng: &mut SmallRng, _opt: &Opt) -> i64 {
        i64::abs(rng.gen())
    }
}

impl RandomKV for [u8; 32] {
    fn gen_key(&self, rng: &mut SmallRng, _opt: &Opt) -> [u8; 32] {
        let num = i64::abs(rng.gen());
        let mut arr = [0_u8; 32];
        let src = format!("{:032}", num).as_bytes().to_vec();
        arr.copy_from_slice(&src);
        arr
    }

    fn gen_val(&self, _rng: &mut SmallRng, _opt: &Opt) -> [u8; 32] {
        let mut arr = [0xAB_u8; 32];
        arr
    }
}

impl RandomKV for Vec<u8> {
    fn gen_key(&self, rng: &mut SmallRng, opt: &Opt) -> Vec<u8> {
        let mut key = Vec::with_capacity(opt.keysize);
        key.resize(opt.keysize, b'0');

        let num = i64::abs(rng.gen());
        let src = format!("{:0width$}", num, width = opt.keysize);
        src.as_bytes().to_vec()
    }

    fn gen_val(&self, _rng: &mut SmallRng, opt: &Opt) -> Vec<u8> {
        let mut value = Vec::with_capacity(opt.keysize);
        value.resize(opt.keysize, 0xAB_u8);
        value
    }
}

fn bounded_key<K>(rng: &mut SmallRng, opt: &Opt) -> Bound<K>
where
    K: RandomKV,
{
    let key: K = unsafe { mem::zeroed() };
    let key = key.gen_key(rng, opt);
    match rng.gen::<u8>() % 3 {
        0 => Bound::Included(key),
        1 => Bound::Excluded(key),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    }
}
