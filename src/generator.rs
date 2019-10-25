use std::mem;
use std::ops::Bound;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use rand::{rngs::SmallRng, Rng, SeedableRng};
use toml;

#[derive(Default)]
pub struct GenOptions {
    pub seed: u128,
    key_size: usize,
    val_size: usize,
    loads: usize,
    sets: usize,
    deletes: usize,
    gets: usize,
    iters: usize,
    ranges: usize,
    reverses: usize,
    channel_size: usize,
}

impl GenOptions {
    pub fn read_ops(&self) -> usize {
        self.gets + self.iters + self.ranges + self.revrs
    }

    pub fn write_ops(&self) -> usize {
        self.sets + self.deletes
    }
}

impl From<toml::Value> for GenOptions {
    fn from(toml_opt: toml::Value) -> GenOptions {
        let gen_opts: GenOptions = Default::default();
        let section = &value["ixperf"];
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "seed" => gen_opts.seed = toml_to_usize(value),
                "key_size" => gen_opts.key_size = toml_to_usize(value),
                "val_size" => gen_opts.val_size = toml_to_usize(value),
                "channel_size" => gen_opts.channel_size = toml_to_usize(value),
                "loads" => gen_opts.loads = toml_to_usize(value),
                "sets" => gen_opts.sets = toml_to_usize(value),
                "deletes" => gen_opts.deletes = toml_to_usize(value),
                "gets" => gen_opts.gets = toml_to_usize(value),
                "iters" => gen_opt.iters = toml_to_usize(value),
                "ranges" => gen_opts.ranges = toml_to_usize(value),
                "revrs" => gen_opts.reverses = toml_to_usize(value),
                _ => panic!("invalid generator option {}", name),
            }
        }
        gen_opts
    }
}

pub struct InitialLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> InitialLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(g: GenOptions) -> InitialLoad<K, V> {
        let (tx, rx) = mpsc::sync_channel(g.channel_size);
        let _thread = { thread::spawn(move || initial_load(g, tx)) };
        InitialLoad { _thread, rx }
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

fn initial_load<K, V>(g: GenOptions, tx: mpsc::SyncSender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(g.seed.to_le_bytes());

    for _i in 0..g.loads {
        tx.send(Cmd::gen_load(&mut rng, &g)).unwrap();
    }

    let elapsed = start.elapsed().unwrap();
    println!(
        "gen--> initial_load(): {:10} items in {:?}",
        g.loads, elapsed
    );
}

pub struct IncrementalRead<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalRead<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(g: GenOptions) -> IncrementalRead<K, V> {
        let (tx, rx) = mpsc::sync_channel(g.gen_channel_size);
        let _thread = { thread::spawn(move || incremental_read(g, tx)) };
        IncrementalRead { _thread, rx }
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

fn incremental_read<K, V>(g: GenOptions, tx: mpsc::SyncSender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(g.seed.to_le_bytes());

    let (mut gets, mut iters) = (g.gets, g.iters);
    let (mut ranges, mut revrs) = (g.ranges, g.revrs);
    let mut total = gets + iters + ranges + revrs;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &g)
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::gen_iter(&mut rng, &g)
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &g)
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            Cmd::gen_reverse(&mut rng, &g)
        } else {
            unreachable!();
        };
        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs;
    }

    let total = g.gets + g.iters + g.ranges + g.revrs;
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
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalWrite<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(g: GenOptions) -> IncrementalWrite<K, V> {
        let (tx, rx) = mpsc::sync_channel(g.gen_channel_size);
        let _thread = { thread::spawn(move || incremental_write(g, tx)) };
        IncrementalWrite { _thread, rx }
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

fn incremental_write<K, V>(g: GenOptions, tx: mpsc::SyncSender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(g.seed.to_le_bytes());

    let (mut sets, mut dels) = (g.sets, g.deletes);
    let mut total = sets + dels;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < sets {
            sets -= 1;
            Cmd::gen_set(&mut rng, &g)
        } else if r < (sets + dels) {
            dels -= 1;
            Cmd::gen_del(&mut rng, &g)
        } else {
            unreachable!();
        };

        tx.send(cmd).unwrap();
        total = sets + dels;
    }

    let total = g.gets + g.iters + g.ranges + g.revrs;
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
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> IncrementalLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(g: GenOptions) -> IncrementalLoad<K, V> {
        let (tx, rx) = mpsc::sync_channel(g.gen_channel_size);
        let _thread = { thread::spawn(move || incremental_load(g, tx)) };
        IncrementalLoad { _thread, rx }
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

fn incremental_load<K, V>(g: GenOptions, tx: mpsc::SyncSender<Cmd<K, V>>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(g.seed.to_le_bytes());

    let (mut gets, mut iters) = (g.gets, g.iters);
    let (mut ranges, mut revrs) = (g.ranges, g.revrs);
    let (mut sets, mut dels) = (g.sets, g.deletes);
    let mut total = gets + iters + ranges + revrs + sets + dels;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &g)
        } else if r < (gets + iters) {
            iters -= 1;
            Cmd::gen_iter(&mut rng, &g)
        } else if r < (gets + iters + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &g)
        } else if r < (gets + iters + ranges + revrs) {
            revrs -= 1;
            Cmd::gen_reverse(&mut rng, &g)
        } else if r < (gets + iters + ranges + revrs + sets) {
            sets -= 1;
            Cmd::gen_set(&mut rng, &g)
        } else if r < (gets + iters + ranges + revrs + sets + dels) {
            dels -= 1;
            Cmd::gen_del(&mut rng, &g)
        } else {
            unreachable!();
        };
        tx.send(cmd).unwrap();
        total = gets + iters + ranges + revrs + sets + dels;
    }

    let total = g.gets + g.iters + g.ranges + g.revrs // reads
    + g.sets + g.deletes; // writes
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
    pub fn gen_load(rng: &mut SmallRng, g: &GenOptions) -> Cmd<K, V> {
        let (key, value): (K, V) = unsafe { (mem::zeroed(), mem::zeroed()) };
        Cmd::Load {
            key: key.gen_key(rng, g),
            value: value.gen_val(rng, g),
        }
    }

    pub fn gen_set(rng: &mut SmallRng, g: &GenOptions) -> Cmd<K, V> {
        let (key, value): (K, V) = unsafe { (mem::zeroed(), mem::zeroed()) };
        Cmd::Set {
            key: key.gen_key(rng, g),
            value: value.gen_val(rng, g),
        }
    }

    pub fn gen_del(rng: &mut SmallRng, g: &GenOptions) -> Cmd<K, V> {
        let key: K = unsafe { mem::zeroed() };
        Cmd::Delete {
            key: key.gen_key(rng, g),
        }
    }

    pub fn gen_get(rng: &mut SmallRng, g: &GenOptions) -> Cmd<K, V> {
        let key: K = unsafe { mem::zeroed() };
        Cmd::Get {
            key: key.gen_key(rng, g),
        }
    }

    pub fn gen_iter(_rng: &mut SmallRng, _g: &GenOptions) -> Cmd<K, V> {
        Cmd::Iter
    }

    pub fn gen_range(rng: &mut SmallRng, g: &GenOptions) -> Cmd<K, V> {
        let low = bounded_key::<K>(rng, g);
        let high = bounded_key::<K>(rng, g);
        Cmd::Range { low, high }
    }

    pub fn gen_reverse(rng: &mut SmallRng, g: &GenOptions) -> Cmd<K, V> {
        let low = bounded_key::<K>(rng, g);
        let high = bounded_key::<K>(rng, g);
        Cmd::Reverse { low, high }
    }
}

pub trait RandomKV {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> Self;
    fn gen_val(&self, rng: &mut SmallRng, g: &GenOptions) -> Self;
}

impl RandomKV for i32 {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> i32 {
        let limit = g.loads as i32;
        i32::abs(rng.gen::<i32>() % limit)
    }

    fn gen_val(&self, rng: &mut SmallRng, _g: &GenOptions) -> i32 {
        i32::abs(rng.gen())
    }
}

impl RandomKV for i64 {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> i64 {
        let limit = g.loads as i64;
        i64::abs(rng.gen::<i64>() % limit)
    }

    fn gen_val(&self, rng: &mut SmallRng, _g: &GenOptions) -> i64 {
        i64::abs(rng.gen())
    }
}

impl RandomKV for [u8; 32] {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> [u8; 32] {
        let limit = g.loads as i64;
        let num = i64::abs(rng.gen::<i64>() % limit);
        let mut arr = [0_u8; 32];
        let src = format!("{:032}", num).as_bytes().to_vec();
        arr.copy_from_slice(&src);
        arr
    }

    fn gen_val(&self, _rng: &mut SmallRng, _g: &GenOptions) -> [u8; 32] {
        let arr = [0xAB_u8; 32];
        arr
    }
}

impl RandomKV for Vec<u8> {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> Vec<u8> {
        let mut key = Vec::with_capacity(g.key_size);
        key.resize(g.key_size, b'0');

        let limit = g.loads as i64;
        let num = i64::abs(rng.gen::<i64>() % limit);
        let src = format!("{:0width$}", num, width = g.key_size);
        src.as_bytes().to_vec()
    }

    fn gen_val(&self, _rng: &mut SmallRng, g: &GenOptions) -> Vec<u8> {
        let mut value = Vec::with_capacity(g.val_size);
        value.resize(g.val_size, 0xAB_u8);
        value
    }
}

fn bounded_key<K>(rng: &mut SmallRng, g: &GenOptions) -> Bound<K>
where
    K: RandomKV,
{
    let key: K = unsafe { mem::zeroed() };
    let key = key.gen_key(rng, g);
    match rng.gen::<u8>() % 3 {
        0 => Bound::Included(key),
        1 => Bound::Excluded(key),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    }
}
