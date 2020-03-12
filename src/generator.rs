use log::debug;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use toml;

use std::{
    convert::TryFrom,
    mem,
    ops::Bound,
    sync::mpsc,
    thread::{self, JoinHandle},
    time::SystemTime,
};

use crate::utils;

enum Tx<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    N(mpsc::Sender<Cmd<K, V>>),
    S(mpsc::SyncSender<Cmd<K, V>>),
}

impl<K, V> Tx<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    fn post(&self, msg: Cmd<K, V>) -> Result<(), String> {
        match self {
            Tx::N(tx) => tx.send(msg).map_err(|e| format!("{:?}", e))?,
            Tx::S(tx) => tx.send(msg).map_err(|e| format!("{:?}", e))?,
        }

        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct GenOptions {
    pub seed: u128,
    pub key_size: usize,
    pub val_size: usize,
    pub loads: usize,
    pub sets: usize,
    pub deletes: usize,
    pub gets: usize,
    pub ranges: usize,
    pub reverses: usize,
    pub iters: bool,
    pub channel_size: usize,
    // from rdms
    pub initial: usize,
}

impl GenOptions {
    pub fn reset_writes(&mut self) {
        self.sets = 0;
        self.deletes = 0;
    }

    pub fn read_ops(&self) -> usize {
        self.gets + self.ranges + self.reverses
    }

    pub fn write_ops(&self) -> usize {
        self.sets + self.deletes
    }
}

impl TryFrom<toml::Value> for GenOptions {
    type Error = String;
    fn try_from(value: toml::Value) -> Result<GenOptions, String> {
        let mut gen_opts: GenOptions = Default::default();
        let section = &value["generator"];
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "seed" => gen_opts.seed = utils::toml_to_u128(value),
                "key_size" => gen_opts.key_size = utils::toml_to_usize(value),
                "value_size" => gen_opts.val_size = utils::toml_to_usize(value),
                "channel_size" => {
                    // something
                    gen_opts.channel_size = utils::toml_to_usize(value)
                }
                "loads" => gen_opts.loads = utils::toml_to_usize(value),
                "sets" => gen_opts.sets = utils::toml_to_usize(value),
                "deletes" => gen_opts.deletes = utils::toml_to_usize(value),
                "gets" => gen_opts.gets = utils::toml_to_usize(value),
                "ranges" => gen_opts.ranges = utils::toml_to_usize(value),
                "reverses" => gen_opts.reverses = utils::toml_to_usize(value),
                "iters" => gen_opts.iters = utils::toml_to_bool(value),
                _ => return Err(format!("invalid generator option {}", name)),
            }
        }
        Ok(gen_opts)
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
        let (tx, rx) = if g.channel_size > 0 {
            let (tx, rx) = mpsc::sync_channel(g.channel_size);
            (Tx::S(tx), rx)
        } else {
            let (tx, rx) = mpsc::channel();
            (Tx::N(tx), rx)
        };

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

fn initial_load<K, V>(g: GenOptions, tx: Tx<K, V>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(g.seed.to_le_bytes());

    for _i in 0..g.loads {
        tx.post(Cmd::gen_load(&mut rng, &g)).unwrap();
    }

    let elapsed = start.elapsed().unwrap();
    debug!(
        target: "genrtr",
        "initial_load: generated {} items in {:?}", g.loads, elapsed
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
        let (tx, rx) = if g.channel_size > 0 {
            let (tx, rx) = mpsc::sync_channel(g.channel_size);
            (Tx::S(tx), rx)
        } else {
            let (tx, rx) = mpsc::channel();
            (Tx::N(tx), rx)
        };

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

fn incremental_read<K, V>(g: GenOptions, tx: Tx<K, V>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(g.seed.to_le_bytes());

    let (mut gets, mut ranges, mut reverses) = (g.gets, g.ranges, g.reverses);
    let mut total = gets + ranges + reverses;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &g)
        } else if r < (gets + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &g)
        } else if r < (gets + ranges + reverses) {
            reverses -= 1;
            Cmd::gen_reverse(&mut rng, &g)
        } else {
            unreachable!();
        };
        tx.post(cmd).unwrap();
        total = gets + ranges + reverses;
    }

    let total = g.gets + g.ranges + g.reverses;
    let elapsed = start.elapsed().unwrap();
    debug!(
        target: "genrtr",
        "incremental_read: generated {:10} ops in {:?}", total, elapsed
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
        let (tx, rx) = if g.channel_size > 0 {
            let (tx, rx) = mpsc::sync_channel(g.channel_size);
            (Tx::S(tx), rx)
        } else {
            let (tx, rx) = mpsc::channel();
            (Tx::N(tx), rx)
        };

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

fn incremental_write<K, V>(g: GenOptions, tx: Tx<K, V>)
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

        tx.post(cmd).unwrap();
        total = sets + dels;
    }

    let total = g.sets + g.deletes;
    let elapsed = start.elapsed().unwrap();
    debug!(
        target: "genrtr",
        "incremental_write: generated {:10} ops in {:?}", total, elapsed
    );
}

pub struct ConcurrentLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    _thread: JoinHandle<()>,
    rx: mpsc::Receiver<Cmd<K, V>>,
}

impl<K, V> ConcurrentLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    pub fn new(g: GenOptions) -> ConcurrentLoad<K, V> {
        let (tx, rx) = if g.channel_size > 0 {
            let (tx, rx) = mpsc::sync_channel(g.channel_size);
            (Tx::S(tx), rx)
        } else {
            let (tx, rx) = mpsc::channel();
            (Tx::N(tx), rx)
        };

        let _thread = { thread::spawn(move || concurrent_load(g, tx)) };
        ConcurrentLoad { _thread, rx }
    }
}

impl<K, V> Iterator for ConcurrentLoad<K, V>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

fn concurrent_load<K, V>(g: GenOptions, tx: Tx<K, V>)
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
    V: 'static + Clone + Default + Send + Sync + RandomKV,
{
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(g.seed.to_le_bytes());

    let (mut gets, mut ranges, mut reverses) = (g.gets, g.ranges, g.reverses);
    let (mut sets, mut dels) = (g.sets, g.deletes);
    let mut total = gets + ranges + reverses + sets + dels;
    while total > 0 {
        let r: usize = rng.gen::<usize>() % total;
        let cmd = if r < gets {
            gets -= 1;
            Cmd::gen_get(&mut rng, &g)
        } else if r < (gets + ranges) {
            ranges -= 1;
            Cmd::gen_range(&mut rng, &g)
        } else if r < (gets + ranges + reverses) {
            reverses -= 1;
            Cmd::gen_reverse(&mut rng, &g)
        } else if r < (gets + ranges + reverses + sets) {
            sets -= 1;
            Cmd::gen_set(&mut rng, &g)
        } else if r < (gets + ranges + reverses + sets + dels) {
            dels -= 1;
            Cmd::gen_del(&mut rng, &g)
        } else {
            unreachable!();
        };
        tx.post(cmd).unwrap();
        total = gets + ranges + reverses + sets + dels;
    }

    let total = g.gets + g.ranges + g.reverses + g.sets + g.deletes;
    let elapsed = start.elapsed().unwrap();
    debug!(
        target: "genrtr",
        "concurrent_load: generated {:10} ops in {:?}", total, elapsed
    );
}

pub enum Cmd<K, V> {
    Load { key: K, value: V },
    Set { key: K, value: V },
    Delete { key: K },
    Get { key: K },
    Range { low: Bound<K>, high: Bound<K> },
    Reverse { low: Bound<K>, high: Bound<K> },
}

impl<K, V> Cmd<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
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
    fn next(&self, g: &GenOptions) -> Self;
}

impl RandomKV for i32 {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> i32 {
        let limit = (g.loads * std::cmp::max(g.initial, 1)) as i32;
        i32::abs(rng.gen::<i32>() % limit)
    }

    fn gen_val(&self, rng: &mut SmallRng, _g: &GenOptions) -> i32 {
        i32::abs(rng.gen())
    }

    fn next(&self, _g: &GenOptions) -> i32 {
        *self + 1
    }
}

impl RandomKV for i64 {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> i64 {
        let limit = (g.loads * std::cmp::max(g.initial, 1)) as i64;
        i64::abs(rng.gen::<i64>() % limit)
    }

    fn gen_val(&self, rng: &mut SmallRng, _g: &GenOptions) -> i64 {
        i64::abs(rng.gen())
    }

    fn next(&self, _g: &GenOptions) -> i64 {
        *self + 1
    }
}

impl RandomKV for [u8; 32] {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> [u8; 32] {
        let limit = (g.loads * std::cmp::max(g.initial, 1)) as i64;
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

    fn next(&self, _g: &GenOptions) -> [u8; 32] {
        let s = std::str::from_utf8(self).unwrap();
        let n: i64 = s.parse().unwrap();
        let mut arr = [0_u8; 32];
        let src = format!("{:032}", n + 1).as_bytes().to_vec();
        arr.copy_from_slice(&src);
        arr
    }
}

impl RandomKV for [u8; 20] {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> [u8; 20] {
        let limit = (g.loads * std::cmp::max(g.initial, 1)) as i64;
        let num = i64::abs(rng.gen::<i64>() % limit);
        let mut arr = [0_u8; 20];
        let src = format!("{:020}", num).as_bytes().to_vec();
        arr.copy_from_slice(&src);
        arr
    }

    fn gen_val(&self, _rng: &mut SmallRng, _g: &GenOptions) -> [u8; 20] {
        let arr = [0xAB_u8; 20];
        arr
    }

    fn next(&self, _g: &GenOptions) -> [u8; 20] {
        let s = std::str::from_utf8(self).unwrap();
        let n: i64 = s.parse().unwrap();
        let mut arr = [0_u8; 20];
        let src = format!("{:020}", n + 1).as_bytes().to_vec();
        arr.copy_from_slice(&src);
        arr
    }
}

impl RandomKV for Vec<u8> {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> Vec<u8> {
        let mut key = Vec::with_capacity(g.key_size);
        key.resize(g.key_size, b'0');

        let limit = (g.loads * std::cmp::max(g.initial, 1)) as i64;
        let num = i64::abs(rng.gen::<i64>() % limit);
        let src = format!("{:0width$}", num, width = g.key_size);
        src.as_bytes().to_vec()
    }

    fn gen_val(&self, _rng: &mut SmallRng, g: &GenOptions) -> Vec<u8> {
        let mut value = Vec::with_capacity(g.val_size);
        value.resize(g.val_size, 0xAB_u8);
        value
    }

    fn next(&self, g: &GenOptions) -> Vec<u8> {
        let s = std::str::from_utf8(self).unwrap();
        let n: i64 = s.parse().unwrap();

        let mut key = Vec::with_capacity(g.key_size);
        key.resize(g.key_size, b'0');
        let src = format!("{:0width$}", n + 1, width = g.key_size);
        src.as_bytes().to_vec()
    }
}

#[allow(dead_code)]
pub struct IterKeys<K>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
{
    from: K,
    g: GenOptions,
}

impl<K> IterKeys<K>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
{
    #[allow(dead_code)]
    fn new(from: K, g: &GenOptions) -> IterKeys<K> {
        IterKeys { from, g: g.clone() }
    }
}

impl<K> Iterator for IterKeys<K>
where
    K: 'static + Clone + Default + Send + Sync + RandomKV,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.from.next(&self.g))
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
