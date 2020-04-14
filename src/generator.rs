use log::debug;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use toml;

use std::{
    convert::TryFrom,
    cmp,
    mem,
    ops::Bound,
    time,
};

use crate::utils;

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
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    g: GenOptions,
    n_load: usize,
    rng: SmallRng,
    items: Vec<Cmd<K,V>>,
    elapsed: time::Duration,
}

impl<K, V> InitialLoad<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    pub fn new(g: GenOptions) -> InitialLoad<K, V> {
        let rng = SmallRng::from_seed(g.seed.to_le_bytes());
        InitialLoad {
            g: g.clone(),
            n_load: g.loads,
            rng,
            items: Default::default(),
            elapsed: Default::default(),
        }
    }

    pub fn log(&self) {
        debug!(
            target: "genrtr",
            "initial_load: generated {} items in {:?}",
            self.g.loads - self.n_load, self.elapsed
        );
    }
}

impl<K, V> Iterator for InitialLoad<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.pop() {
            Some(item) => Some(item),
            None if self.n_load == 0 => {
                self.log();
                None
            }
            None => {
                let start = time::SystemTime::now();
                let n = cmp::min(self.n_load, self.g.channel_size);
                for _ in 0..n {
                    self.items.push(Cmd::gen_load(&mut self.rng, &self.g));
                }
                self.elapsed += start.elapsed().unwrap();
                self.n_load -= n;
                self.items.pop()
            }
        }
    }

}

pub struct IncrementalRead<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    g: GenOptions,
    n_gets: usize,
    n_ranges: usize,
    n_reverses: usize,
    rng: SmallRng,
    items: Vec<Cmd<K,V>>,
    elapsed: time::Duration,
}

impl<K, V> IncrementalRead<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    pub fn new(g: GenOptions) -> IncrementalRead<K, V> {
        let rng = SmallRng::from_seed(g.seed.to_le_bytes());
        IncrementalRead {
            g: g.clone(),
            n_gets: g.gets,
            n_ranges: g.ranges,
            n_reverses: g.reverses,
            rng,
            items: Default::default(),
            elapsed: Default::default(),
        }
    }

    pub fn log(&self) {
        debug!(
            target: "genrtr",
            "incr_read: generated {} items in {:?}",
            self.to_total() - self.to_n_total(), self.elapsed
        );
    }

    fn to_n_total(&self) -> usize {
        self.n_gets + self.n_ranges + self.n_reverses
    }

    fn to_total(&self) -> usize {
        self.g.gets + self.g.ranges + self.g.reverses
    }
}

impl<K, V> Iterator for IncrementalRead<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.pop() {
            Some(item) => Some(item),
            None if self.to_n_total() == 0 => {
                self.log();
                None
            }
            None => {
                let start = time::SystemTime::now();
                let n = cmp::min(self.to_n_total(), self.g.channel_size);
                for _ in 0..n {
                    let r: usize = self.rng.gen::<usize>() % self.to_n_total();
                    let cmd = if r < self.n_gets {
                        self.n_gets -= 1;
                        Cmd::gen_get(&mut self.rng, &self.g)
                    } else if r < (self.n_gets + self.n_ranges) {
                        self.n_ranges -= 1;
                        Cmd::gen_range(&mut self.rng, &self.g)
                    } else if r < self.to_n_total() {
                        self.n_reverses -= 1;
                        Cmd::gen_reverse(&mut self.rng, &self.g)
                    } else {
                        unreachable!();
                    };
                    self.items.push(cmd);
                }
                self.elapsed += start.elapsed().unwrap();
                self.items.pop()
            }
        }
    }
}

pub struct IncrementalWrite<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    g: GenOptions,
    n_sets: usize,
    n_deletes: usize,
    rng: SmallRng,
    items: Vec<Cmd<K,V>>,
    elapsed: time::Duration,
}

impl<K, V> IncrementalWrite<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    pub fn new(g: GenOptions) -> IncrementalWrite<K, V> {
        let rng = SmallRng::from_seed(g.seed.to_le_bytes());
        IncrementalWrite { 
            g: g.clone(),
            n_sets: g.sets,
            n_deletes: g.deletes,
            rng,
            items: Default::default(),
            elapsed: Default::default(),
        }
    }

    pub fn log(&self) {
        debug!(
            target: "genrtr",
            "incr_write: generated {} items in {:?}",
            self.to_total() - self.to_n_total(), self.elapsed
        );
    }

    fn to_n_total(&self) -> usize {
        self.n_sets + self.n_deletes
    }

    fn to_total(&self) -> usize {
        self.g.sets + self.g.deletes
    }
}

impl<K, V> Iterator for IncrementalWrite<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.pop() {
            Some(item) => Some(item),
            None if self.to_n_total() == 0 => {
                self.log();
                None
            }
            None => {
                let start = time::SystemTime::now();
                let n = cmp::min(self.to_n_total(), self.g.channel_size);
                for _ in 0..n {
                    let r: usize = self.rng.gen::<usize>() % self.to_n_total();
                    let cmd = if r < self.n_sets {
                        self.n_sets -= 1;
                        Cmd::gen_set(&mut self.rng, &self.g)
                    } else if r < self.to_n_total() {
                        self.n_deletes -= 1;
                        Cmd::gen_del(&mut self.rng, &self.g)
                    } else {
                        unreachable!();
                    };
                    self.items.push(cmd);
                }
                self.elapsed += start.elapsed().unwrap();
                self.items.pop()
            }
        }
    }
}

pub struct IncrementalLoad<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    g: GenOptions,
    n_gets: usize,
    n_ranges: usize,
    n_reverses: usize,
    n_sets: usize,
    n_deletes: usize,
    rng: SmallRng,
    items: Vec<Cmd<K,V>>,
    elapsed: time::Duration,
}

impl<K, V> IncrementalLoad<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    pub fn new(g: GenOptions) -> IncrementalLoad<K, V> {
        let rng = SmallRng::from_seed(g.seed.to_le_bytes());
        IncrementalLoad {
            g: g.clone(),
            n_gets: g.gets,
            n_ranges: g.ranges ,
            n_reverses: g.reverses,
            n_sets: g.sets,
            n_deletes: g.deletes,
            rng,
            items: Default::default(),
            elapsed: Default::default(),
        }
    }

    pub fn log(&self) {
        debug!(
            target: "genrtr",
            "incr_load: generated {} items in {:?}",
            self.to_total() - self.to_n_total(), self.elapsed
        );
    }

    fn to_n_total(&self) -> usize {
        self.n_gets + self.n_ranges + self.n_reverses +
        //
        self.n_sets + self.n_deletes
    }

    fn to_total(&self) -> usize {
        self.g.gets + self.g.ranges + self.g.reverses +
        //
        self.g.sets + self.g.deletes
    }
}

impl<K, V> Iterator for IncrementalLoad<K, V>
where
    K: Clone + Default + RandomKV,
    V: Clone + Default + RandomKV,
{
    type Item = Cmd<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.pop() {
            Some(item) => Some(item),
            None if self.to_n_total() == 0 => {
                self.log();
                None
            }
            None => {
                let a = self.n_gets + self.n_ranges + self.n_reverses;
                let b = a + self.n_sets;

                let start = time::SystemTime::now();
                let n = cmp::min(self.to_n_total(), self.g.channel_size);
                for _ in 0..n {
                    let r: usize = self.rng.gen::<usize>() % self.to_n_total();
                    let cmd = if r < self.n_gets {
                        self.n_gets -= 1;
                        Cmd::gen_get(&mut self.rng, &self.g)
                    } else if r < (self.n_gets + self.n_ranges) {
                        self.n_ranges -= 1;
                        Cmd::gen_range(&mut self.rng, &self.g)
                    } else if r < a {
                        self.n_reverses -= 1;
                        Cmd::gen_reverse(&mut self.rng, &self.g)
                    } else if r < b {
                        self.n_sets -= 1;
                        Cmd::gen_set(&mut self.rng, &self.g)
                    } else if r < self.to_n_total() {
                        self.n_deletes -= 1;
                        Cmd::gen_del(&mut self.rng, &self.g)
                    } else {
                        unreachable!();
                    };
                    self.items.push(cmd);
                }
                self.elapsed += start.elapsed().unwrap();
                self.items.pop()
            }
        }
    }
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

impl RandomKV for u64 {
    fn gen_key(&self, rng: &mut SmallRng, g: &GenOptions) -> u64 {
        let limit = (g.loads * std::cmp::max(g.initial, 1)) as u64;
        rng.gen::<u64>() % limit
    }

    fn gen_val(&self, rng: &mut SmallRng, _g: &GenOptions) -> u64 {
        rng.gen()
    }

    fn next(&self, _g: &GenOptions) -> u64 {
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

pub struct IterKeys<K>
where
    K: Clone + Default + RandomKV,
{
    key: K,
    rng: SmallRng,
    g: GenOptions,
}

impl<K> IterKeys<K>
where
    K: Clone + Default + RandomKV,
{
    #[allow(dead_code)]
    pub(crate) fn new(g: &GenOptions) -> IterKeys<K> {
        let rng = SmallRng::from_seed(g.seed.to_le_bytes());
        IterKeys {
            key: Default::default(),
            rng,
            g: g.clone(),
        }
    }
}

impl<K> Iterator for IterKeys<K>
where
    K: Clone + Default + RandomKV,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.key.gen_key(&mut self.rng, &self.g))
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
