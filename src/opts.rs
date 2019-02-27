use std::fmt::{self, Display};
use std::ops::Bound;

use rand::{rngs::SmallRng, Rng};
use structopt::StructOpt;

use crate::stats;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    TypeError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?})", self)
    }
}

#[derive(Debug, StructOpt, Clone)]
pub struct Opt {
    pub index: String,

    #[structopt(long = "path", default_value = "/tmp/ixperf")]
    pub path: String,

    #[structopt(long = "key-size", default_value = "16")]
    pub keysize: usize,

    #[structopt(long = "val-size", default_value = "16")]
    pub valsize: usize,

    #[structopt(long = "working-set", default_value = "1.0")]
    pub working_set: f64,

    #[structopt(long = "json")]
    pub json: bool,

    #[structopt(long = "lsm")]
    pub lsm: bool,

    #[structopt(long = "seed", default_value = "0")]
    pub seed: u128,

    #[structopt(long = "readers", default_value = "1")]
    pub readers: usize,

    #[structopt(long = "load", default_value = "10000000")]
    pub load: usize,

    #[structopt(long = "sets", default_value = "1000000")]
    pub sets: usize,

    #[structopt(long = "deletes", default_value = "1000000")]
    pub deletes: usize,

    #[structopt(long = "gets", default_value = "1000000")]
    pub gets: usize,

    #[structopt(long = "iters", default_value = "0")]
    pub iters: usize,

    #[structopt(long = "ranges", default_value = "0")]
    pub ranges: usize,

    #[structopt(long = "revrs", default_value = "0")]
    pub revrs: usize,
}

impl Opt {
    pub fn new() -> Opt {
        Opt::from_args()
    }

    //pub fn gen_key(&self, rng: &mut SmallRng) -> Vec<u8> {
    //    let mut key: Vec<u8> = Vec::with_capacity(self.keysize);
    //    key.resize(self.keysize, 0);
    //    let key_slice: &mut [u8] = key.as_mut();
    //    rng.fill(key_slice);
    //    key
    //}

    pub fn gen_key(&self, rng: &mut SmallRng) -> Vec<u8> {
        if self.keysize <= 20 {
            self.gen_key32(rng)
        } else {
            self.gen_key64(rng)
        }
    }

    pub fn gen_key32(&self, rng: &mut SmallRng) -> Vec<u8> {
        let m = self.load as u32;
        let mut key_print = [b'0'; 1024];
        let key = &mut key_print[..self.keysize];
        let key_num = (rng.gen::<u32>() % m).to_string().into_bytes();
        (&mut key[(self.keysize - key_num.len())..]).copy_from_slice(&key_num);
        key.to_vec()
    }

    pub fn gen_key64(&self, rng: &mut SmallRng) -> Vec<u8> {
        let m = self.load as u64;
        let mut key_print = [b'0'; 1024];
        let key = &mut key_print[..self.keysize];
        let key_num = (rng.gen::<u64>() % m).to_string().into_bytes();
        (&mut key[(self.keysize - key_num.len())..]).copy_from_slice(&key_num);
        key.to_vec()
    }

    #[allow(dead_code)]
    pub fn gen_value(&mut self, rng: &mut SmallRng) -> Vec<u8> {
        let mut val: Vec<u8> = Vec::with_capacity(self.valsize);
        val.resize(self.valsize, 0);
        let val_slice: &mut [u8] = val.as_mut();
        rng.fill(val_slice);
        val
    }

    pub fn init_load(&self) -> usize {
        self.load
    }

    #[allow(dead_code)]
    pub fn incr_load(&self) -> usize {
        self.sets + self.deletes + self.gets + self.iters + self.ranges + self.revrs
    }

    pub fn read_load(&self) -> usize {
        self.gets + self.iters + self.ranges + self.revrs
    }

    pub fn write_load(&self) -> usize {
        self.sets + self.deletes
    }

    pub fn periodic_log(&self, op_stats: &stats::Ops, fin: bool) {
        if self.json {
            println!("{}", op_stats.json());
        } else {
            op_stats.pretty_print("", fin);
        }
    }
}

pub enum Cmd {
    Load {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    Get {
        key: Vec<u8>,
    },
    Iter,
    Range {
        low: Bound<Vec<u8>>,
        high: Bound<Vec<u8>>,
    },
    Reverse {
        low: Bound<Vec<u8>>,
        high: Bound<Vec<u8>>,
    },
}
