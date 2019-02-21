use std::fmt::{self, Display};
use std::ops::Bound;

use rand::{rngs::SmallRng, Rng};
use structopt::StructOpt;

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

    #[structopt(long = "key-size", default_value = "16")]
    pub keysize: usize,

    #[structopt(long = "val-size", default_value = "16")]
    pub valsize: usize,

    #[structopt(long = "working-set", default_value = "1.0")]
    pub working_set: f64,

    #[structopt(long = "load", default_value = "1000000")]
    pub load: usize,

    #[structopt(long = "lsm")]
    pub lsm: bool,

    #[structopt(long = "seed", default_value = "0")]
    pub seed: u128,

    #[structopt(long = "readers", default_value = "1")]
    pub readers: usize,

    #[structopt(long = "create", default_value = "1000000")]
    pub creates: usize,

    #[structopt(long = "sets", default_value = "1000000")]
    pub sets: usize,

    #[structopt(long = "deletes", default_value = "1000000")]
    pub deletes: usize,

    #[structopt(long = "gets", default_value = "1000000")]
    pub gets: usize,

    #[structopt(long = "iters", default_value = "1000000")]
    pub iters: usize,

    #[structopt(long = "ranges", default_value = "1000000")]
    pub ranges: usize,

    #[structopt(long = "revrs", default_value = "1000000")]
    pub revrs: usize,
}

impl Opt {
    pub fn new() -> Opt {
        Opt::from_args()
    }

    pub fn gen_key(&self, rng: &mut SmallRng) -> Vec<u8> {
        let mut key: Vec<u8> = Vec::with_capacity(self.keysize);
        key.resize(self.keysize, 0);
        let key_slice: &mut [u8] = key.as_mut();
        rng.fill(key_slice);
        key
    }

    pub fn gen_value(&mut self, rng: &mut SmallRng) -> Vec<u8> {
        let mut val: Vec<u8> = Vec::with_capacity(self.valsize);
        val.resize(self.keysize, 0);
        let val_slice: &mut [u8] = val.as_mut();
        rng.fill(val_slice);
        val
    }

    pub fn init_load(&self) -> usize {
        self.load
    }

    pub fn incr_load(&self) -> usize {
        self.creates + self.sets + self.deletes + self.gets + self.iters + self.ranges + self.revrs
    }

    pub fn read_load(&self) -> usize {
        self.gets + self.iters + self.ranges + self.revrs
    }

    pub fn write_load(&self) -> usize {
        self.creates + self.sets + self.deletes
    }
}

pub enum Cmd {
    Load {
        key: Vec<u8>,
    },
    Create {
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
