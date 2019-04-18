use std::ops::Bound;

use rand::{rngs::SmallRng, Rng};
use structopt::StructOpt;

use crate::generator::RandomKV;
use crate::stats;

#[derive(Debug, StructOpt, Clone)]
pub struct Opt {
    pub index: String,

    #[structopt(long = "path", default_value = "/tmp/ixperf")]
    pub path: String,

    #[structopt(long = "type", default_value = "u64")]
    pub typ: String,

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

pub enum Cmd<K> {
    Load { key: K, value: K },
    Set { key: K, value: K },
    Delete { key: K },
    Get { key: K },
    Iter,
    Range { low: Bound<K>, high: Bound<K> },
    Reverse { low: Bound<K>, high: Bound<K> },
}

impl<K> Cmd<K>
where
    K: RandomKV,
{
    pub fn generate_load(rng: &mut SmallRng, opt: &Opt, k: &K) -> Cmd<K> {
        Cmd::Load {
            key: k.generate_key(rng, opt),
            value: k.generate_value(rng, opt),
        }
    }

    pub fn generate_set(rng: &mut SmallRng, opt: &Opt, k: &K) -> Cmd<K> {
        Cmd::Set {
            key: k.generate_key(rng, opt),
            value: k.generate_value(rng, opt),
        }
    }

    pub fn generate_delete(rng: &mut SmallRng, opt: &Opt, k: &K) -> Cmd<K> {
        Cmd::Delete {
            key: k.generate_key(rng, opt),
        }
    }

    pub fn generate_get(rng: &mut SmallRng, opt: &Opt, k: &K) -> Cmd<K> {
        Cmd::Get {
            key: k.generate_key(rng, opt),
        }
    }

    pub fn generate_iter(_rng: &mut SmallRng, _opt: &Opt, _k: &K) -> Cmd<K> {
        Cmd::Iter
    }

    pub fn generate_range(rng: &mut SmallRng, opt: &Opt, k: &K) -> Cmd<K> {
        let low = bounded_key(k.generate_key(rng, opt), rng);
        let high = bounded_key(k.generate_key(rng, opt), rng);
        Cmd::Range { low, high }
    }

    pub fn generate_reverse(rng: &mut SmallRng, opt: &Opt, k: &K) -> Cmd<K> {
        let low = bounded_key(k.generate_key(rng, opt), rng);
        let high = bounded_key(k.generate_key(rng, opt), rng);
        Cmd::Reverse { low, high }
    }
}

fn bounded_key<T>(key: T, rng: &mut SmallRng) -> Bound<T> {
    match rng.gen::<u8>() % 3 {
        0 => Bound::Included(key),
        1 => Bound::Excluded(key),
        2 => Bound::Unbounded,
        _ => unreachable!(),
    }
}
