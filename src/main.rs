mod generator;
mod latency;
mod mod_bogn_llrb;
mod mod_llrb;
//mod mod_lmdb;
mod stats;

use std::time::{SystemTime, UNIX_EPOCH};

use structopt::StructOpt;

const LOG_BATCH: usize = 1_000_000;

#[derive(Debug, StructOpt, Clone)]
pub struct Opt {
    pub index: String,

    #[structopt(long = "path", default_value = "/tmp/ixperf")]
    pub path: String,

    #[structopt(long = "key-type", default_value = "i64")]
    pub key_type: String,

    #[structopt(long = "value-type", default_value = "i64")]
    pub val_type: String,

    #[structopt(long = "key-size", default_value = "16")]
    pub keysize: usize,

    #[structopt(long = "val-size", default_value = "16")]
    pub valsize: usize,

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

fn main() {
    let mut opt = Opt::new();
    make_seed(&mut opt);
    println!("starting with seed = {}", opt.seed);

    match (
        opt.index.as_str(),
        opt.key_type.as_str(),
        opt.val_type.as_str(),
    ) {
        ("llrb", "i32", "i32") => mod_llrb::perf::<i32, i32>(opt),
        ("llrb", "i32", "array") => mod_llrb::perf::<i32, [u8; 32]>(opt),
        ("llrb", "i32", "bytes") => mod_llrb::perf::<i32, Vec<u8>>(opt),
        ("llrb", "i64", "i64") => mod_llrb::perf::<i64, i64>(opt),
        ("llrb", "i64", "array") => mod_llrb::perf::<i64, [u8; 32]>(opt),
        ("llrb", "i64", "bytes") => mod_llrb::perf::<i64, Vec<u8>>(opt),
        ("llrb", "array", "array") => mod_llrb::perf::<[u8; 32], [u8; 32]>(opt),
        ("llrb", "array", "bytes") => mod_llrb::perf::<[u8; 32], Vec<u8>>(opt),
        ("llrb", "bytes", "bytes") => mod_llrb::perf::<Vec<u8>, Vec<u8>>(opt),
        ("bogn-llrb", "i32", "i32") => mod_bogn_llrb::perf::<i32, i32>(opt),
        // ("bogn-llrb", "i32", "array") => mod_bogn_llrb::perf::<i32, [u8; 32]>(opt),
        ("bogn-llrb", "i32", "bytes") => mod_bogn_llrb::perf::<i32, Vec<u8>>(opt),
        ("bogn-llrb", "i64", "i64") => mod_bogn_llrb::perf::<i64, i64>(opt),
        // ("bogn-llrb", "i64", "array") => mod_bogn_llrb::perf::<i64, [u8; 32]>(opt),
        ("bogn-llrb", "i64", "bytes") => mod_bogn_llrb::perf::<i64, Vec<u8>>(opt),
        // ("bogn-llrb", "array", "array") => mod_bogn_llrb::perf::<[u8; 32], [u8; 32]>(opt),
        // ("bogn-llrb", "array", "bytes") => mod_bogn_llrb::perf::<[u8; 32], Vec<u8>>(opt),
        ("bogn-llrb", "bytes", "bytes") => mod_bogn_llrb::perf::<Vec<u8>, Vec<u8>>(opt),
        //"lmdb" => mod_lmdb::perf(opt),
        _ => panic!(
            "unsupported inded/type {}/<{},{}>",
            opt.index, opt.key_type, opt.val_type
        ),
    }
}

fn make_seed(opt: &mut Opt) -> u128 {
    if opt.seed == 0 {
        opt.seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
    };
    opt.seed
}
