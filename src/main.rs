mod generator;
mod latency;
mod mod_llrb;
//mod mod_lmdb;
mod stats;

use std::time::{SystemTime, UNIX_EPOCH};

use structopt::StructOpt;

const NUM_GENERATORS: usize = 1;
const LOG_BATCH: usize = 1_000_000;

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

fn main() {
    let mut opt = Opt::new();
    make_seed(&mut opt);
    println!("starting with seed = {}", opt.seed);

    //match (opt.index.as_str(), opt.typ.as_str()) {
    //    ("llrb", "u32") => mod_llrb::perf::<u32>(opt),
    //    ("llrb", "u64") => mod_llrb::perf::<u64>(opt),
    //    ("llrb", "array") => mod_llrb::perf::<[u8; 32]>(opt),
    //    ("llrb", "bytes") => mod_llrb::perf::<Vec<u8>>(opt),
    //    //"lmdb" => mod_lmdb::perf(opt),
    //    _ => panic!("unsupported inded/type {}/{}", opt.index, opt.typ),
    //}
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
