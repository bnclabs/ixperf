mod generator;
mod latency;
mod mod_llrb;
mod mod_lmdb;
mod opts;
mod stats;

use std::time::{SystemTime, UNIX_EPOCH};

use opts::Opt;

const NUM_GENERATORS: usize = 4;
const LOG_BATCH: usize = 1_000_000;

fn main() {
    let mut opt = Opt::new();
    make_seed(&mut opt);
    println!("starting with seed = {}", opt.seed);

    match opt.index.as_str() {
        "llrb" => mod_llrb::perf(opt),
        "lmdb" => mod_lmdb::perf(opt),
        index @ _ => panic!("invalid index {}", index),
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
