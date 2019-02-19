mod generator;
mod index_llrb;
mod index_ordmap;
mod latency;
mod opts;

use std::time::{SystemTime, UNIX_EPOCH};

use opts::Opt;

const NUM_GENERATORS: usize = 4;

fn main() {
    let mut opt = Opt::new();
    make_seed(&mut opt);
    println!("starting with seed = {}", opt.seed);

    match opt.index.as_str() {
        "ordmap" => index_ordmap::perf(opt),
        "llrb" => index_llrb::perf(opt),
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
