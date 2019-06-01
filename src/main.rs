mod generator;
mod latency;
mod mod_llrb;
//mod mod_lmdb;
mod opts;
mod stats;

use std::time::{SystemTime, UNIX_EPOCH};

use opts::Opt;

const NUM_GENERATORS: usize = 1;
const LOG_BATCH: usize = 1_000_000;

fn main() {
    let mut opt = Opt::new();
    make_seed(&mut opt);
    println!("starting with seed = {}", opt.seed);

    match (opt.index.as_str(), opt.typ.as_str()) {
        ("llrb", "u32") => mod_llrb::perf::<u32>(opt),
        ("llrb", "u64") => mod_llrb::perf::<u64>(opt),
        ("llrb", "array") => mod_llrb::perf::<[u8; 32]>(opt),
        ("llrb", "bytes") => mod_llrb::perf::<Vec<u8>>(opt),
        //"lmdb" => mod_lmdb::perf(opt),
        _ => panic!("unsupported inded/type {}/{}", opt.index, opt.typ),
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
