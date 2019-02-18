mod lib;
mod type_u64;

use std::time::{SystemTime, UNIX_EPOCH};

use lib::Opt;

fn main() {
    let mut opt = Opt::new();
    make_seed(&mut opt);
    println!("starting with seed = {}", opt.cmdopt.seed);

    match opt.cmdopt.ktype.as_str() {
        "u64" => {
            use type_u64::initial_index;
            initial_index(&opt);
        }
        v @ _ => panic!("{} not supported", v),
    }
}

fn make_seed(opt: &mut Opt) -> u128 {
    if opt.cmdopt.seed == 0 {
        opt.cmdopt.seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
    };
    opt.cmdopt.seed
}
