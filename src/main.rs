mod latency;
mod llrb_u64;
mod opts;
mod ordmap_u64;

use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use opts::{Cmd, Opt};

fn main() {
    let mut opt = Opt::new();
    make_seed(&mut opt);
    println!("starting with seed = {}", opt.cmdopt.seed);

    match opt.cmdopt.index.as_str() {
        "ordmap" => do_ordmap_u64(&opt),
        "llrb" => do_llrb_u64(&opt),
        index @ _ => panic!("invalid index {}", index),
    }
}

fn do_ordmap_u64(opt: &Opt) {
    if opt.cmdopt.load > 0 {
        let (tx, rx) = mpsc::channel();
        let newopt = opt.clone();
        let loader = thread::spawn(move || initial_generator_u64(newopt, tx));

        match opt.cmdopt.ktype.as_str() {
            "u64" => {
                use crate::ordmap_u64::{do_initial_u64, SharedOrdMap};

                do_initial_u64(&opt, SharedOrdMap::new(), rx);
            }
            v @ _ => panic!("{} not supported", v),
        }

        loader.join().unwrap();
    }
}

fn do_llrb_u64(opt: &Opt) {
    if opt.cmdopt.load > 0 {
        let (tx, rx) = mpsc::channel();
        let newopt = opt.clone();
        let loader = thread::spawn(move || initial_generator_u64(newopt, tx));

        match opt.cmdopt.ktype.as_str() {
            "u64" => {
                use crate::llrb_u64::do_initial_u64;
                use llrb_index::Llrb;

                do_initial_u64(&opt, Llrb::new("ixperf"), rx);
            }
            v @ _ => panic!("{} not supported", v),
        }

        loader.join().unwrap();
    }
}

fn initial_generator_u64(opt: Opt, tx: mpsc::Sender<Cmd<u64>>) {
    let start = SystemTime::now();
    let mut rng = SmallRng::from_seed(opt.cmdopt.seed.to_le_bytes());
    for _ in 0..opt.cmdopt.load {
        tx.send(Cmd::Load {
            key: rng.gen(),
            value: rng.gen(),
        })
        .unwrap();
    }
    let elapsed = start.elapsed().unwrap();
    println!(
        "initial generator: {} items in {:?}",
        opt.cmdopt.load, elapsed
    );
}

//fn initial_generator_u64(opt: Opt, tx: mpsc::Sender<Cmd<u64>>) {
//    let start = SystemTime::now();
//    let mut rng = SmallRng::from_seed(opt.cmdopt.seed.to_le_bytes());
//    for _ in 0..opt.cmdopt.load {
//        tx.send(Cmd::Load {
//            key: rng.gen(),
//            value: rng.gen(),
//        })
//        .unwrap();
//    }
//    let elapsed = start.elapsed().unwrap();
//    println!("generated {} items in {:?}", opt.cmdopt.load, elapsed);
//}

fn make_seed(opt: &mut Opt) -> u128 {
    if opt.cmdopt.seed == 0 {
        opt.cmdopt.seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
    };
    opt.cmdopt.seed
}
