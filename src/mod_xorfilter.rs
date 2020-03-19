use log::{debug, info};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::time::{Duration, SystemTime};
use xorfilter::Xor8;

use std::fs;

use crate::Profile;

pub fn perf(p: Profile) -> Result<(), String> {
    if p.g.loads == 0 {
        return Ok(());
    }

    let mut rng = SmallRng::from_seed(p.g.seed.to_le_bytes());

    let (keys, elapsed) = {
        let start = SystemTime::now();

        let mut keys: Vec<u64> = vec![];
        for _ in 0..p.g.loads {
            keys.push(rng.gen());
        }
        (
            keys,
            Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64),
        )
    };
    debug!(
        target: "xorf  ", "Took {:?} to generate {} keys, {:?} per key",
        elapsed, keys.len(), elapsed/(keys.len() as u32)
    );

    let mut filter = Xor8::new();
    filter.populate_keys(&keys);
    let elapsed = {
        let start = SystemTime::now();
        filter.build();
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };
    info!(
        target: "xorf  ", "Took {:?} to build {} keys, {:?} per key",
        elapsed, keys.len(), elapsed/(keys.len() as u32)
    );

    {
        let fpath = {
            let mut fpath = std::env::temp_dir();
            fpath.push("ixperf-xorfilter");
            fpath.into_os_string()
        };
        filter.write_file(&fpath).unwrap();
        let n = fs::metadata(&fpath).unwrap().len();
        let bpv = (n as f64) * 8.0 / (p.g.loads as f64);
        fs::remove_file(&fpath).ok();
        info!(target: "xorf  ", "bits per entry, {} bits", bpv);
    }

    let elapsed = {
        let start = SystemTime::now();
        for _i in 0..p.g.gets {
            let off: usize = rng.gen::<usize>() % keys.len();
            filter.contains_key(keys[off]);
        }
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };
    info!(
        target: "xorf  ",
        "Took {:?} to check {} keys, {:?} per key",
        elapsed, keys.len(), elapsed / (keys.len() as u32)
    );

    Ok(())
}
