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
    let keys = generate_keys(&p, &mut rng);

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

fn generate_keys(p: &Profile, rng: &mut SmallRng) -> Vec<u64> {
    let mut keys = vec![];
    let elapsed = {
        let start = SystemTime::now();

        let mut lookup: Vec<u8> = Vec::with_capacity((p.g.loads / 8) + 1);
        lookup.resize(lookup.capacity(), Default::default());
        for _ in 0..p.g.loads {
            let key = rng.gen::<u64>() % p.g.loads as u64;
            let (off, bit_off) = ((key / 8) as usize, (key % 8));
            if (lookup[off] & (1_u8 << bit_off)) == 0 {
                keys.push(key);
                lookup[off] = lookup[off] | (1_u8 << bit_off);
            }
        }
        let mut key = 0;
        for m in lookup.into_iter() {
            for i in 0..8 {
                if (m & (1_u8 << i)) == 0 {
                    keys.push(key);
                }
                key += 1;
            }
        }
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    debug!(
        target: "xorf  ", "Took {:?} to generate {} keys, {:?} per key",
        elapsed, keys.len(), elapsed/(keys.len() as u32)
    );

    assert!(keys.len() >= p.g.loads);
    let mut lookup: Vec<u8> = Vec::with_capacity((p.g.loads / 8) + 1);
    lookup.resize(lookup.capacity(), Default::default());
    for key in keys.iter() {
        let (off, bit_off) = ((key / 8) as usize, (key % 8));
        assert!((lookup[off] & (1_u8 << bit_off)) == 0, "key:{}", key);
        lookup[off] = lookup[off] | (1_u8 << bit_off);
    }
    for key in 0..p.g.loads {
        let (off, bit_off) = ((key / 8) as usize, (key % 8));
        assert!((lookup[off] & (1_u8 << bit_off)) != 0, "key:{}", key)
    }

    keys.truncate(p.g.loads);
    keys
}

#[cfg(test)]
#[path = "mod_xorfilter_test.rs"]
mod mod_xorfilter_test;
