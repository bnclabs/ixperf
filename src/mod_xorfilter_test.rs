use rand::{random, rngs::SmallRng, SeedableRng};

use crate::Profile;

use super::*;

#[test]
fn test_generate_keys() {
    let seed: i128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut p: Profile = Default::default();
    for i in 0..100 {
        p.g.loads = i;
        generate_keys(&p, &mut rng);
    }
}
