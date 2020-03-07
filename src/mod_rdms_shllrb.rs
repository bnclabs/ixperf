use log::info;
use rand::{rngs::SmallRng, SeedableRng};

use rdms::{
    self,
    core::{Diff, Footprint, Validate},
    llrb::Stats as LlrbStats,
    shllrb,
};

use std::{
    convert::{TryFrom, TryInto},
    fmt,
    hash::Hash,
    time,
};

use crate::generator::Cmd;
use crate::generator::RandomKV;
use crate::mod_rdms;
use crate::stats;
use crate::Profile;

#[derive(Default, Clone)]
pub struct ShllrbOpt {
    lsm: bool,
    sticky: bool,
    spin: bool,
    interval: i64,
    max_shards: i64,
    max_entries: i64,
}

impl TryFrom<toml::Value> for ShllrbOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut opt: ShllrbOpt = Default::default();

        let section = match &value.get("rdms-shllrb") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "lsm" => opt.lsm = value.as_bool().unwrap(),
                "sticky" => opt.sticky = value.as_bool().unwrap(),
                "spin" => opt.spin = value.as_bool().unwrap(),
                "interval" => opt.interval = value.as_integer().unwrap(),
                "max_shards" => opt.max_shards = value.as_integer().unwrap(),
                "max_entries" => opt.max_entries = value.as_integer().unwrap(),
                _ => panic!("invalid profile parameter {}", name),
            }
        }
        Ok(opt)
    }
}

impl ShllrbOpt {
    fn new<K, V>(&self, name: &str) -> Box<shllrb::ShLlrb<K, V>>
    where
        K: 'static + Send + Clone + Ord + Footprint,
        V: 'static + Send + Clone + Diff + Footprint,
        <V as Diff>::D: Send,
    {
        let mut config: shllrb::Config = Default::default();
        config
            .set_lsm(self.lsm)
            .set_sticky(self.sticky)
            .set_spinlatch(self.spin)
            .set_shard_config(self.max_shards as usize, self.max_entries as usize)
            .set_interval(time::Duration::from_secs(self.interval as u64));
        shllrb::ShLlrb::new(name, config)
    }
}

pub(crate) fn perf<K, V>(name: &str, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + fmt::Debug + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    <V as Diff>::D: Send,
{
    let index = p.rdms_shllrb.new(name);
    let mut index = rdms::Rdms::new(name, index).unwrap();

    let fstats = mod_rdms::do_perf::<K, V, Box<shllrb::ShLlrb<K, V>>>(&mut index, &p);

    let istats = index.validate().unwrap();
    info!(target: "ixperf", "rdms shllrb stats\n{}", istats);
    validate_shllrb::<K, V>(&istats, &fstats, &p);
}

fn validate_shllrb<K, V>(stats: &LlrbStats, fstats: &stats::Ops, p: &Profile)
where
    K: Clone + Ord + Default + Footprint + fmt::Debug + RandomKV,
    V: Clone + Diff + Default + Footprint + RandomKV,
{
    if p.rdms_shllrb.lsm || p.rdms_shllrb.sticky {
        let expected_entries = (fstats.load.count - fstats.load.items)
            + (fstats.set.count - fstats.set.items)
            + fstats.delete.items;
        assert_eq!(stats.entries, expected_entries);
    } else {
        let expected_entries = (fstats.load.count - fstats.load.items)
            + (fstats.set.count - fstats.set.items)
            - (fstats.delete.count - fstats.delete.items);
        assert_eq!(stats.entries, expected_entries);
    }

    // assert_eq!(stats.rw_latch.read_locks, fstats.to_total_reads() + 3);
    // assert_eq!(stats.rw_latch.write_locks, fstats.to_total_writes());
    if fstats.to_total_reads() == 0 || fstats.to_total_writes() == 0 {
        assert_eq!(stats.rw_latch.conflicts, 0);
    }

    if p.rdms_shllrb.lsm == false {
        let mut rng = SmallRng::from_seed(p.g.seed.to_le_bytes());
        let (kfp1, kfp2, vfp) = match Cmd::<K, V>::gen_load(&mut rng, &p.g) {
            Cmd::Load { key, value } => (
                std::mem::size_of::<K>() + (key.footprint().unwrap() as usize),
                key.footprint().unwrap() as usize,
                std::mem::size_of::<V>() + (value.footprint().unwrap() as usize),
            ),
            _ => unreachable!(),
        };
        let entries = stats.entries;

        let key_footprint: isize = ((kfp1 + kfp2) * entries).try_into().unwrap();
        assert_eq!(stats.key_footprint, key_footprint);

        let mut tree_footprint: isize = ((stats.node_size + kfp2 + vfp) * entries)
            .try_into()
            .unwrap();
        tree_footprint -= (vfp * stats.n_deleted) as isize; // for sticky mode.
        assert_eq!(stats.tree_footprint, tree_footprint);
    }
}
