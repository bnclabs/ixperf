use log::info;
use rand::{rngs::SmallRng, SeedableRng};

use rdms::{
    self,
    core::{Diff, Footprint, Validate},
    mvcc::{Mvcc, Stats as MvccStats},
};

use std::{
    convert::{TryFrom, TryInto},
    fmt,
    hash::Hash,
};

use crate::generator::Cmd;
use crate::generator::RandomKV;
use crate::mod_rdms;
use crate::stats;
use crate::Profile;

#[derive(Default, Clone)]
pub struct MvccOpt {
    lsm: bool,
    sticky: bool,
    spin: bool,
}

impl TryFrom<toml::Value> for MvccOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut mvcc_opt: MvccOpt = Default::default();

        let section = match &value.get("rdms-mvcc") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "lsm" => mvcc_opt.lsm = value.as_bool().unwrap(),
                "sticky" => mvcc_opt.sticky = value.as_bool().unwrap(),
                "spin" => mvcc_opt.spin = value.as_bool().unwrap(),
                _ => panic!("invalid profile parameter {}", name),
            }
        }
        Ok(mvcc_opt)
    }
}

impl MvccOpt {
    fn new<K, V>(&self, name: &str) -> Box<Mvcc<K, V>>
    where
        K: Clone + Ord,
        V: Clone + Diff,
    {
        let mut index = if self.lsm {
            Mvcc::new_lsm(name)
        } else {
            Mvcc::new(name)
        };
        index.set_sticky(self.sticky).unwrap();
        index.set_spinlatch(self.spin).unwrap();
        index
    }
}

pub(crate) fn perf<K, V>(name: &str, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + fmt::Debug + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    <V as Diff>::D: Send,
{
    info!(target: "ixperf", "for type <{},{}>", p.key_type, p.val_type);
    let mvcc_index = p.rdms_mvcc.new(name);
    let mut index = rdms::Rdms::new(name, mvcc_index).unwrap();

    let fstats = mod_rdms::do_perf::<K, V, Box<Mvcc<K, V>>>(&mut index, &p);

    let istats = index.validate().unwrap();
    info!(target: "ixperf", "rdms mvcc stats\n{}", istats);
    validate_mvcc::<K, V>(&istats, &fstats, &p);
}

fn validate_mvcc<K, V>(stats: &MvccStats, fstats: &stats::Ops, p: &Profile)
where
    K: Clone + Ord + Default + Footprint + fmt::Debug + RandomKV,
    V: Clone + Diff + Default + Footprint + RandomKV,
{
    if p.rdms_mvcc.lsm || p.rdms_mvcc.sticky {
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

    assert_eq!(stats.rw_latch.write_locks, fstats.to_total_writes());
    if fstats.to_total_reads() == 0 || fstats.to_total_writes() == 0 {
        assert_eq!(stats.rw_latch.conflicts, 0);
    }

    assert_eq!(stats.snapshot_latch.read_locks, fstats.to_total_reads() + 3);
    assert_eq!(stats.snapshot_latch.write_locks, fstats.to_total_writes());
    if fstats.to_total_reads() == 0 || fstats.to_total_writes() == 0 {
        assert_eq!(stats.snapshot_latch.conflicts, 0);
    }

    if p.rdms_mvcc.lsm == false {
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
