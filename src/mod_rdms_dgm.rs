use log::info;

use rdms::{
    self,
    core::{Diff, DiskIndexFactory, Footprint, Serialize, WriteIndexFactory},
    core::{Index, Validate},
    croaring::CRoaring,
    dgm,
};

use std::{
    convert::{TryFrom, TryInto},
    ffi, fmt,
    hash::Hash,
    time,
};

use crate::generator::RandomKV;
use crate::mod_rdms;
use crate::Profile;

#[derive(Default, Clone)]
pub struct DgmOpt {
    dir: ffi::OsString,
    mem_index: String,
    disk_index: String,
    lsm: bool,
    m0_limit: Option<usize>,
    mem_ratio: f64,
    disk_ratio: f64,
    commit_interval: Option<time::Duration>,
    compact_interval: Option<time::Duration>,
}

impl TryFrom<toml::Value> for DgmOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut opt: DgmOpt = Default::default();

        let section = match &value.get("rdms-dgm") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "dir" => {
                    let dir: &ffi::OsStr = value.as_str().unwrap().as_ref();
                    opt.dir = dir.to_os_string();
                }
                "mem_index" => {
                    let s = value.as_str().unwrap();
                    opt.mem_index = s.to_string();
                }
                "disk_index" => {
                    let s = value.as_str().unwrap();
                    opt.disk_index = s.to_string();
                }
                "lsm" => opt.lsm = value.as_bool().unwrap(),
                "m0_limit" => {
                    let m0_limit = value.as_integer().unwrap();
                    opt.m0_limit = Some(m0_limit.try_into().unwrap());
                }
                "mem_ratio" => opt.mem_ratio = value.as_float().unwrap(),
                "disk_ratio" => opt.disk_ratio = value.as_float().unwrap(),
                "commit_interval" => {
                    let ci = value.as_integer().unwrap() as u64;
                    opt.commit_interval = Some(time::Duration::from_secs(ci));
                }
                "compact_interval" => {
                    let ci = value.as_integer().unwrap() as u64;
                    opt.compact_interval = Some(time::Duration::from_secs(ci));
                }
                _ => panic!("invalid profile parameter {}", name),
            }
        }
        Ok(opt)
    }
}

impl DgmOpt {
    fn new<K, V, M, D>(&self, name: &str, m: M, d: D) -> Box<dgm::Dgm<K, V, M, D>>
    where
        K: 'static + Send + Clone + Ord + Serialize + Footprint,
        V: 'static + Send + Clone + Diff + Serialize + Footprint,
        <V as Diff>::D: Send + Serialize,
        M: 'static + Send + WriteIndexFactory<K, V>,
        D: 'static + Send + DiskIndexFactory<K, V>,
        M::I: 'static + Send,
        D::I: 'static + Send,
        <M::I as Index<K, V>>::R: 'static + Send,
        <M::I as Index<K, V>>::W: 'static + Send,
        <D::I as Index<K, V>>::R: 'static + Send,
        <D::I as Index<K, V>>::W: 'static + Send,
    {
        let mut config: dgm::Config = Default::default();
        config.set_lsm(self.lsm);
        if let Some(m0_limit) = self.m0_limit {
            config.set_m0_limit(m0_limit);
        }
        config
            .set_mem_ratio(self.mem_ratio)
            .set_disk_ratio(self.disk_ratio);
        if let Some(ci) = self.commit_interval {
            config.set_commit_interval(ci);
        }
        if let Some(ci) = self.compact_interval {
            config.set_compact_interval(ci);
        }

        dgm::Dgm::new(&self.dir, name, m, d, config).unwrap()
    }
}

pub(crate) fn perf<K, V>(name: &str, p: Profile)
where
    K: 'static
        + Clone
        + Default
        + Send
        + Sync
        + Ord
        + Serialize
        + Footprint
        + fmt::Debug
        + RandomKV
        + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Serialize + Footprint + RandomKV,
    <V as Diff>::D: Send + Default + Serialize,
{
    let m = p.rdms_dgm.mem_index.clone();
    let d = p.rdms_dgm.disk_index.clone();
    let istats = match (m.as_str(), d.as_str(), p.rdms_robt.bitmap.as_str()) {
        ("llrb", "robt", "croaring") => {
            let mut index = {
                let m = p.rdms_llrb.new_factory::<K, V>(name);
                let d = p.rdms_robt.new_factory::<K, V, CRoaring>(name);
                rdms::Rdms::new(name, p.rdms_dgm.new(name, m, d)).unwrap()
            };
            let _fstats = mod_rdms::do_perf::<K, V, _>(&mut index, &p);
            index.validate().unwrap()
        }
        _ => unreachable!(),
    };

    info!(target: "ixperf", "rdms shllrb stats\n{}", istats);
}
