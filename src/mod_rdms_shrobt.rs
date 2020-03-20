use log::info;
use rand::{rngs::SmallRng, Rng, SeedableRng};

use rdms::{
    self,
    core::{
        Bloom, CommitIter, Cutoff, Diff, DiskIndexFactory, Entry, Footprint, Index, Reader,
        Serialize, Validate, Writer,
    },
    llrb::Llrb,
    robt,
    shrobt::{self, ShRobt, ShrobtFactory},
};

use std::{
    convert::{TryFrom, TryInto},
    ffi, fmt,
    hash::Hash,
    ops::Bound,
    thread,
    time::{Duration, SystemTime},
};

use crate::generator::RandomKV;
use crate::generator::{Cmd, IncrementalWrite};
use crate::mod_rdms;
use crate::stats;
use crate::Profile;

#[derive(Default, Clone)]
pub struct ShrobtOpt {
    pub num_shards: usize,

    pub dir: ffi::OsString,
    pub z_blocksize: usize,
    pub m_blocksize: usize,
    pub v_blocksize: usize,
    pub delta_ok: bool,
    pub vlog_file: Option<ffi::OsString>,
    pub value_in_vlog: bool,
    pub flush_queue_size: usize,

    pub mmap: bool,
    pub bitmap: String,
}

impl TryFrom<toml::Value> for ShrobtOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut opt: ShrobtOpt = Default::default();

        let section = match &value.get("rdms-shrobt") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "num_shards" => {
                    opt.num_shards = value.as_integer().unwrap().try_into().unwrap(),
                }
                "dir" => {
                    let dir: &ffi::OsStr = value.as_str().unwrap().as_ref();
                    opt.dir = dir.to_os_string();
                }
                "z_blocksize" => opt.z_blocksize = value.as_integer().unwrap().try_into().unwrap(),
                "m_blocksize" => opt.m_blocksize = value.as_integer().unwrap().try_into().unwrap(),
                "v_blocksize" => opt.v_blocksize = value.as_integer().unwrap().try_into().unwrap(),
                "delta_ok" => opt.delta_ok = value.as_bool().unwrap(),
                "vlog_file" if value.as_str().unwrap() == "" => opt.vlog_file = None,
                "vlog_file" => {
                    let vlog_file: &ffi::OsStr = value.as_str().unwrap().as_ref();
                    opt.vlog_file = Some(vlog_file.to_os_string());
                }
                "value_in_vlog" => opt.value_in_vlog = value.as_bool().unwrap(),
                "flush_queue_size" => {
                    opt.flush_queue_size = value.as_integer().unwrap().try_into().unwrap()
                }
                "mmap" => opt.mmap = value.as_bool().unwrap(),
                "bitmap" => opt.bitmap = value.as_str().unwrap().to_string(),
                _ => panic!("invalid profile parameter {}", name),
            }
        }

        if opt.num_shards < 1 {
            Err(format!("invalid num_shards:{}", opt.num_shards));
        }  else {
            Ok(opt)
        }
    }
}

impl ShrobtOpt {
    fn new<K, V, B>(&self, name: &str) -> ShRobt<K, V, B>
    where
        K: 'static + Default + Clone + Ord + Send + Hash + Footprint + Serialize,
        V: 'static + Clone + Default + Send + Diff + Footprint + Serialize,
        <V as Diff>::D: Default + Serialize,
        B: 'static + Sync + Send + Bloom,
    {
        self.new_factory(name).new(&self.dir, name).unwrap()
    }

    pub(crate) fn new_factory<K, V, B>(&self, _name: &str) -> ShrobtFactory<K, V, B>
    where
        K: 'static + Default + Clone + Ord + Send + Hash + Footprint + Serialize,
        V: 'static + Clone + Default + Send + Diff + Footprint + Serialize,
        <V as Diff>::D: Default + Serialize,
        B: 'static + Send + Bloom,
    {
        let mut config: robt::Config = Default::default();
        config
            .set_blocksize(self.z_blocksize, self.m_blocksize, self.v_blocksize)
            .unwrap();
        config
            .set_delta(self.vlog_file.clone(), self.delta_ok)
            .unwrap();
        config
            .set_value_log(self.vlog_file.clone(), self.value_in_vlog)
            .unwrap();
        config.set_flush_queue_size(self.flush_queue_size).unwrap();

        shrobt::shrobt_factory(config, self.num_shards, self.mmap)
    }

    pub(crate) fn to_bitmap(&self) -> &str {
        self.bitmap.as_str()
    }
}

pub(crate) fn perf<K, V, B>(name: &str, mut p: Profile)
where
    K: 'static
        + Clone
        + Default
        + Send
        + Sync
        + Ord
        + Footprint
        + Serialize
        + fmt::Debug
        + RandomKV
        + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + Serialize + RandomKV,
    <V as Diff>::D: Send + Default + Serialize,
    B: 'static + Bloom + Send + Sync,
{
    let srindex = p.rdms_shrobt.new(name);
    let mut index = rdms::Rdms::new(name, srindex).unwrap();

    // load initial data.
    let mut fstats = stats::Ops::new();
    let mut rng = SmallRng::from_seed(p.g.seed.to_le_bytes());
    let mut seqno = 0;
    for i in 0..(p.g.loads / p.g.write_ops()) {
        let mut mem_index = if p.rdms_shrobt.delta_ok {
            Llrb::new_lsm("load-shrobt")
        } else {
            Llrb::new("load-shrobt")
        };
        mem_index.set_sticky(rng.gen::<bool>()).unwrap();
        mem_index.set_seqno(seqno).unwrap();
        p.g.seed += i as u128 * 100;
        let gen = IncrementalWrite::<K, V>::new(p.g.clone());
        let mut w = mem_index.to_writer().unwrap();
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Set { key, value } => {
                    fstats.set.sample_start(false);
                    let n = w.set(key, value.clone()).unwrap().map_or(0, |_| 1);
                    fstats.set.sample_end(n);
                }
                Cmd::Delete { key } => {
                    fstats.delete.sample_start(false);
                    let items = w.delete(&key).unwrap().map_or(1, |_| 0);
                    fstats.delete.sample_end(items);
                }
                _ => unreachable!(),
            };
        }
        seqno = mem_index.to_seqno().unwrap();
        std::mem::drop(w);
        index
            .commit(
                CommitIter::new(mem_index, (Bound::Unbounded, Bound::Included(seqno))),
                |meta| meta,
            )
            .unwrap();
    }

    let cutoff = Cutoff::new_lsm(Bound::Excluded(0));
    index.compact(cutoff).unwrap();

    // validate
    let mut r = index.to_reader().unwrap();
    validate_shrobt::<K, V, B>(&mut index, &mut r, &fstats, &p);

    // optional iteration
    let mut r = index.to_reader().unwrap();
    let (start, mut iter_count) = (SystemTime::now(), 0);
    if p.g.iters {
        for _ in r.iter().unwrap() {
            iter_count += 1
        }
    }
    let idur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);

    // concurrent readers
    let mut fstats = stats::Ops::new();
    let mut threads = vec![];
    for i in 0..p.rdms.readers {
        let r = index.to_reader().unwrap();
        let pr = p.clone();
        threads.push(thread::spawn(move || mod_rdms::do_read(i, r, pr)));
    }
    for t in threads {
        fstats.merge(&t.join().unwrap());
    }

    if p.g.iters {
        info!(
            target: "ixperf",
            "rdms took {:?} to iter over {} items", idur, iter_count
        );
    }
    info!(target: "ixperf", "concurrent stats\n{:?}", fstats);
}

fn validate_shrobt<K, V, B>(
    index: &mut rdms::Rdms<K, V, shrobt::ShRobt<K, V, B>>,
    r: &mut shrobt::ShrobtReader<K, V, B>,
    fstats: &stats::Ops,
    p: &Profile,
) where
    K: Clone + Ord + Default + Send + Hash + Footprint + Serialize + fmt::Debug + RandomKV,
    V: Clone + Send + Default + Diff + Footprint + Serialize + RandomKV,
    <V as Diff>::D: Default + Clone + Serialize,
    B: Send + Sync + Bloom,
{
    info!(target: "ixperf", "validating shrobt index ...");

    let stats: robt::Stats = index.validate().unwrap();
    if p.rdms_shrobt.delta_ok {
        let (mut n_muts, iter) = (0, r.iter_with_versions().unwrap());
        for entry in iter {
            let entry = entry.unwrap();
            let versions: Vec<Entry<K, V>> = entry.versions().collect();
            n_muts += versions.len();
        }
        assert_eq!(n_muts, fstats.to_total_writes());
    }

    let footprint: isize = (stats.m_bytes + stats.z_bytes + stats.v_bytes + stats.n_abytes)
        .try_into()
        .unwrap();
    let useful: isize =
        (stats.key_mem + stats.val_mem + stats.diff_mem + stats.n_abytes + stats.padding)
            .try_into()
            .unwrap();

    assert!(
        useful < footprint,
        "failed because useful:{} footprint:{}",
        useful,
        footprint
    )
}
