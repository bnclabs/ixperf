use log::info;
use rand::{rngs::SmallRng, Rng, SeedableRng};

use rdms::{
    self,
    core::{
        Bloom, CommitIter, Diff, DiskIndexFactory, Entry, Footprint, Index, Reader, Serialize,
        Validate, Writer,
    },
    croaring::CRoaring,
    llrb::{Llrb, Stats as LlrbStats},
    mvcc::{Mvcc, Stats as MvccStats},
    nobitmap::NoBitmap,
    robt::{self, Robt, Stats as RobtStats},
};
use rdms_ee::shllrb;

use std::{
    convert::{TryFrom, TryInto},
    ffi, fmt,
    hash::Hash,
    ops::Bound,
    thread, time,
    time::{Duration, SystemTime},
};

use crate::generator::{Cmd, IncrementalLoad, IncrementalRead, IncrementalWrite};
use crate::generator::{InitialLoad, RandomKV};
use crate::stats;
use crate::Profile;

#[derive(Default, Clone)]
pub struct LlrbOpt {
    lsm: bool,
    sticky: bool,
    spin: bool,
}

impl TryFrom<toml::Value> for LlrbOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut llrb_opt: LlrbOpt = Default::default();

        let section = match &value.get("rdms-llrb") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "lsm" => llrb_opt.lsm = value.as_bool().unwrap(),
                "sticky" => llrb_opt.sticky = value.as_bool().unwrap(),
                "spin" => llrb_opt.spin = value.as_bool().unwrap(),
                _ => panic!("invalid profile parameter {}", name),
            }
        }
        Ok(llrb_opt)
    }
}

impl LlrbOpt {
    fn new<K, V>(&self, name: &str) -> Box<Llrb<K, V>>
    where
        K: Clone + Ord,
        V: Clone + Diff,
    {
        let mut index = if self.lsm {
            Llrb::new_lsm(name)
        } else {
            Llrb::new(name)
        };
        index.set_sticky(self.sticky).set_spinlatch(self.spin);
        index
    }
}

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
        index.set_sticky(self.sticky).set_spinlatch(self.spin);
        index
    }
}

#[derive(Default, Clone)]
pub struct RobtOpt {
    dir: ffi::OsString,
    z_blocksize: usize,
    m_blocksize: usize,
    v_blocksize: usize,
    delta_ok: bool,
    vlog_file: Option<ffi::OsString>,
    value_in_vlog: bool,
    flush_queue_size: usize,
    mmap: bool,
    bitmap: String,
}

impl TryFrom<toml::Value> for RobtOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut robt_opt: RobtOpt = Default::default();

        let section = match &value.get("rdms-robt") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "dir" => {
                    let dir: &ffi::OsStr = value.as_str().unwrap().as_ref();
                    robt_opt.dir = dir.to_os_string();
                }
                "z_blocksize" => {
                    robt_opt.z_blocksize = value.as_integer().unwrap().try_into().unwrap()
                }
                "m_blocksize" => {
                    robt_opt.m_blocksize = value.as_integer().unwrap().try_into().unwrap()
                }
                "v_blocksize" => {
                    robt_opt.v_blocksize = value.as_integer().unwrap().try_into().unwrap()
                }
                "delta_ok" => robt_opt.delta_ok = value.as_bool().unwrap(),
                "vlog_file" if value.as_str().unwrap() == "" => robt_opt.vlog_file = None,
                "vlog_file" => {
                    let vlog_file: &ffi::OsStr = value.as_str().unwrap().as_ref();
                    robt_opt.vlog_file = Some(vlog_file.to_os_string());
                }
                "value_in_vlog" => robt_opt.value_in_vlog = value.as_bool().unwrap(),
                "flush_queue_size" => {
                    robt_opt.flush_queue_size = value.as_integer().unwrap().try_into().unwrap()
                }
                "mmap" => robt_opt.mmap = value.as_bool().unwrap(),
                "bitmap" => robt_opt.bitmap = value.as_str().unwrap().to_string(),
                _ => panic!("invalid profile parameter {}", name),
            }
        }

        Ok(robt_opt)
    }
}

impl RobtOpt {
    fn new<K, V, B>(&self, name: &str) -> Robt<K, V, B>
    where
        K: 'static + Clone + Ord + Send + Hash + Footprint + Serialize,
        V: Clone + Diff + Footprint + Serialize,
        <V as Diff>::D: Serialize,
        B: 'static + Send + Bloom,
    {
        let mut config: robt::Config = Default::default();
        config.set_blocksize(self.z_blocksize, self.m_blocksize, self.v_blocksize);
        config.set_delta(self.vlog_file.clone(), self.delta_ok);
        config
            .set_value_log(self.vlog_file.clone(), self.value_in_vlog)
            .set_flush_queue_size(self.flush_queue_size);

        robt::robt_factory(config).new(&self.dir, name).unwrap()
    }
}

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
    fn new<K, V>(&self, name: &str) -> Box<shllrb::Shllrb<K, V>>
    where
        K: 'static + Send + Clone + Ord + Footprint,
        V: 'static + Send + Clone + Diff + Footprint,
        <V as Diff>::D: Send,
    {
        let mut index = shllrb::Shllrb::new(name);
        index
            .set_lsm(self.lsm)
            .set_sticky(self.sticky)
            .set_spinlatch(self.spin)
            .set_shard_config(self.max_shards as usize, self.max_entries as usize)
            .set_interval(time::Duration::from_secs(self.interval as u64));
        index
    }
}

#[derive(Default, Clone)]
pub struct RdmsOpt {
    pub index: String,
    pub name: String,
    pub commit_interval: u64,
    pub initial: usize,
    pub readers: usize,
    pub writers: usize,
}

impl RdmsOpt {
    fn concur_threads(&self) -> usize {
        self.readers + self.writers
    }

    fn initial_threads(&self) -> usize {
        self.initial
    }

    fn configure<K, V, I>(&self, index: &mut rdms::Rdms<K, V, I>)
    where
        K: Send + Clone + Ord + Footprint,
        V: Send + Clone + Diff + Footprint,
        <V as Diff>::D: Send,
        I: 'static + Send + Index<K, V>,
    {
        if self.commit_interval > 0 {
            let interval = Duration::from_secs(self.commit_interval);
            index.set_commit_interval(interval);
        }
    }
}

impl TryFrom<toml::Value> for RdmsOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut rdms_opt: RdmsOpt = Default::default();

        let section = match &value.get("rdms") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "name" => rdms_opt.name = value.as_str().unwrap().to_string(),
                "index" => rdms_opt.index = value.as_str().unwrap().to_string(),
                "commit_interval" => {
                    let v = value.as_integer().unwrap();
                    rdms_opt.commit_interval = v.try_into().unwrap();
                }
                "initial" => {
                    let v = value.as_integer().unwrap();
                    rdms_opt.initial = v.try_into().unwrap();
                }
                "readers" => {
                    let v = value.as_integer().unwrap();
                    rdms_opt.readers = v.try_into().unwrap();
                }
                "writers" => {
                    let v = value.as_integer().unwrap();
                    rdms_opt.writers = v.try_into().unwrap();
                }
                _ => panic!("invalid profile parameter {}", name),
            }
        }
        Ok(rdms_opt)
    }
}

pub fn do_rdms_index(p: Profile) -> Result<(), String> {
    let name = p.rdms.name.clone();
    match (p.key_type.as_str(), p.val_type.as_str()) {
        ("i32", "i32") => Ok(perf::<i32, i32>(&name, p)),
        ("i32", "i64") => Ok(perf::<i32, i64>(&name, p)),
        ("i32", "array") => Ok(perf::<i32, [u8; 20]>(&name, p)),
        ("i32", "bytes") => Ok(perf::<i32, Vec<u8>>(&name, p)),
        ("i64", "i64") => Ok(perf::<i64, i64>(&name, p)),
        ("i64", "array") => Ok(perf::<i64, [u8; 20]>(&name, p)),
        ("i64", "bytes") => Ok(perf::<i64, Vec<u8>>(&name, p)),
        ("array", "array") => Ok(perf::<[u8; 20], [u8; 20]>(&name, p)),
        ("array", "bytes") => Ok(perf::<[u8; 20], Vec<u8>>(&name, p)),
        ("bytes", "bytes") => Ok(perf::<Vec<u8>, Vec<u8>>(&name, p)),
        _ => Err(format!(
            "unsupported key/value types {}/{}",
            p.key_type, p.val_type
        )),
    }
}

fn perf<K, V>(name: &str, p: Profile)
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
    <V as Diff>::D: Send + Serialize,
{
    match p.rdms.index.as_str() {
        "llrb" => perf_llrb::<K, V>(name, p),
        "mvcc" => perf_mvcc::<K, V>(name, p),
        "robt" => match p.rdms_robt.bitmap.as_str() {
            "nobitmap" => perf_robt::<K, V, NoBitmap>(name, p),
            "croaring" => perf_robt::<K, V, CRoaring>(name, p),
            bitmap => panic!("unsupported bitmap {}", bitmap),
        },
        "shllrb" => perf_shllrb::<K, V>(name, p),
        name => panic!("unsupported index {}", name),
    }
}

fn perf_llrb<K, V>(name: &str, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + fmt::Debug + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    <V as Diff>::D: Send,
{
    let llrb_index = p.rdms_llrb.new(name);
    let mut index = rdms::Rdms::new(name, llrb_index).unwrap();
    p.rdms.configure(&mut index);

    let fstats = do_perf::<K, V, Box<Llrb<K, V>>>(&mut index, &p);

    let istats = index.validate().unwrap();
    info!(target: "ixperf", "rdms llrb stats\n{}", istats);
    validate_llrb::<K, V>(&istats, &fstats, &p);
}

fn perf_mvcc<K, V>(name: &str, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + fmt::Debug + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    <V as Diff>::D: Send,
{
    let mvcc_index = p.rdms_mvcc.new(name);
    let mut index = rdms::Rdms::new(name, mvcc_index).unwrap();
    p.rdms.configure(&mut index);

    let fstats = do_perf::<K, V, Box<Mvcc<K, V>>>(&mut index, &p);

    let istats = index.validate().unwrap();
    info!(target: "ixperf", "rdms mvcc stats\n{}", istats);
    validate_mvcc::<K, V>(&istats, &fstats, &p);
}

fn perf_robt<K, V, B>(name: &str, mut p: Profile)
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
    <V as Diff>::D: Send + Serialize,
    B: 'static + Bloom + Send + Sync,
{
    let robt_index = p.rdms_robt.new(name);
    let mut index = rdms::Rdms::new(name, robt_index).unwrap();
    p.rdms.configure(&mut index);

    // load initial data.
    let mut fstats = stats::Ops::new();
    let mut rng = SmallRng::from_seed(p.g.seed.to_le_bytes());
    let mut seqno = 0;
    for i in 0..(p.g.loads / p.g.write_ops()) {
        let mut mem_index = if p.rdms_robt.delta_ok {
            Llrb::new_lsm("load-robt")
        } else {
            Llrb::new("load-rbt")
        };
        mem_index.set_sticky(rng.gen::<bool>());
        mem_index.set_seqno(seqno);
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
        seqno = mem_index.to_seqno();
        index
            .commit(
                CommitIter::new(mem_index, (Bound::Unbounded, Bound::Included(seqno))),
                |meta| meta,
            )
            .unwrap();
    }

    index.compact(Bound::Excluded(0), |_| vec![]).unwrap();

    // validate
    let mut r = index.to_reader().unwrap();
    validate_robt::<K, V, B>(&mut r, &fstats, &p);

    // optional iteration
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
        let mut r = index.to_reader().unwrap();
        r.set_mmap(p.rdms_robt.mmap).unwrap();
        let pr = p.clone();
        threads.push(thread::spawn(move || do_read(i, r, pr)));
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

fn perf_shllrb<K, V>(name: &str, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + fmt::Debug + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    <V as Diff>::D: Send,
{
    let index = p.rdms_shllrb.new(name);
    let mut index = rdms::Rdms::new(name, index).unwrap();
    p.rdms.configure(&mut index);

    let fstats = do_perf::<K, V, Box<shllrb::Shllrb<K, V>>>(&mut index, &p);

    let istats = index.validate().unwrap();
    info!(target: "ixperf", "rdms shllrb stats\n{}", istats);
    // TODO
    // validate_llrb::<K, V>(&istats, &fstats, &p);
}

fn do_perf<K, V, I>(index: &mut rdms::Rdms<K, V, I>, p: &Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    I: Index<K, V>,
    <I as Index<K, V>>::R: 'static + Send + Sync,
    <I as Index<K, V>>::W: 'static + Send + Sync,
{
    let start = SystemTime::now();
    let mut fstats = do_initial_load(index, &p);
    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    info!(target: "ixperf", "initial-load completed in {:?}", dur);

    let (start, mut iter_count) = (SystemTime::now(), 0);
    if p.g.iters {
        let mut r = index.to_reader().unwrap();
        for _ in r.iter().unwrap() {
            iter_count += 1
        }
    }
    let idur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);

    let total_ops = p.g.read_ops() + p.g.write_ops();
    let fstats = if p.rdms.concur_threads() == 0 && total_ops > 0 {
        fstats.merge(&do_incremental(index, &p));
        fstats
    } else if (p.g.read_ops() + p.g.write_ops()) > 0 {
        let mut threads = vec![];
        for i in 0..p.rdms.writers {
            let w = index.to_writer().unwrap();
            let pr = p.clone();
            threads.push(thread::spawn(move || do_write(i, w, pr)));
        }
        for i in 0..p.rdms.readers {
            let r = index.to_reader().unwrap();
            let pr = p.clone();
            threads.push(thread::spawn(move || do_read(i, r, pr)));
        }
        for t in threads {
            fstats.merge(&t.join().unwrap());
        }
        fstats
    } else {
        fstats
    };

    if p.g.iters {
        info!(
            target: "ixperf",
            "rdms took {:?} to iter over {} items", idur, iter_count
        );
    }
    info!(target: "ixperf", "concurrent stats\n{:?}", fstats);
    fstats
}

fn do_initial_load<K, V, I>(
    index: &mut rdms::Rdms<K, V, I>, // index
    p: &Profile,
) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    I: Index<K, V>,
    <I as Index<K, V>>::W: 'static + Send + Sync,
{
    if p.g.loads == 0 {
        return stats::Ops::new();
    }

    let n_threads = p.rdms.initial_threads();
    info!(
        target: "ixperf",
        "initial load for type <{},{}> {} threads",
        p.key_type, p.val_type, n_threads
    );

    let mut threads = vec![];
    for i in 0..n_threads {
        let w = index.to_writer().unwrap();
        let pr = p.clone();
        threads.push(thread::spawn(move || do_initial(i, w, pr)));
    }
    let mut fstats = stats::Ops::new();
    for t in threads {
        fstats.merge(&t.join().unwrap());
    }
    info!(target: "ixperf", "initial stats\n{:?}\n", fstats);
    fstats
}

fn do_incremental<K, V, I>(
    index: &mut rdms::Rdms<K, V, I>, // index
    p: &Profile,
) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    I: Index<K, V>,
{
    if (p.g.read_ops() + p.g.write_ops()) == 0 {
        return stats::Ops::new();
    }

    info!(
        target: "ixperf",
        "incremental load for type <{},{}>", p.key_type, p.val_type
    );

    let mut w = index.to_writer().unwrap();
    let mut r = index.to_reader().unwrap();
    let mut fstats = stats::Ops::new();
    let mut lstats = stats::Ops::new();
    let gen = IncrementalLoad::<K, V>::new(p.g.clone());
    for (_i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                lstats.set.sample_start(false);
                let n = w.set(key, value.clone()).unwrap().map_or(0, |_| 1);
                lstats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                lstats.delete.sample_start(false);
                let items = w.delete(&key).unwrap().map_or(1, |_| 0);
                lstats.delete.sample_end(items);
            }
            Cmd::Get { key } => {
                lstats.get.sample_start(false);
                let items = r.get(&key).ok().map_or(1, |_| 0);
                lstats.get.sample_end(items);
            }
            Cmd::Range { low, high } => {
                let iter = r.range((low, high)).unwrap();
                lstats.range.sample_start(true);
                lstats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Reverse { low, high } => {
                let iter = r.reverse((low, high)).unwrap();
                lstats.reverse.sample_start(true);
                lstats.reverse.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            _ => unreachable!(),
        };
        if p.verbose && lstats.is_sec_elapsed() {
            info!(target: "ixperf", "incremental periodic-stats\n{}", lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }
    fstats.merge(&lstats);

    info!(target: "ixperf", "incremental stats\n{:?}", fstats);
    fstats
}

fn do_initial<W, K, V>(id: usize, mut w: W, mut p: Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    W: Writer<K, V>,
{
    p.g.seed += (id * 100) as u128; // change the seed

    let mut fstats = stats::Ops::new();
    let mut lstats = stats::Ops::new();
    let gen = InitialLoad::<K, V>::new(p.g.clone());
    for (_i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => {
                lstats.load.sample_start(false);
                let items = w.set(key, value).unwrap().map_or(0, |_| 1);
                lstats.load.sample_end(items);
            }
            _ => unreachable!(),
        };
        if p.verbose && lstats.is_sec_elapsed() {
            info!(target: "ixperf", "initial-{} periodic-stats\n{}", id, lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }
    fstats.merge(&lstats);

    info!(target: "ixperf", "initial-{} stats\n{:?}", id, fstats);
    fstats
}

fn do_read<R, K, V>(id: usize, mut r: R, mut p: Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV + Hash,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    R: Reader<K, V>,
{
    p.g.seed += (id * 100) as u128; // change the seed

    if p.g.read_ops() == 0 {
        return stats::Ops::new();
    }

    info!(
        target: "ixperf",
        "reader-{} for type <{},{}>", id, p.key_type, p.val_type
    );

    let mut fstats = stats::Ops::new();
    let mut lstats = stats::Ops::new();
    let gen = IncrementalRead::<K, V>::new(p.g.clone());
    for (_i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Get { key } => {
                lstats.get.sample_start(false);
                let items = r.get(&key).ok().map_or(1, |_| 0);
                lstats.get.sample_end(items);
            }
            Cmd::Range { low, high } => {
                let iter = r.range((low, high)).unwrap();
                lstats.range.sample_start(true);
                lstats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            Cmd::Reverse { low, high } => {
                let iter = r.reverse((low, high)).unwrap();
                lstats.reverse.sample_start(true);
                lstats.reverse.sample_end(iter.fold(0, |acc, _| acc + 1));
            }
            _ => unreachable!(),
        };
        if p.verbose && lstats.is_sec_elapsed() {
            info!(target: "ixperf", "reader-{} periodic-stats\n{}", id, lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }
    fstats.merge(&lstats);

    info!(target: "ixperf", "reader-{} stats {:?}", id, fstats);
    fstats
}

fn do_write<W, K, V>(id: usize, mut w: W, mut p: Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    W: Writer<K, V>,
{
    p.g.seed += (id * 100) as u128; // change the seed

    if p.g.write_ops() == 0 {
        return stats::Ops::new();
    }

    info!(
        target: "ixperf",
        "writer-{} for type <{},{}>", id, p.key_type, p.val_type
    );

    let mut fstats = stats::Ops::new();
    let mut lstats = stats::Ops::new();
    let gen = IncrementalWrite::<K, V>::new(p.g.clone());
    for (_i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                lstats.set.sample_start(false);
                let n = w.set(key, value.clone()).unwrap().map_or(0, |_| 1);
                lstats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                lstats.delete.sample_start(false);
                let items = w.delete(&key).unwrap().map_or(1, |_| 0);
                lstats.delete.sample_end(items);
            }
            _ => unreachable!(),
        };
        if p.verbose && lstats.is_sec_elapsed() {
            info!(target: "ixperf", "writer-{} periodic-stats\n{}", id, lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }
    fstats.merge(&lstats);

    info!(target: "ixperf", "writer-{} stats\n{:?}", id, fstats);
    fstats
}

fn validate_llrb<K, V>(stats: &LlrbStats, fstats: &stats::Ops, p: &Profile)
where
    K: Clone + Ord + Default + Footprint + fmt::Debug + RandomKV,
    V: Clone + Diff + Default + Footprint + RandomKV,
{
    if p.rdms_llrb.lsm || p.rdms_llrb.sticky {
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

    assert_eq!(stats.rw_latch.read_locks, fstats.to_total_reads() + 3);
    assert_eq!(stats.rw_latch.write_locks, fstats.to_total_writes());
    if fstats.to_total_reads() == 0 || fstats.to_total_writes() == 0 {
        assert_eq!(stats.rw_latch.conflicts, 0);
    }

    if p.rdms_llrb.lsm == false {
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
        let (kfp, vfp) = match Cmd::<K, V>::gen_load(&mut rng, &p.g) {
            Cmd::Load { key, value } => (
                key.footprint().unwrap() as usize,
                value.footprint().unwrap() as usize,
            ),
            _ => unreachable!(),
        };
        let (entries, vfp) = (stats.entries, vfp + std::mem::size_of::<V>());

        let key_footprint: isize = (kfp * entries).try_into().unwrap();
        assert_eq!(stats.key_footprint, key_footprint);

        let mut tree_footprint: isize = ((stats.node_size + kfp + vfp) * entries)
            .try_into()
            .unwrap();
        tree_footprint -= (vfp * stats.n_deleted) as isize; // for sticky mode.
        assert_eq!(stats.tree_footprint, tree_footprint);
    }
}

fn validate_robt<K, V, B>(r: &mut robt::Snapshot<K, V, B>, fstats: &stats::Ops, p: &Profile)
where
    K: Clone + Ord + Default + Footprint + Serialize + fmt::Debug + RandomKV,
    V: Clone + Diff + Default + Footprint + Serialize + RandomKV,
    <V as Diff>::D: Clone + Serialize,
    B: Bloom,
{
    info!(target: "ixperf", "validating robt index ...");

    let stats: RobtStats = r.validate().unwrap();
    if p.rdms_robt.delta_ok {
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
