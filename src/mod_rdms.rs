use log::info;

use rdms::{
    self,
    core::{Diff, Footprint, Index, Reader, Serialize, Writer},
    croaring::CRoaring,
    nobitmap::NoBitmap,
};

use std::{
    convert::{TryFrom, TryInto},
    fmt,
    hash::Hash,
    thread,
    time::{Duration, SystemTime},
};

use crate::generator::{Cmd, ConcurrentLoad, IncrementalRead, IncrementalWrite};
use crate::generator::{InitialLoad, RandomKV};
use crate::mod_rdms_dgm as mod_dgm;
use crate::mod_rdms_llrb as mod_llrb;
use crate::mod_rdms_mvcc as mod_mvcc;
use crate::mod_rdms_robt as mod_robt;
use crate::mod_rdms_shllrb as mod_shllrb;
use crate::stats;
use crate::Profile;

#[derive(Default, Clone)]
pub struct RdmsOpt {
    pub index: String,
    pub name: String,
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
    <V as Diff>::D: Send + Default + Serialize,
{
    match p.rdms.index.as_str() {
        "llrb" => mod_llrb::perf::<K, V>(name, p),
        "mvcc" => mod_mvcc::perf::<K, V>(name, p),
        "robt" => match p.rdms_robt.to_bitmap() {
            "nobitmap" => mod_robt::perf::<K, V, NoBitmap>(name, p),
            "croaring" => mod_robt::perf::<K, V, CRoaring>(name, p),
            bitmap => panic!("unsupported bitmap {}", bitmap),
        },
        "shllrb" => mod_shllrb::perf::<K, V>(name, p),
        "dgm" => mod_dgm::perf::<K, V>(name, p),
        name => panic!("unsupported index {}", name),
    }
}

pub(crate) fn do_perf<K, V, I>(index: &mut rdms::Rdms<K, V, I>, p: &Profile) -> stats::Ops
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
    let gen = ConcurrentLoad::<K, V>::new(p.g.clone());
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

pub(crate) fn do_read<R, K, V>(id: usize, mut r: R, mut p: Profile) -> stats::Ops
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
