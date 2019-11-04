use log::info;
use rdms::{
    self,
    llrb::{Llrb, Stats as LlrbStats},
    mvcc::{Mvcc, Stats as MvccStats},
    Diff, Footprint, Index, Reader, Writer,
};

use std::{
    convert::{TryFrom, TryInto},
    fmt, thread,
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
        K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
        V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
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
        K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
        V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
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
pub struct RdmsOpt {
    index: String,
    commit_interval: u64,
    readers: usize,
    writers: usize,
}

impl RdmsOpt {
    fn threads(&self) -> usize {
        self.readers + self.writers
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
                "index" => rdms_opt.index = value.as_str().unwrap().to_string(),
                "commit_interval" => {
                    let v = value.as_integer().unwrap();
                    rdms_opt.commit_interval = v.try_into().unwrap();
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

pub fn do_rdms_index(name: &str, p: Profile) -> Result<(), String> {
    match (p.key_type.as_str(), p.val_type.as_str()) {
        ("i32", "i32") => Ok(perf::<i32, i32>(name, p)),
        ("i32", "array") => Ok(perf::<i32, [u8; 20]>(name, p)),
        ("i32", "bytes") => Ok(perf::<i32, Vec<u8>>(name, p)),
        ("i64", "i64") => Ok(perf::<i64, i64>(name, p)),
        ("i64", "array") => Ok(perf::<i64, [u8; 20]>(name, p)),
        ("i64", "bytes") => Ok(perf::<i64, Vec<u8>>(name, p)),
        ("array", "array") => Ok(perf::<[u8; 20], [u8; 20]>(name, p)),
        ("array", "bytes") => Ok(perf::<[u8; 20], Vec<u8>>(name, p)),
        ("bytes", "bytes") => Ok(perf::<Vec<u8>, Vec<u8>>(name, p)),
        _ => Err(format!(
            "unsupported key/value types {}/{}",
            p.key_type, p.val_type
        )),
    }
}

fn perf<K, V>(name: &str, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + fmt::Debug + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    match p.rdms.index.as_str() {
        "llrb" => {
            let llrb_index = p.rdms_llrb.new(name);
            let mut index = rdms::Rdms::new(name, llrb_index).unwrap();
            if p.rdms.commit_interval > 0 {
                let interval = Duration::from_secs(p.rdms.commit_interval);
                index.set_commit_interval(interval);
            }

            let fstats = perf1::<K, V, Box<Llrb<K, V>>>(&mut index, &p);

            let istats = index.validate().unwrap();
            info!(target: "ixperf", "rdms llrb stats\n{}", istats);
            validate_llrb(&istats, &fstats, &p);
        }
        "mvcc" => {
            let mvcc_index = p.rdms_mvcc.new(name);
            let mut index = rdms::Rdms::new(name, mvcc_index).unwrap();
            if p.rdms.commit_interval > 0 {
                let interval = Duration::from_secs(p.rdms.commit_interval);
                index.set_commit_interval(interval);
            }

            let fstats = perf1::<K, V, Box<Mvcc<K, V>>>(&mut index, &p);

            let istats = index.validate().unwrap();
            info!(target: "ixperf", "rdms mvcc stats\n{}", istats);
            validate_mvcc(&istats, &fstats, &p);
        }
        name => panic!("unsupported index {}", name),
    }
}

fn perf1<K, V, I>(index: &mut rdms::Rdms<K, V, I>, p: &Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
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
    let fstats = if p.rdms.threads() == 0 && total_ops > 0 {
        fstats.merge(&do_incremental(index, &p));
        fstats
    } else if (p.g.read_ops() + p.g.write_ops()) > 0 {
        let mut threads = vec![];
        for i in 0..p.rdms.readers {
            let r = index.to_reader().unwrap();
            let pr = p.clone();
            threads.push(thread::spawn(move || do_read(i, r, pr)));
        }
        for i in 0..p.rdms.writers {
            let w = index.to_writer().unwrap();
            let pr = p.clone();
            threads.push(thread::spawn(move || do_write(i, w, pr)));
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
{
    if p.g.loads == 0 {
        return stats::Ops::new();
    }

    info!(
        target: "ixperf",
        "initial load for type <{},{}>", p.key_type, p.val_type
    );
    let mut w = index.to_writer().unwrap();
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
            info!(target: "ixperf", "initial periodic-stats\n{}", lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }
    fstats.merge(&lstats);

    info!(target: "ixperf", "initial stats\n{:?}\n", fstats);
    fstats
}

fn do_incremental<K, V, I>(
    index: &mut rdms::Rdms<K, V, I>, // index
    p: &Profile,
) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
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

fn do_read<R, K, V>(id: usize, mut r: R, p: Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    R: Reader<K, V>,
{
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

fn do_write<W, K, V>(id: usize, mut w: W, p: Profile) -> stats::Ops
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    W: Writer<K, V>,
{
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
                let items = w
                    .delete(&key)
                    .unwrap()
                    .map_or(1, |e| if e.is_deleted() { 1 } else { 0 });
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

fn validate_llrb(stats: &LlrbStats, fstats: &stats::Ops, p: &Profile) {
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
}

fn validate_mvcc(stats: &MvccStats, fstats: &stats::Ops, p: &Profile) {
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
}
