use log::info;
use rdms::{self, Diff, Footprint, Index, Reader, Writer};

use std::{
    convert::{TryFrom, TryInto},
    thread,
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
    fn new<K, V>(&self, name: &str) -> Box<rdms::llrb::Llrb<K, V>>
    where
        K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
        V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    {
        let mut index = if self.lsm {
            rdms::llrb::Llrb::new_lsm(name)
        } else {
            rdms::llrb::Llrb::new(name)
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
    fn new_with_llrb<K, V>(
        &self, // from options
        name: &str,
        p: &Profile,
    ) -> rdms::Rdms<K, V, Box<rdms::llrb::Llrb<K, V>>>
    where
        K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
        V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    {
        let index = p.rdms_llrb.new(name);
        let mut index = rdms::Rdms::new(name, index).unwrap();
        if self.commit_interval > 0 {
            index.set_commit_interval(Duration::from_secs(self.commit_interval));
        }
        index
    }

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

pub fn do_rdms_index(p: Profile) -> Result<(), String> {
    match (p.key_type.as_str(), p.val_type.as_str()) {
        ("i32", "i32") => Ok(perf::<i32, i32>(p)),
        //("i32", "array") => Ok(perf::<i32, [u8; 32]>(p)),
        ("i32", "bytes") => Ok(perf::<i32, Vec<u8>>(p)),
        ("i64", "i64") => Ok(perf::<i64, i64>(p)),
        //("i64", "array") => Ok(perf::<i64, [u8; 32]>(p)),
        ("i64", "bytes") => Ok(perf::<i64, Vec<u8>>(p)),
        //("array", "array") => Ok(perf::<[u8; 32], [u8; 32]>(p)),
        //("array", "bytes") => Ok(perf::<[u8; 32], Vec<u8>>(p)),
        ("bytes", "bytes") => Ok(perf::<Vec<u8>, Vec<u8>>(p)),
        _ => Err(format!(
            "unsupported key/value types {}/{}",
            p.key_type, p.val_type
        )),
    }
}

fn perf<K, V>(p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
{
    match p.rdms.index.as_str() {
        "llrb" => {
            let index = p.rdms.new_with_llrb("ixperf", &p);
            perf1::<K, V, Box<rdms::llrb::Llrb<K, V>>>(index, p)
        }
        name => panic!("unsupported index {}", name),
    }
}

fn perf1<K, V, I>(mut index: rdms::Rdms<K, V, I>, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    I: Index<K, V>,
    <I as Index<K, V>>::R: 'static + Send + Sync,
    <I as Index<K, V>>::W: 'static + Send + Sync,
{
    let start = SystemTime::now();
    do_initial_load(&mut index, &p);
    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    info!(target: "rdmsix", "initial-load completed in {:?}", dur);

    let (start, mut iter_count) = (SystemTime::now(), 0);
    if p.g.iters {
        let mut r = index.to_reader().unwrap();
        for _ in r.iter().unwrap() {
            iter_count += 1
        }
    }
    let idur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);

    if p.rdms.threads() == 0 && (p.g.read_ops() + p.g.write_ops()) > 0 {
        do_incremental(&mut index, &p);
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
            t.join().unwrap()
        }
    }

    if p.g.iters {
        info!(
            target: "rdmsix",
            "rdms took {:?} to iter over {} items", idur, iter_count
        );
    }

    // TODO validate(index, p);
}

fn do_initial_load<K, V, I>(index: &mut rdms::Rdms<K, V, I>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    I: Index<K, V>,
{
    if p.g.loads == 0 {
        return;
    }

    info!(
        target: "rdmsix",
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
            info!(target: "rdmsix", "initial periodic-stats\n{}", lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }
    info!(target: "llrbix", "initial stats\n{:?}\n", fstats);
}

fn do_incremental<K, V, I>(index: &mut rdms::Rdms<K, V, I>, p: &Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    I: Index<K, V>,
{
    if (p.g.read_ops() + p.g.write_ops()) == 0 {
        return;
    }

    info!(
        target: "rdmsix",
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
                let items = w.delete(&key).ok().map_or(1, |_| 0);
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
            info!(target: "rdmsix", "incremental periodic-stats\n{}", lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }

    info!(target: "rdmsix", "incremental stats\n{:?}", lstats);
}

fn do_read<R, K, V>(id: usize, mut r: R, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    R: Reader<K, V>,
{
    if p.g.read_ops() == 0 {
        return;
    }

    info!(
        target: "rdmsix",
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
            info!(target: "rdmsix", "reader-{} periodic-stats\n{}", id, lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }

    info!(target: "rdmsix", "reader-{} stats {:?}", id, lstats);
}

fn do_write<W, K, V>(id: usize, mut w: W, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
    W: Writer<K, V>,
{
    if p.g.write_ops() == 0 {
        return;
    }

    info!(
        target: "rdmsix",
        "writer-{} for type <{},{}>", id, p.key_type, p.val_type
    );

    let mut fstats = stats::Ops::new();
    let mut lstats = stats::Ops::new();
    let gen = IncrementalWrite::<K, V>::new(p.g.clone());
    for (_i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Set { key, value } => {
                lstats.set.sample_start(false);
                let n = w.set(key, value.clone()).map_or_else(|_| 0, |_| 1);
                lstats.set.sample_end(n);
            }
            Cmd::Delete { key } => {
                lstats.delete.sample_start(false);
                let items = w.delete(&key).map_or_else(|_| 0, |_| 1);
                lstats.delete.sample_end(items);
            }
            _ => unreachable!(),
        };
        if p.verbose && lstats.is_sec_elapsed() {
            info!(target: "rdmsix", "writer-{} periodic-stats\n{}", id, lstats);
            fstats.merge(&lstats);
            lstats = stats::Ops::new();
        }
    }

    info!(target: "rdmsix", "writer-{} stats\n{:?}", id, lstats);
}

//fn validate<K, V, I>(index: rdms::Rdms<K, V, I>, p: Profile)
//where
//    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV + fmt::Debug,
//    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV,
//    I: Index<K, V>,
//{
//    // TODO: validate the statitics
//    //match index.validate() {
//    //    Ok(stats) => {
//    //        if p.write_ops() == 0 {
//    //            assert!(stats.to_conflicts() == 0);
//    //        }
//    //    }
//    //    Err(err) => panic!(err),
//    //}
//}
