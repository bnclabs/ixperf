use std::{
    ffi, fmt, path, thread,
    time::{Duration, SystemTime},
};

use rdms::{llrb::Llrb, robt};
use rdms::{Diff, Footprint, Reader, Serialize, Writer};

use crate::generator::{Cmd, InitialLoad, RandomKV};
use crate::Profile;

pub fn perf<K, V>(p: Profile)
where
    K: 'static
        + Clone
        + Default
        + Send
        + Sync
        + Ord
        + Footprint
        + RandomKV
        + fmt::Debug
        + Serialize,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV + Serialize,
    <V as Diff>::D: Send + Sync + Serialize,
{
    let (dir, name) = do_initial_load::<K, V>(&p);

    let mut threads = vec![];
    for _i in 0..p.readers {
        let r = robt::Snapshot::<K, V>::open(&dir, &name).unwrap();
        let pr = p.clone();
        threads.push(thread::spawn(|| do_read(r, pr)));
    }
    for t in threads {
        t.join().unwrap()
    }

    let index = robt::Snapshot::<K, V>::open(&dir, &name).unwrap();
    validate(index, p);
}

fn do_initial_load<K, V>(p: &Profile) -> (ffi::OsString, String)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV + Serialize,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV + Serialize,
    <V as Diff>::D: Send + Sync + Serialize,
{
    let start = SystemTime::now();
    let mut ref_index: Box<Llrb<K, V>> = Llrb::new("ixperf-load");

    // load reference index
    let (kt, vt) = (&p.key_type, &p.val_type);
    println!("\n==== INITIAL LOAD for type <{},{}> ====", kt, vt);
    let gen = InitialLoad::<K, V>::new(p.clone());
    for (i, cmd) in gen.enumerate() {
        match cmd {
            Cmd::Load { key, value } => ref_index.set(key, value).unwrap(),
            _ => unreachable!(),
        };
        if ((i + 1) % 1_000_000) == 0 {
            let elapsed = start.elapsed().unwrap().as_nanos() as u64;
            let dur = Duration::from_nanos(elapsed);
            println!("loaded {}/{} items in {:?}", ref_index.len(), i, dur);
        }
    }
    println!("");

    // build robt index
    let dir = {
        let mut dir = path::PathBuf::from(std::env::temp_dir());
        dir.push("ixperf-rdms-robt");
        dir.into_os_string()
    };
    let name = "ixperf";
    let config: robt::Config = Default::default();
    let b = robt::Builder::commit(&dir, name, config).unwrap();
    let iter = ref_index.iter().unwrap();
    println!("dir:{:?}  name:{}", dir, name);
    b.build(iter, vec![]).unwrap();

    let dur = Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64);
    println!("initial-load {} items in {:?}", ref_index.len(), dur);

    (dir, name.to_string())
}

fn do_read<K, V>(_r: robt::Snapshot<K, V>, p: Profile)
where
    K: 'static + Clone + Default + Send + Sync + Ord + Footprint + RandomKV + Serialize,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV + Serialize,
{
    if p.read_ops() == 0 {
        return;
    }

    //let mut ostats = stats::Ops::new();
    //let start = SystemTime::now();

    //let (kt, vt) = (&p.key_type, &p.val_type);
    //println!("\n==== INCREMENTAL Read for type <{},{}> ====", kt, vt);
    //let gen = IncrementalRead::<K, V>::new(p.clone());
    //for (i, cmd) in gen.enumerate() {
    //    match cmd {
    //        Cmd::Get { key } => {
    //            ostats.get.sample_start();
    //            let items = r.get(&key).ok().map_or(1, |_| 0);
    //            ostats.get.sample_end(items);
    //        }
    //        Cmd::Iter => {
    //            let iter = r.iter().unwrap();
    //            ostats.iter.sample_start();
    //            ostats.iter.sample_end(iter.fold(0, |acc, _| acc + 1));
    //        }
    //        Cmd::Range { low, high } => {
    //            let iter = r.range((low, high)).unwrap();
    //            ostats.range.sample_start();
    //            ostats.range.sample_end(iter.fold(0, |acc, _| acc + 1));
    //        }
    //        Cmd::Reverse { low, high } => {
    //            let iter = r.reverse((low, high)).unwrap();
    //            ostats.reverse.sample_start();
    //            ostats.reverse.sample_end(iter.fold(0, |acc, _| acc + 1));
    //        }
    //        _ => unreachable!(),
    //    };
    //    if ((i + 1) % crate::LOG_BATCH) == 0 {
    //        p.periodic_log("incremental-read ", &ostats, false /*fin*/);
    //    }
    //}

    //p.periodic_log("incremental-read ", &ostats, true /*fin*/);
    //let ops = ostats.total_ops();
    //let elapsed = start.elapsed().unwrap();
    //let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    //println!("incremental-read {} in {:?}", ops, dur);
}

fn validate<K, V>(_index: robt::Snapshot<K, V>, _p: Profile)
where
    K: 'static
        + Clone
        + Default
        + Send
        + Sync
        + Ord
        + Footprint
        + RandomKV
        + fmt::Debug
        + Serialize,
    V: 'static + Clone + Default + Send + Sync + Diff + Footprint + RandomKV + Serialize,
    <V as Diff>::D: Send + Sync,
{
    // TODO: validate the statitics
    //match index.validate() {
    //    Ok(stats) => {
    //        if p.write_ops() == 0 {
    //            assert!(stats.to_conflicts() == 0);
    //        }
    //    }
    //    Err(err) => panic!(err),
    //}
}
