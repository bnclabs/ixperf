use std::io;
use std::ops::Bound;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use llrb_index::Llrb;
use lmdb::{self, Cursor, Transaction};

use crate::generator::{init_generators, read_generator, write_generator};
use crate::opts::{Cmd, Opt};
use crate::stats;

pub fn perf(opt: Opt) {
    println!("\n==== INITIAL LOAD ====");
    let refn = Arc::new(Llrb::new("reference"));
    let (opt1, opt2) = (opt.clone(), opt.clone());

    let (tx_idx, rx_idx) = mpsc::channel();
    let (tx_ref, rx_ref) = mpsc::channel();

    let generator = thread::spawn(move || init_generators(opt1, tx_idx, tx_ref));

    let refn1 = Arc::clone(&refn);
    let reference = thread::spawn(move || {
        let refn1 = unsafe {
            (Arc::into_raw(refn1) as *mut Llrb<Vec<u8>, Vec<u8>>)
                .as_mut()
                .unwrap()
        };
        for item in rx_ref {
            let value: Vec<u8> = vec![];
            refn1.set(item, value);
        }
        let _refn1 = unsafe { Arc::from_raw(refn1) };
    });

    do_initial(opt2, rx_idx);

    generator.join().unwrap();
    reference.join().unwrap();

    println!("\n==== INCREMENTAL LOAD ====");
    let refn = if let Ok(refn) = Arc::try_unwrap(refn) {
        refn
    } else {
        unreachable!();
    };

    // incremental writer
    let mut threads: Vec<JoinHandle<()>> = vec![];
    let (tx, rx) = mpsc::channel();
    let (opt1, opt2) = (opt.clone(), opt.clone());
    let refn1 = refn.clone();
    let g = thread::spawn(move || write_generator(opt1, tx, refn1));
    let w = thread::spawn(move || do_writer(opt2, rx));
    threads.push(g);
    threads.push(w);

    // incremental reader
    for _i in 0..opt.readers {
        let (tx, rx) = mpsc::channel();
        let (opt1, opt2) = (opt.clone(), opt.clone());
        let refn1 = refn.clone();
        let g = thread::spawn(move || read_generator(1, opt1, tx, refn1));
        let r = thread::spawn(move || do_reader(opt2, rx));
        threads.push(g);
        threads.push(r);
    }

    for t in threads.into_iter() {
        t.join().unwrap();
    }
}

fn do_initial(opt: Opt, rx: mpsc::Receiver<Cmd>) {
    let mut op_stats = stats::Ops::new();
    let start = SystemTime::now();

    let (mut env, db) = init_lmdb(&opt, "ixperf");

    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);

    let write_flags: lmdb::WriteFlags = Default::default();
    let mut seqno = 1;

    let lmdb_batch = 100_000;
    {
        let mut txn = env.begin_rw_txn().unwrap();
        for cmd in rx {
            match cmd {
                Cmd::Load { key } => {
                    op_stats.load.latency.start();
                    txn.put(db, &key, &value, write_flags.clone()).unwrap();
                    op_stats.load.latency.stop();
                }
                _ => unreachable!(),
            };
            if (seqno % lmdb_batch) == 0 {
                txn.commit().unwrap();
                txn = env.begin_rw_txn().unwrap();
            }
            seqno += 1;
        }
    }

    let entries = env.stat().unwrap().entries();
    let (elapsed, len) = (start.elapsed().unwrap(), entries);
    let rate = len / ((elapsed.as_nanos() / 1000_000_000) as usize);
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("loaded {} items in {:?} @ {} ops/sec", len, dur, rate);

    unsafe { env.close_db(db) };

    if opt.json {
        println!("{}", op_stats.json());
    } else {
        op_stats.pretty_print("");
    }
}

fn do_writer(opt: Opt, rx: mpsc::Receiver<Cmd>) {
    let mut op_stats = stats::Ops::new();
    let mut value: Vec<u8> = Vec::with_capacity(opt.valsize);
    value.resize(opt.valsize, 0xAD);
    let mut ops = 0;
    let write_flags: lmdb::WriteFlags = Default::default();

    let (mut env, db) = open_lmdb(&opt, "ixperf");

    let start = SystemTime::now();
    for cmd in rx {
        ops += 1;
        match cmd {
            Cmd::Create { key } => {
                op_stats.create.latency.start();
                let mut txn = env.begin_rw_txn().unwrap();
                txn.put(db, &key, &value, write_flags.clone()).unwrap();
                txn.commit().unwrap();
                op_stats.create.latency.stop();
                op_stats.create.count += 1;
            }
            Cmd::Set { key } => {
                op_stats.set.latency.start();
                let mut txn = env.begin_rw_txn().unwrap();
                txn.put(db, &key, &value, write_flags.clone()).unwrap();
                txn.commit().unwrap();
                op_stats.set.latency.stop();
                op_stats.set.count += 1;
            }
            Cmd::Delete { key } => {
                op_stats.delete.latency.start();
                let mut txn = env.begin_rw_txn().unwrap();
                match txn.del(db, &key, None /*data*/) {
                    Ok(_) | Err(lmdb::Error::NotFound) => (),
                    res @ _ => panic!("lmdb del: {:?}", res),
                }
                txn.commit().unwrap();
                op_stats.delete.latency.stop();
                op_stats.delete.count += 1;
            }
            _ => (),
        };
    }

    let entries = env.stat().unwrap().entries();
    let (elapsed, len) = (start.elapsed().unwrap(), entries);
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("writer ops {} in {:?}, index-len: {}", ops, dur, len);

    unsafe { env.close_db(db) };

    if opt.json {
        println!("{}", op_stats.json());
    } else {
        op_stats.pretty_print("");
    }
}

fn do_reader(opt: Opt, rx: mpsc::Receiver<Cmd>) {
    let mut op_stats = stats::Ops::new();
    let mut ops = 0;

    let (mut env, db) = open_lmdb(&opt, "ixperf");

    let start = SystemTime::now();
    for cmd in rx {
        ops += 1;
        match cmd {
            Cmd::Get { key } => {
                op_stats.get.latency.start();
                let txn = env.begin_ro_txn().unwrap();
                match txn.get(db, &key) {
                    Ok(_) => (),
                    Err(lmdb::Error::NotFound) => op_stats.get.items += 1,
                    Err(err) => panic!(err),
                }
                op_stats.get.latency.stop();
                op_stats.get.count += 1;
            }
            Cmd::Iter => {
                let txn = env.begin_ro_txn().unwrap();
                let mut cur = txn.open_ro_cursor(db).unwrap();
                let iter = cur.iter();

                op_stats.iter.latency.start();
                iter.for_each(|_| op_stats.iter.items += 1);
                op_stats.iter.latency.stop();
                op_stats.iter.count += 1;
            }
            Cmd::Range { low, high } => {
                let txn = env.begin_ro_txn().unwrap();
                let mut cur = txn.open_ro_cursor(db).unwrap();
                let iter = match low {
                    Bound::Included(low) => cur.iter_from(low.clone()),
                    Bound::Excluded(low) => cur.iter_from(low.clone()),
                    _ => cur.iter(),
                };

                op_stats.range.latency.start();
                for (key, _value) in iter {
                    match high {
                        Bound::Included(ref high) if key > high => break,
                        Bound::Excluded(ref high) if key >= high => break,
                        _ => (),
                    }
                    op_stats.range.items += 1;
                }
                op_stats.range.latency.stop();
                op_stats.range.count += 1;
            }
            Cmd::Reverse { low: _, high: _ } => (),
            _ => unreachable!(),
        };
    }

    let entries = env.stat().unwrap().entries();
    let (elapsed, len) = (start.elapsed().unwrap(), entries);
    let dur = Duration::from_nanos(elapsed.as_nanos() as u64);
    println!("reader ops {} in {:?}, index-len: {}", ops, dur, len);

    op_stats.pretty_print("");
    unsafe { env.close_db(db) };
}

fn init_lmdb(opt: &Opt, name: &str) -> (lmdb::Environment, lmdb::Database) {
    // setup directory
    match std::fs::remove_dir_all(&opt.path) {
        Ok(()) => (),
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => (),
        Err(err) => panic!("{:?}", err),
    }
    let path = std::path::Path::new(&opt.path).join(name);
    std::fs::create_dir_all(&path).unwrap();

    // create the environment
    let mut flags = lmdb::EnvironmentFlags::empty();
    flags.insert(lmdb::EnvironmentFlags::NO_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_META_SYNC);
    let env = lmdb::Environment::new()
        .set_flags(flags)
        .set_map_size(150_000_000_000)
        .open(&path)
        .unwrap();

    let db = env.open_db(None).unwrap();

    (env, db)
}

fn open_lmdb(opt: &Opt, name: &str) -> (lmdb::Environment, lmdb::Database) {
    let path = std::path::Path::new(&opt.path).join(name);

    // create the environment
    let mut flags = lmdb::EnvironmentFlags::empty();
    flags.insert(lmdb::EnvironmentFlags::NO_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_META_SYNC);
    let env = lmdb::Environment::new()
        .set_flags(flags)
        .set_map_size(150_000_000_000)
        .open(&path)
        .unwrap();

    let db = env.open_db(None).unwrap();

    (env, db)
}
