use lmdb::{self, Cursor, Transaction};
use log::{debug, info};

use std::{
    convert::{TryFrom, TryInto},
    ffi, io,
    ops::Bound,
    path,
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};

use crate::generator::InitialLoad;
use crate::generator::{Cmd, IncrementalLoad, IncrementalRead, IncrementalWrite};
use crate::stats;
use crate::Profile;

#[derive(Default, Clone)]
pub struct LmdbOpt {
    pub name: String,
    pub dir: String,
    pub readers: usize,
    pub writers: usize,
    pub load_batch: usize,
}

impl LmdbOpt {
    fn concur_threads(&self) -> usize {
        self.readers + self.writers
    }
}

impl TryFrom<toml::Value> for LmdbOpt {
    type Error = String;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        let mut lmdb_opt: LmdbOpt = Default::default();

        let section = match &value.get("lmdb") {
            None => return Err("not found".to_string()),
            Some(section) => section.clone(),
        };
        for (name, value) in section.as_table().unwrap().iter() {
            match name.as_str() {
                "name" => lmdb_opt.name = value.as_str().unwrap().to_string(),
                "dir" => lmdb_opt.dir = value.as_str().unwrap().to_string(),
                "readers" => {
                    let v = value.as_integer().unwrap();
                    lmdb_opt.readers = v.try_into().unwrap();
                }
                "writers" => {
                    let v = value.as_integer().unwrap();
                    lmdb_opt.writers = v.try_into().unwrap();
                }
                "load_batch" => {
                    let v = value.as_integer().unwrap();
                    lmdb_opt.load_batch = v.try_into().unwrap();
                }
                _ => panic!("invalid profile parameter {}", name),
            }
        }

        lmdb_opt.dir = if lmdb_opt.dir.len() == 0 {
            let mut pp = path::PathBuf::new();
            pp.push(".");
            pp.push("lmdb_data");
            let dir: &ffi::OsStr = pp.as_ref();
            dir.to_str().unwrap().to_string()
        } else {
            lmdb_opt.dir
        };

        Ok(lmdb_opt)
    }
}

pub fn perf(p: Profile) -> Result<(), String> {
    info!(target: "ixperf", "for type <{},{}>", p.key_type, p.val_type);

    {
        let (env, db) = init_lmdb(&p, "lmdb");
        do_initial(&p, env, db);
    }

    let (iter_elapsed, iter_count) = if p.g.iters {
        let (env, db) = open_lmdb(&p, "lmdb");
        let start = SystemTime::now();
        let txn = env.begin_ro_txn().unwrap();
        let iter = txn.open_ro_cursor(db).unwrap().iter();
        let count = iter.map(|_| true).collect::<Vec<bool>>().len();
        (
            Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64),
            count,
        )
    } else {
        (Default::default(), Default::default())
    };

    let total_ops = p.g.read_ops() + p.g.write_ops();
    let (mut env, db) = if p.lmdb.concur_threads() == 0 && total_ops > 0 {
        let (env, db) = open_lmdb(&p, "lmdb");
        do_incremental(&p, env, db);
        let (env, db) = open_lmdb(&p, "lmdb");
        (Arc::new(env), db)
    } else if total_ops > 0 {
        let (env, db) = open_lmdb(&p, "lmdb");
        let env = Arc::new(env);

        let mut w_threads = vec![];
        for i in 0..p.lmdb.writers {
            let pp = p.clone();
            let envv = Arc::clone(&env);
            w_threads.push(thread::spawn(move || do_write(i, pp, envv, db)));
        }
        let mut r_threads = vec![];
        for i in 0..p.lmdb.readers {
            let pp = p.clone();
            let envv = Arc::clone(&env);
            r_threads.push(thread::spawn(move || do_read(i, pp, envv, db)));
        }
        {
            let mut fstats = stats::Ops::new();
            for t in w_threads {
                fstats.merge(&t.join().unwrap());
            }
            stats!(&p.cmd_opts, "ixperf", "all-writers stats\n{:?}", fstats);
        }
        {
            let mut fstats = stats::Ops::new();
            for t in r_threads {
                fstats.merge(&t.join().unwrap());
            }
            stats!(&p.cmd_opts, "ixperf", "all-readers stats\n{:?}", fstats);
        }
        (env, db)
    } else {
        let (env, db) = open_lmdb(&p, "lmdb");
        (Arc::new(env), db)
    };

    unsafe { Arc::get_mut(&mut env).unwrap().close_db(db) };
    env.sync(true).unwrap();

    if p.g.iters {
        info!(
            target: "ixperf",
            "took {:?} to iter over {} items", iter_elapsed, iter_count
        );
    }

    Ok(())
}

fn do_initial(
    p: &Profile,
    mut env: lmdb::Environment,
    db: lmdb::Database, // index
) -> stats::Ops {
    if p.g.loads == 0 {
        return stats::Ops::new();
    }

    let mut txn = env.begin_rw_txn().unwrap();
    let write_flags: lmdb::WriteFlags = Default::default();
    let mut load_count = 0;
    let mut fstats = stats::Ops::new();
    let elapsed = {
        let start = SystemTime::now();

        let mut lstats = stats::Ops::new();
        let gen = InitialLoad::<Vec<u8>, Vec<u8>>::new(p.g.clone());
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Load { key, value } => {
                    lstats.load.sample_start(false);
                    txn.put(db, &key, &value, write_flags.clone()).unwrap();
                    lstats.load.sample_end(0);
                    load_count += 1;
                }
                _ => unreachable!(),
            };
            if (load_count % p.lmdb.load_batch) == 0 {
                txn.commit().unwrap();
                txn = env.begin_rw_txn().unwrap();
            }
            if lstats.is_sec_elapsed() {
                stats!(&p.cmd_opts, "ixperf", "initial periodic-stats\n{}", lstats);
                fstats.merge(&lstats);
                lstats = stats::Ops::new();
            }
        }

        txn.commit().unwrap();
        fstats.merge(&lstats);
        unsafe { env.close_db(db) };
        env.sync(true).unwrap();
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    let stat = {
        let (env, _) = open_lmdb(&p, "lmdb");
        env.stat().unwrap()
    };
    stats!(&p.cmd_opts, "ixperf", "initial stats\n{:?}", fstats);
    info!(
        target: "ixperf",
        "initial-load load:{} index.len:{} elapsed:{:?}",
        p.g.loads, stat.entries(), elapsed
    );

    fstats
}

fn do_incremental(
    p: &Profile,
    env: lmdb::Environment,
    db: lmdb::Database, // lmdb index
) -> stats::Ops {
    if (p.g.read_ops() + p.g.write_ops()) == 0 {
        return stats::Ops::new();
    }

    let write_flags: lmdb::WriteFlags = Default::default();
    let mut fstats = stats::Ops::new();
    let elapsed = {
        let start = SystemTime::now();
        let mut lstats = stats::Ops::new();
        let gen = IncrementalLoad::<Vec<u8>, Vec<u8>>::new(p.g.clone());
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Set { key, value } => {
                    lstats.set.sample_start(false);
                    let mut txn = env.begin_rw_txn().unwrap();
                    txn.put(db, &key, &value, write_flags.clone()).unwrap();
                    txn.commit().unwrap();
                    lstats.set.sample_end(0);
                }
                Cmd::Delete { key } => {
                    lstats.delete.sample_start(false);
                    let mut txn = env.begin_rw_txn().unwrap();
                    let n = match txn.del(db, &key, None /*data*/) {
                        Ok(_) => 0,
                        Err(lmdb::Error::NotFound) => 1,
                        res @ _ => panic!("lmdb del: {:?}", res),
                    };
                    txn.commit().unwrap();
                    lstats.delete.sample_end(n);
                }
                Cmd::Get { key } => {
                    lstats.get.sample_start(false);
                    let txn = env.begin_ro_txn().unwrap();
                    let n = match txn.get(db, &key) {
                        Ok(_) => 0,
                        Err(lmdb::Error::NotFound) => 1,
                        Err(err) => panic!(err),
                    };
                    lstats.get.sample_end(n);
                }
                Cmd::Range { low, high } => {
                    let txn = env.begin_ro_txn().unwrap();
                    let mut cur = txn.open_ro_cursor(db).unwrap();
                    let iter = match low {
                        Bound::Included(low) => cur.iter_from(low.clone()),
                        Bound::Excluded(low) => cur.iter_from(low.clone()),
                        _ => cur.iter(),
                    };

                    let mut iter_count = 0;
                    for (key, _) in iter {
                        match high {
                            Bound::Included(h) if key.gt(&h) => break,
                            Bound::Excluded(h) if key.ge(&h) => break,
                            _ => iter_count += 1,
                        };
                    }

                    lstats.range.sample_start(true);
                    lstats.range.sample_end(iter_count);
                }
                Cmd::Reverse { .. } => (),
                _ => unreachable!(),
            };
            if lstats.is_sec_elapsed() {
                stats!(
                    p.cmd_opts,
                    "ixperf",
                    "incremental periodic-stats\n{}",
                    lstats
                );
                fstats.merge(&lstats);
                lstats = stats::Ops::new();
            }
        }
        fstats.merge(&lstats);
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    let stat = {
        let (env, _) = open_lmdb(&p, "lmdb");
        env.stat().unwrap()
    };
    stats!(&p.cmd_opts, "ixperf", "incremental stats\n{:?}", fstats);
    info!(
        target: "ixperf",
        "incremental-load r_ops:{} w_ops:{} index.len:{}, elapsed:{:?}",
        p.g.read_ops(), p.g.write_ops(), stat.entries(), elapsed
    );

    fstats
}

fn do_write(
    i: usize,
    p: Profile,
    env: Arc<lmdb::Environment>,
    db: lmdb::Database, // index
) -> stats::Ops {
    if p.g.write_ops() == 0 {
        return stats::Ops::new();
    }

    let write_flags: lmdb::WriteFlags = Default::default();
    let mut fstats = stats::Ops::new();
    let elapsed = {
        let start = SystemTime::now();
        let mut lstats = stats::Ops::new();
        let gen = IncrementalWrite::<Vec<u8>, Vec<u8>>::new(p.g.clone());
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Set { key, value } => {
                    lstats.set.sample_start(false);
                    let mut txn = env.begin_rw_txn().unwrap();
                    txn.put(db, &key, &value, write_flags.clone()).unwrap();
                    txn.commit().unwrap();
                    lstats.set.sample_end(0);
                }
                Cmd::Delete { key } => {
                    lstats.delete.sample_start(false);
                    let mut txn = env.begin_rw_txn().unwrap();
                    let n = match txn.del(db, &key, None /*data*/) {
                        Ok(_) => 0,
                        Err(lmdb::Error::NotFound) => 1,
                        res @ _ => panic!("lmdb del: {:?}", res),
                    };
                    txn.commit().unwrap();
                    lstats.delete.sample_end(n);
                }
                _ => unreachable!(),
            };
            if lstats.is_sec_elapsed() {
                stats!(
                    &p.cmd_opts,
                    "ixperf",
                    "writer-{} periodic-stats\n{}",
                    i,
                    lstats
                );
                fstats.merge(&lstats);
                lstats = stats::Ops::new();
            }
        }
        fstats.merge(&lstats);
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    stats!(&p.cmd_opts, "ixperf", "writer-{} stats\n{:?}", i, fstats);
    info!(
        target: "ixperf", "writer-{} w_ops:{} elapsed:{:?}",
        i, p.g.write_ops(), elapsed
    );

    fstats
}

fn do_read(
    i: usize,
    p: Profile,
    env: Arc<lmdb::Environment>,
    db: lmdb::Database, // index handle
) -> stats::Ops {
    if p.g.read_ops() == 0 {
        return stats::Ops::new();
    }

    let mut fstats = stats::Ops::new();
    let elapsed = {
        let start = SystemTime::now();

        let mut lstats = stats::Ops::new();
        let gen = IncrementalRead::<Vec<u8>, Vec<u8>>::new(p.g.clone());
        for (_i, cmd) in gen.enumerate() {
            match cmd {
                Cmd::Get { key } => {
                    lstats.get.sample_start(false);
                    let txn = env.begin_ro_txn().unwrap();
                    let n = match txn.get(db, &key) {
                        Ok(_) => 0,
                        Err(lmdb::Error::NotFound) => 1,
                        Err(err) => panic!(err),
                    };
                    lstats.get.sample_end(n);
                }
                Cmd::Range { low, high } => {
                    let txn = env.begin_ro_txn().unwrap();
                    let mut cur = txn.open_ro_cursor(db).unwrap();
                    let iter = match low {
                        Bound::Included(low) => cur.iter_from(low.clone()),
                        Bound::Excluded(low) => cur.iter_from(low.clone()),
                        _ => cur.iter(),
                    };

                    let mut iter_count = 0;
                    for (key, _) in iter {
                        match high {
                            Bound::Included(h) if key.gt(&h) => break,
                            Bound::Excluded(h) if key.ge(&h) => break,
                            _ => iter_count += 1,
                        };
                    }

                    lstats.range.sample_start(true);
                    lstats.range.sample_end(iter_count);
                }
                Cmd::Reverse { .. } => (),
                _ => unreachable!(),
            };
            if lstats.is_sec_elapsed() {
                stats!(
                    &p.cmd_opts,
                    "ixperf",
                    "reader-{} periodic-stats\n{}",
                    i,
                    lstats
                );
                fstats.merge(&lstats);
                lstats = stats::Ops::new();
            }
        }
        fstats.merge(&lstats);
        Duration::from_nanos(start.elapsed().unwrap().as_nanos() as u64)
    };

    stats!(&p.cmd_opts, "ixperf", "reader-{} stats\n{:?}", i, fstats);
    info!(
        target: "ixperf", "reader-{} r_ops:{} elapsed:{:?}",
        i, p.g.read_ops(), elapsed
    );

    fstats
}

fn init_lmdb(p: &Profile, name: &str) -> (lmdb::Environment, lmdb::Database) {
    // setup directory
    match std::fs::remove_dir_all(&p.lmdb.dir) {
        Ok(()) => (),
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => (),
        Err(err) => panic!("{:?}", err),
    }
    let path = std::path::Path::new(&p.lmdb.dir).join(name);
    std::fs::create_dir_all(&path).unwrap();

    // create the environment
    let mut flags = lmdb::EnvironmentFlags::empty();
    flags.insert(lmdb::EnvironmentFlags::NO_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_META_SYNC);
    let env = lmdb::Environment::new()
        .set_flags(flags)
        .set_map_size(10_000_000_000)
        .open(&path)
        .unwrap();

    let db = env.open_db(None).unwrap();

    (env, db)
}

fn open_lmdb(p: &Profile, name: &str) -> (lmdb::Environment, lmdb::Database) {
    let path = std::path::Path::new(&p.lmdb.dir).join(name);

    // create the environment
    let mut flags = lmdb::EnvironmentFlags::empty();
    flags.insert(lmdb::EnvironmentFlags::NO_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_META_SYNC);
    flags.insert(lmdb::EnvironmentFlags::NO_TLS);
    let env = {
        let mut env = lmdb::Environment::new();
        env.set_flags(flags).set_map_size(10_000_000_000);
        if p.lmdb.readers > 0 {
            env.set_max_readers(p.lmdb.readers as u32);
        }
        env.open(&path).unwrap()
    };

    let db = env.open_db(None).unwrap();

    (env, db)
}
