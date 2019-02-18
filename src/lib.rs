use std::fmt::{self, Display};

use rand::{rngs::SmallRng, Rng};
use structopt::StructOpt;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum Error {
    TypeError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?})", self)
    }
}

#[derive(Debug, StructOpt, Clone)]
pub(crate) struct CmdOpt {
    pub(crate) command: String,

    #[structopt(long = "working-set", default_value = "1.0")]
    pub(crate) working_set: f64,

    #[structopt(long = "load", default_value = "10000000")]
    pub(crate) load: u64,

    #[structopt(long = "ktype", default_value = "u64")]
    pub(crate) ktype: String,

    #[structopt(long = "vtype", default_value = "u64")]
    pub(crate) vtype: String,

    #[structopt(long = "lsm")]
    pub(crate) lsm: bool,

    #[structopt(long = "seed", default_value = "0")]
    pub(crate) seed: u128,

    #[structopt(long = "ops", default_value = "1000000000")]
    pub(crate) ops: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Opt {
    pub(crate) cmdopt: CmdOpt,
    pub(crate) keysize: usize,
    pub(crate) valsize: usize,
}

impl Opt {
    pub(crate) fn new() -> Opt {
        let cmdopt = CmdOpt::from_args();
        let mut opt = Opt {
            cmdopt,
            keysize: 0,
            valsize: 0,
        };
        opt.keysize = match opt.cmdopt.ktype.as_bytes()[0] as char {
            'b' => opt.cmdopt.ktype[1..].parse().unwrap(),
            _ => 0,
        };
        opt.valsize = match opt.cmdopt.vtype.as_bytes()[0] as char {
            'b' => opt.cmdopt.vtype[1..].parse().unwrap(),
            _ => 0,
        };
        opt
    }

    pub(crate) fn gen_key(&self, rng: &mut SmallRng) -> Vec<u8> {
        let mut key: Vec<u8> = Vec::with_capacity(self.keysize);
        let key_slice: &mut [u8] = key.as_mut();
        rng.fill(key_slice);
        key
    }

    pub(crate) fn gen_val(&mut self, rng: &mut SmallRng) -> Vec<u8> {
        let mut val: Vec<u8> = Vec::with_capacity(self.valsize);
        let val_slice: &mut [u8] = val.as_mut();
        rng.fill(val_slice);
        val
    }
}
