use std::fmt;

use crate::latency::Latency;

pub struct Op {
    name: String,
    latency: Latency,
    count: usize,
    items: usize,
}

impl Op {
    pub fn new(name: &str) -> Op {
        Op {
            name: name.to_string(),
            latency: Latency::new(name),
            count: Default::default(),
            items: Default::default(),
        }
    }

    fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.items += other.items;
        self.latency.merge(&other.latency);
    }

    #[inline]
    pub fn sample_start(&mut self) {
        self.count += 1;
        if (self.count % 8) == 0 {
            self.latency.start();
        }
    }

    #[inline]
    pub fn sample_end(&mut self, items: usize) {
        if (self.count % 8) == 0 {
            self.latency.stop();
        }
        self.items += items;
    }

    pub fn to_json(&self) -> String {
        if self.count == 0 {
            return "".to_string();
        }
        match self.name.as_str() {
            "load" => format!(
                r#""load": {{ "ops": {}, "updates": {}, "latency": {}}}"#,
                self.count, self.items, self.latency
            ),
            "set" => format!(
                r#""set": {{ "ops": {}, "updates": {}, "latency": {}}}"#,
                self.count, self.items, self.latency
            ),
            "delete" => format!(
                r#""delete": {{ "ops": {}, "updates": {}, "latency": {}}}"#,
                self.count, self.items, self.latency
            ),
            "get" => format!(
                r#""get": {{ "ops": {}, "updates": {}, "latency": {}}}"#,
                self.count, self.items, self.latency
            ),
            "iter" => format!(
                r#""iter": {{ "ops": {}, "updates": {}, "latency": {}}}"#,
                self.count, self.items, self.latency
            ),
            "range" => format!(
                r#""range": {{ "ops": {}, "updates": {}, "latency": {}}}"#,
                self.count, self.items, self.latency
            ),
            "reverse" => format!(
                r#""reverse": {{ "ops": {}, "updates": {}, "latency": {}}}"#,
                self.count, self.items, self.latency
            ),
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.count == 0 {
            return Ok(());
        }

        match self.name.as_str() {
            "load" | "set" => write!(
                f,
                "{} = {{ ops={}, updates={}, latency={} }}",
                self.name, self.count, self.items, self.latency
            ),
            "delete" | "get" => write!(
                f,
                "{} = {{ ops={}, missing={}, latency={} }}",
                self.name, self.count, self.items, self.latency
            ),
            "iter" | "range" | "reverse" => write!(
                f,
                "{} = {{ ops={}, items={}, latency={} }}",
                self.name, self.count, self.items, self.latency
            ),
            _ => unreachable!(),
        }
    }
}

impl fmt::Debug for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.count == 0 {
            return Ok(());
        }

        match self.name.as_str() {
            "load" | "set" => write!(
                f,
                "{} = {{ ops={}, updates={} }}\n",
                self.name, self.count, self.items,
            )?,
            "delete" | "get" => write!(
                f,
                "{} = {{ ops={}, missing={} }}",
                self.name, self.count, self.items,
            )?,
            "iter" | "range" | "reverse" => write!(
                f,
                "{} = {{ ops={}, items={} }}",
                self.name, self.count, self.items,
            )?,
            _ => unreachable!(),
        }
        write!(f, "{:?}", self.latency)
    }
}

pub struct Ops {
    pub load: Op,
    pub set: Op,
    pub delete: Op,
    pub get: Op,
    pub iter: Op,
    pub range: Op,
    pub reverse: Op,
}

impl Ops {
    pub fn new() -> Ops {
        Ops {
            load: Op::new("load"),
            set: Op::new("set"),
            delete: Op::new("delete"),
            get: Op::new("get"),
            iter: Op::new("iter"),
            range: Op::new("range"),
            reverse: Op::new("reverse"),
        }
    }

    #[allow(dead_code)] // TODO: remove this once ixperf stabilizes.
    pub fn to_total(&self) -> usize {
        self.load.count
            + self.set.count
            + self.delete.count
            + self.get.count
            + self.iter.count
            + self.range.count
            + self.reverse.count
    }

    pub fn merge(&mut self, other: &Self) {
        self.load.merge(&other.load);
        self.set.merge(&other.set);
        self.delete.merge(&other.delete);
        self.get.merge(&other.get);
        self.iter.merge(&other.iter);
        self.range.merge(&other.range);
        self.reverse.merge(&other.reverse);
    }

    #[allow(dead_code)] // TODO: remove this once ixperf stabilizes.
    pub fn to_json(&self) -> String {
        let strs = [
            self.load.to_json(),
            self.set.to_json(),
            self.delete.to_json(),
            self.get.to_json(),
            self.iter.to_json(),
            self.range.to_json(),
            self.reverse.to_json(),
        ];
        let strs: Vec<String> = strs
            .iter()
            .filter_map(|item| {
                if item.len() > 0 {
                    Some(item.clone())
                } else {
                    None
                }
            })
            .collect();
        ("stats { ".to_string() + &strs.join(", ") + " }").to_string()
    }
}

impl fmt::Display for Ops {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.load.count > 0 {
            write!(f, "{}", self.load)?;
        }
        if self.set.count > 0 {
            write!(f, "\n{}", self.set)?;
        }
        if self.delete.count > 0 {
            write!(f, "\n{}", self.delete)?;
        }
        if self.get.count > 0 {
            write!(f, "\n{}", self.get)?;
        }
        if self.iter.count > 0 {
            write!(f, "\n{}", self.iter)?;
        }
        if self.range.count > 0 {
            write!(f, "\n{}", self.range)?;
        }
        if self.reverse.count > 0 {
            write!(f, "\n{}", self.reverse)?;
        }
        Ok(())
    }
}

impl fmt::Debug for Ops {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.load.count > 0 {
            write!(f, "{:?}", self.load)?;
        }
        if self.set.count > 0 {
            write!(f, "\n{:?}", self.set)?;
        }
        if self.delete.count > 0 {
            write!(f, "\n{:?}", self.delete)?;
        }
        if self.get.count > 0 {
            write!(f, "\n{:?}", self.get)?;
        }
        if self.iter.count > 0 {
            write!(f, "\n{:?}", self.iter)?;
        }
        if self.range.count > 0 {
            write!(f, "\n{:?}", self.range)?;
        }
        if self.reverse.count > 0 {
            write!(f, "\n{:?}", self.reverse)?;
        }
        Ok(())
    }
}
