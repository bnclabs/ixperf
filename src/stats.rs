use std::fmt;

use crate::latency::Latency;

pub struct Op {
    pub name: String,
    pub latency: Latency,
    pub count: usize,
    pub items: usize,
    pub force: bool,
}

impl Op {
    pub fn new(name: &str) -> Op {
        Op {
            name: name.to_string(),
            latency: Latency::new(name),
            count: Default::default(),
            items: Default::default(),
            force: Default::default(),
        }
    }

    fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.items += other.items;
        self.latency.merge(&other.latency);
    }

    #[inline]
    pub fn sample_start(&mut self, force: bool) {
        self.count += 1;
        self.force = force;
        if force || (self.count % 8) == 0 {
            self.latency.start();
        }
    }

    #[inline]
    pub fn sample_end(&mut self, items: usize) {
        if self.force || (self.count % 8) == 0 {
            self.latency.stop();
        }
        self.items += items;
        self.force = false;
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
                "{} = {{ ops={}, updates={}",
                self.name, self.count, self.items
            )?,
            "delete" | "get" => write!(
                f,
                "{} = {{ ops={}, missing={}",
                self.name, self.count, self.items
            )?,
            "range" | "reverse" => write!(
                f,
                "{} = {{ ops={}, items={}",
                self.name, self.count, self.items
            )?,
            _ => unreachable!(),
        };
        if self.latency.to_samples() > 0 {
            write!(f, ", latency={} }}", self.latency)
        } else {
            write!(f, "}}")
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
                "{} = {{ ops={}, missing={} }}\n",
                self.name, self.count, self.items,
            )?,
            "range" | "reverse" => write!(
                f,
                "{} = {{ ops={}, items={} }}\n",
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
            range: Op::new("range"),
            reverse: Op::new("reverse"),
        }
    }

    pub fn to_total_reads(&self) -> usize {
        self.get.count + self.range.count + self.reverse.count
    }

    pub fn to_total_writes(&self) -> usize {
        self.load.count + self.set.count + self.delete.count
    }

    pub fn is_sec_elapsed(&self) -> bool {
        let mut elapsed = self.load.latency.elapsed() * 8;
        elapsed += self.set.latency.elapsed() * 8;
        elapsed += self.delete.latency.elapsed() * 8;
        elapsed += self.get.latency.elapsed() * 8;
        elapsed += self.range.latency.elapsed() * 8;
        elapsed += self.reverse.latency.elapsed() * 8;
        elapsed > 1_000_000_000
    }

    pub fn merge(&mut self, other: &Self) {
        self.load.merge(&other.load);
        self.set.merge(&other.set);
        self.delete.merge(&other.delete);
        self.get.merge(&other.get);
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
        let items: Vec<String> = [
            &self.load,
            &self.set,
            &self.delete,
            &self.get,
            &self.range,
            &self.reverse,
        ]
        .iter()
        .filter_map(|item| {
            if item.count > 0 {
                Some(format!("{}", item))
            } else {
                None
            }
        })
        .collect();
        write!(f, "{}", items.join("\n"))
    }
}

impl fmt::Debug for Ops {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut lines = vec![];
        if self.load.count > 0 {
            lines.push(format!("{:?}", self.load));
        }
        if self.set.count > 0 {
            lines.push(format!("{:?}", self.set));
        }
        if self.delete.count > 0 {
            lines.push(format!("{:?}", self.delete));
        }
        if self.get.count > 0 {
            lines.push(format!("{:?}", self.get));
        }
        if self.range.count > 0 {
            lines.push(format!("{:?}", self.range));
        }
        if self.reverse.count > 0 {
            lines.push(format!("{:?}", self.reverse));
        }
        write!(f, "{}", lines.join("\n"))
    }
}
