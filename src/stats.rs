use std::time::Duration;

use crate::latency::Latency;

pub struct Op {
    pub name: String,
    pub latency: Latency,
    pub count: usize,
    pub items: usize,
}

impl Op {
    fn pretty_print(&self, p: &str) {
        if self.count == 0 {
            return;
        }

        let (c, i) = (self.count, self.items);
        match self.name.as_str() {
            "load" => {
                println!("{}load ops {}, updates {}", p, c, i);
            }
            "create" => {
                println!("{}create ops {}, updates {}", p, c, i);
            }
            "set" => {
                println!("{}set ops {}, inserts {}", p, c, i);
            }
            "delete" => {
                println!("{}delete ops {}, missing {}", p, c, i);
            }
            "get" => {
                println!("{}get ops {}, missing {}", p, c, i);
            }
            "iter" => {
                let t = self.latency.mean() * (self.latency.samples() as u128);
                let ns = t / (self.items as u128);
                let dur = Duration::from_nanos(ns as u64);
                println!("{}iter ops {}, items {} mean {:?}", p, c, i, dur);
            }
            "range" => {
                let t = self.latency.mean() * (self.latency.samples() as u128);
                let ns = t / (self.items as u128);
                let dur = Duration::from_nanos(ns as u64);
                println!("{}range ops {}, items {} mean {:?}", p, c, i, dur);
            }
            "reverse" => {
                let t = self.latency.mean() * (self.latency.samples() as u128);
                let ns = t / (self.items as u128);
                let dur = Duration::from_nanos(ns as u64);
                println!("{}revese ops {}, items {} mean {:?}", p, c, i, dur);
            }
            _ => unreachable!(),
        }
        self.latency.pretty_print(p);
    }

    pub fn json(&self) -> String {
        if self.count == 0 {
            return "".to_string();
        }

        let strs = [
            format!("count: {}", self.count),
            format!("items: {}", self.items),
            format!("latency: {}", self.latency.json()),
        ];
        let value = "{ ".to_string() + &strs.join(", ") + " }";
        format!("{}: {}", self.name, value)
    }
}

pub struct Ops {
    pub load: Op,
    pub create: Op,
    pub set: Op,
    pub delete: Op,
    pub get: Op,
    pub iter: Op,
    pub range: Op,
    pub reverse: Op,
}

impl Ops {
    pub fn new() -> Ops {
        let (count, items) = (0, 0);
        Ops {
            load: Op {
                name: "load".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
            create: Op {
                name: "create".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
            set: Op {
                name: "set".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
            delete: Op {
                name: "delete".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
            get: Op {
                name: "get".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
            iter: Op {
                name: "iter".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
            range: Op {
                name: "range".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
            reverse: Op {
                name: "reverse".to_string(),
                latency: Latency::new(),
                count,
                items,
            },
        }
    }

    pub fn pretty_print(&self, prefix: &str) {
        self.load.pretty_print(prefix);
        self.create.pretty_print(prefix);
        self.set.pretty_print(prefix);
        self.delete.pretty_print(prefix);
        self.get.pretty_print(prefix);
        self.iter.pretty_print(prefix);
        self.range.pretty_print(prefix);
        self.reverse.pretty_print(prefix);
    }

    pub fn json(&self) -> String {
        let strs = [
            self.load.json(),
            self.create.json(),
            self.set.json(),
            self.delete.json(),
            self.get.json(),
            self.iter.json(),
            self.range.json(),
            self.reverse.json(),
        ];
        let strs: Vec<String> = strs.iter().filter_map(|item| {
            if item.len() > 0 {
                Some(item.clone())
            } else {
                None
            }).collect();
        ("stats { ".to_string() + &strs.join(", ") + " }").to_string()
    }
}
