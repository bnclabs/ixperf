use std::time::{Duration, SystemTime};

use crate::latency::Latency;

pub struct Op {
    name: String,
    latency: Latency,
    count: usize,
    items: usize,
}

impl Op {
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

    fn pretty_print(&self, p: &str, fin: bool) {
        if self.count == 0 {
            return;
        }

        if fin == false {
            self.latency.pretty_print(p);
            return;
        }

        let (c, i) = (self.count, self.items);
        match self.name.as_str() {
            "load" => {
                println!("{}load ops {}, updates {}", p, c, i);
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
    pub set: Op,
    pub delete: Op,
    pub get: Op,
    pub iter: Op,
    pub range: Op,
    pub reverse: Op,
    start: SystemTime,
}

impl Ops {
    pub fn new() -> Ops {
        let (count, items) = (0, 0);
        Ops {
            load: Op {
                name: "load".to_string(),
                latency: Latency::new(),
                items,
                count,
            },
            set: Op {
                name: "set".to_string(),
                latency: Latency::new(),
                items,
                count,
            },
            delete: Op {
                name: "delete".to_string(),
                latency: Latency::new(),
                items,
                count,
            },
            get: Op {
                name: "get".to_string(),
                latency: Latency::new(),
                items,
                count,
            },
            iter: Op {
                name: "iter".to_string(),
                latency: Latency::new(),
                items,
                count,
            },
            range: Op {
                name: "range".to_string(),
                latency: Latency::new(),
                items,
                count,
            },
            reverse: Op {
                name: "reverse".to_string(),
                latency: Latency::new(),
                items,
                count,
            },
            start: SystemTime::now(),
        }
    }

    pub fn pretty_print(&self, prefix: &str, fin: bool) {
        if fin {
            let elapsed = self.start.elapsed().unwrap().as_nanos() as u64;
            let elapsed = elapsed / (self.total_ops() as u64);
            let avg_lat_per_op = Duration::from_nanos(elapsed);
            println!("average latency per op: {:?}", avg_lat_per_op);
        }

        self.load.pretty_print(prefix, fin);
        self.set.pretty_print(prefix, fin);
        self.delete.pretty_print(prefix, fin);
        self.get.pretty_print(prefix, fin);
        self.iter.pretty_print(prefix, fin);
        self.range.pretty_print(prefix, fin);
        self.reverse.pretty_print(prefix, fin);
    }

    pub fn json(&self) -> String {
        let strs = [
            self.load.json(),
            self.set.json(),
            self.delete.json(),
            self.get.json(),
            self.iter.json(),
            self.range.json(),
            self.reverse.json(),
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

    #[inline]
    pub fn total_ops(&self) -> usize {
        self.load.count
            + self.set.count
            + self.delete.count
            + self.get.count
            + self.iter.count
            + self.range.count
            + self.reverse.count
    }
}
