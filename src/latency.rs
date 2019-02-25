use std::time::{Duration, SystemTime};

pub struct Latency {
    samples: usize,
    total: u128,
    start: SystemTime,
    min: u128,
    max: u128,
    latencies: Vec<usize>, // NOTE: large value, can't be in stack.
}

impl Latency {
    pub fn new() -> Latency {
        let mut lat = Latency {
            samples: 0,
            total: 0,
            start: SystemTime::now(),
            min: 0,
            max: 0,
            latencies: Vec::with_capacity(10_000_000),
        };
        lat.latencies.resize(10_000_000, 0);
        lat
    }

    pub fn start(&mut self) {
        self.samples += 1;
        self.start = SystemTime::now();
    }

    pub fn stop(&mut self) {
        let elapsed = self.start.elapsed().unwrap().as_nanos();
        if self.min == 0 || elapsed < self.min {
            self.min = elapsed
        }
        if self.min == 0 || elapsed > self.max {
            self.max = elapsed
        }
        let latency = elapsed / 100;
        if latency < 10_000_000 {
            self.latencies[latency as usize] += 1;
        } else {
            panic!("latency larger than one second");
        }
        self.total += elapsed;
    }

    pub fn percentiles(&self) -> [(i32, u128); 7] {
        let total: usize = self.latencies.iter().map(|x| *x).sum();
        let mut percentiles = [
            (80, 0_u128),
            (90, 0_u128),
            (95, 0_u128),
            (96, 0_u128),
            (97, 0_u128),
            (98, 0_u128),
            (99, 0_u128),
        ];
        let mut iter = percentiles.iter_mut();
        let mut item: &mut (i32, u128) = iter.next().unwrap();
        let mut acc = 0;
        for (latency, samples) in self.latencies.iter().enumerate() {
            acc += samples;
            if acc > (((total as f64) * ((item.0 as f64) / 100_f64)) as usize) {
                item.1 = latency as u128;
                match iter.next() {
                    Some(x) => item = x,
                    None => break,
                }
            }
        }
        percentiles
    }

    pub fn mean(&self) -> u128 {
        println!("{}", self.samples);
        self.total / (self.samples as u128)
    }

    pub fn samples(&self) -> usize {
        self.samples
    }

    pub fn pretty_print(&self, prefix: &str) {
        let arg1 = (
            Duration::from_nanos(self.min as u64),
            Duration::from_nanos(self.mean() as u64),
            Duration::from_nanos(self.max as u64),
        );
        println!("{}latency (min, avg, max): {:?}", prefix, arg1);
        for (percentile, ns_cent) in self.percentiles().into_iter() {
            let ns = Duration::from_nanos((ns_cent * 100) as u64);
            println!("{}  {} percentile = {:?}", prefix, percentile, ns);
        }
    }

    pub fn json(&self) -> String {
        let ps: Vec<String> = self
            .percentiles()
            .into_iter()
            .map(|(p, ns)| format!(r#""{}": {}"#, p, (ns * 100)))
            .collect();
        let strs = [
            format!("min: {}", self.min),
            format!("mean: {}", self.mean()),
            format!("max: {}", self.max),
            format!("percentiles: {{ {} }}", ps.join(", ")),
        ];
        ("{ ".to_string() + &strs.join(", ") + " }").to_string()
    }
}
