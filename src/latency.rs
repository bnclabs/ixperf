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
            latencies: Vec::with_capacity(1_000_000),
        };
        lat.latencies.resize(1_000_000, 0);
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
        let latency = (elapsed / 100) as usize;
        let ln = self.latencies.len();
        if latency < ln {
            self.latencies[latency] += 1;
        } else {
            self.latencies[ln-1] += 1;
        }
        self.total += elapsed;
    }

    pub fn percentiles(&self) -> Vec<(u8, u128)> {
        let mut percentiles: Vec<(u8, u128)> = vec![];
        let (mut acc, mut prev_perc) = (0_f64, 89_u8);
        let iter = self.latencies.iter().enumerate().filter(|(_, &x)| x > 0);
        for (latency, &samples) in iter {
            acc += samples as f64;
            let perc = ((acc / (self.samples as f64)) * 100_f64) as u8;
            if perc > prev_perc {
                percentiles.push((perc, latency as u128));
                prev_perc = perc;
            }
        }
        percentiles
    }

    pub fn mean(&self) -> u128 {
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
