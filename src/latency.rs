use std::time::{Duration, SystemTime};

pub struct Latency {
    count: u64,
    elapsed: u128,
    start: SystemTime,
    min: u128,
    max: u128,
    latencies: Vec<u64>,
}

impl Latency {
    pub fn new() -> Latency {
        let mut l = Latency {
            count: 0,
            elapsed: 0,
            start: SystemTime::now(),
            min: 0,
            max: 0,
            latencies: Vec::with_capacity(10_000_000),
        };
        (0..10_000_000).for_each(|_| l.latencies.push(0));
        l
    }

    pub fn start(&mut self) {
        self.count += 1;
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
            // larger than 1 second
            self.latencies[latency as usize] += 1;
        }
        self.elapsed += elapsed;
    }

    pub fn percentiles(&self) -> [(i32, u64); 7] {
        let total: u64 = self.latencies.iter().sum();
        let mut percentiles = [
            (80, 0_u64),
            (90, 0_u64),
            (95, 0_u64),
            (96, 0_u64),
            (97, 0_u64),
            (98, 0_u64),
            (99, 0_u64),
        ];
        let mut iter = percentiles.iter_mut();
        let mut item: &mut (i32, u64) = iter.next().unwrap();
        let mut acc = 0;
        for (latency, count) in self.latencies.iter().enumerate() {
            acc += count;
            if acc > (((total as f64) * ((item.0 as f64) / 100_f64)) as u64) {
                item.1 = latency as u64;
                match iter.next() {
                    Some(x) => item = x,
                    None => break,
                }
            }
        }
        percentiles
    }

    pub fn average(&self) -> u64 {
        ((self.elapsed as u64) / self.count) as u64
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn stats(&self) -> (u128, u128) {
        (self.min, self.max)
    }

    pub fn print_latency(&self, prefix: &str) {
        let (min, max) = self.stats();
        let arg1 = (
            Duration::from_nanos(min as u64),
            Duration::from_nanos(max as u64),
            Duration::from_nanos(self.average() as u64),
        );
        println!("{}latency (min, max, avg): {:?}", prefix, arg1);
        println!("{}latency percentiles ----", prefix);
        for (percentile, ns_cent) in self.percentiles().into_iter() {
            let ns = Duration::from_nanos((ns_cent * 100) as u64);
            println!("{}    {} percentile = {:?}", prefix, percentile, ns);
        }
    }
}
