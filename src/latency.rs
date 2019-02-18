use std::time::SystemTime;

pub(crate) struct Latency {
    count: u64,
    start: SystemTime,
    min: u64,
    max: u64,
    latencies: Vec<u64>,
}

impl Latency {
    pub(crate) fn new() -> Latency {
        let mut l = Latency {
            count: 0,
            start: SystemTime::now(),
            min: 0,
            max: 0,
            latencies: Vec::with_capacity(10_000_000),
        };
        (0..10_000_000).for_each(|_| l.latencies.push(0));
        l
    }

    pub(crate) fn start(&mut self) {
        self.count += 1;
        self.start = SystemTime::now();
    }

    pub(crate) fn stop(&mut self) {
        let elapsed = self.start.elapsed().unwrap().as_nanos() as u64;
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
    }

    pub(crate) fn percentiles(&self) -> [(i32, u64); 14] {
        let total: u64 = self.latencies.iter().sum();
        let mut percentiles = [
            (10, 0_u64),
            (20, 0_u64),
            (30, 0_u64),
            (40, 0_u64),
            (50, 0_u64),
            (60, 0_u64),
            (70, 0_u64),
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

    pub(crate) fn stats(&self) -> (u64, u64) {
        (self.min, self.max)
    }
}
