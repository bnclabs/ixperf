use std::{
    fmt,
    time::{Duration, SystemTime},
};

pub struct Latency {
    name: String,
    samples: usize,
    total: Duration,
    start: SystemTime,
    min: u128,
    max: u128,
    latencies: Vec<usize>, // NOTE: large value, can't be in stack.
}

impl Default for Latency {
    fn default() -> Latency {
        let mut lat = Latency {
            name: "".to_string(),
            samples: Default::default(),
            total: Default::default(),
            start: SystemTime::now(),
            min: std::u128::MAX,
            max: std::u128::MIN,
            latencies: Vec::with_capacity(1_000_000),
        };
        lat.latencies.resize(lat.latencies.capacity(), 0);
        lat
    }
}

impl Latency {
    pub fn new(name: &str) -> Latency {
        let mut latency: Latency = Default::default();
        latency.name = name.to_string();
        latency
    }

    pub fn start(&mut self) {
        self.samples += 1;
        self.start = SystemTime::now();
    }

    pub fn stop(&mut self) {
        let elapsed = self.start.elapsed().unwrap().as_nanos();
        self.min = std::cmp::min(self.min, elapsed);
        self.max = std::cmp::max(self.max, elapsed);
        let latency = (elapsed / 100) as usize;
        let ln = self.latencies.len();
        if latency < ln {
            self.latencies[latency] += 1;
        } else {
            self.latencies[ln - 1] += 1;
        }
        self.total += Duration::from_nanos(elapsed as u64);
    }

    pub fn to_percentiles(&self) -> Vec<(u8, u128)> {
        let mut percentiles: Vec<(u8, u128)> = vec![];
        let (mut acc, mut prev_perc) = (0_f64, 90_u8);
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

    pub fn to_mean(&self) -> u128 {
        self.total.as_nanos() / (self.samples as u128)
    }

    pub fn merge(&mut self, other: &Self) {
        self.samples += other.samples;
        self.total += other.total;
        self.min = std::cmp::min(self.min, other.min);
        self.max = std::cmp::max(self.max, other.max);
        self.latencies
            .iter_mut()
            .zip(other.latencies.iter())
            .for_each(|(x, y)| *x = *x + *y);
    }

    #[allow(dead_code)] // TODO: remove this once ixperf stabilizes.
    pub fn to_json(&self) -> String {
        let total = self.total.as_nanos();
        let rate = (self.samples as u128) / (total / 1_000_000_000);
        let ps: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(p, ns)| format!(r#""{}": {}"#, p, (ns * 100)))
            .collect();
        let strs = [
            format!(r#""n": {}"#, self.samples),
            format!(r#""elapsed": {}"#, total),
            format!(r#""rate": {}"#, rate),
            format!(r#""min": {}"#, self.min),
            format!(r#""mean": {}"#, self.to_mean()),
            format!(r#""max": {}"#, self.max),
            format!(r#""latencies": {{ {} }}"#, ps.join(", ")),
        ];
        ("{ ".to_string() + &strs.join(", ") + " }").to_string()
    }
}

impl fmt::Display for Latency {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let total = self.total.as_nanos();
        let rate = (self.samples as f64) / (total as f64 / 1_000_000_000.0);
        let props: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(perc, latn)| format!(r#""{}"={}"#, perc, (latn * 100)))
            .collect();
        let latencies = props.join(", ");
        write!(
            f,
            concat!(
                "{{ n={}, elapsed={}, rate={}, min={}, ",
                "mean={}, max={}, latencies={{ {} }} }}"
            ),
            self.samples,
            self.total.as_nanos(),
            rate as u64,
            self.min,
            self.max,
            self.to_mean(),
            latencies,
        )
    }
}

impl fmt::Debug for Latency {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let total = self.total.as_nanos();
        let rate = (self.samples as f64) / (total as f64 / 1_000_000_000.0);
        let props: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(perc, latn)| {
                let latn = (latn * 100) as u64;
                format!(r#""{}"={:?}"#, perc, Duration::from_nanos(latn))
            })
            .collect();
        let latencies = props.join(", ");
        write!(
            f,
            "{}.latency = {{ n={}, min={:?}, mean={:?}, max={:?} }}\n",
            self.name,
            self.samples,
            Duration::from_nanos(self.min as u64),
            Duration::from_nanos(self.to_mean() as u64),
            Duration::from_nanos(self.max as u64)
        )?;
        write!(
            f,
            "{}.latency.percentiles = {{ {} }}\n",
            self.name, latencies
        )?;
        write!(f, "rate: {}/sec", rate as u64)
    }
}
