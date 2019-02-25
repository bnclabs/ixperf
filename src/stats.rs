use std::time::Duration;

use crate::latency::Latency;

pub struct OpStat {
    pub name: String,
    pub latency: Latency,
    pub count: u64,
    pub items: u64,
}

pub fn init_stats() -> [OpStat; 7] {
    let (count, items) = (0, 0);
    [
        OpStat {
            name: "create".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "set".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "delete".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "get".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "iter".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "range".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
        OpStat {
            name: "reverse".to_string(),
            latency: Latency::new(),
            count,
            items,
        },
    ]
}

pub fn print_stats(op_stats: &[OpStat; 7]) {
    for s in op_stats.iter() {
        if s.count == 0 {
            continue;
        }
        match s.name.as_str() {
            "create" | "set" | "delete" | "get" if s.count > 0 => {
                println!("{} ops {}", s.name, s.count);
            }
            "iter" | "range" | "reverse" if s.count > 0 => {
                println!("{} ops {}, items: {}", s.name, s.count, s.items);
                let dur = Duration::from_nanos((s.latency.average() * s.latency.count()) / s.items);
                println!("    average latency per item: {:?}", dur);
            }
            _ => unreachable!(),
        }
        s.latency.print_latency("    ");
    }
}
