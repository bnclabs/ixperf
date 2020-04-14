**Performance measurement for [llrb-index][llrbindex] instances**

* [x] Latency of a operations in min, max, average and in percentile.
* [x] Throughput of operations.

**Enable cpuprofile**

```bash
cargo build --features cpuprofile
```

**Profile with different key-types and value-types**

```bash
cargo build --features all_types
```

[llrbindex]: http://github.com/bnclabs/llrb-index
