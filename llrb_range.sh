# fix mod_llrb.rs for range case, to iterate just 10 or 20 or 100 entries.
cargo run --release -- llrb --type u64 --load 1000000 --sets 0 --deletes 0 --gets 0 --ranges 100000
