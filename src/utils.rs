use std::convert::TryInto;

use toml;

pub fn toml_to_usize(val: &toml::Value) -> usize {
    val.as_integer()
        .map_or(Default::default(), |x| x.try_into().unwrap())
}

pub fn toml_to_bool(val: &toml::Value) -> bool {
    val.as_bool().unwrap()
}

pub fn toml_to_u128(val: &toml::Value) -> u128 {
    val.as_integer()
        .map_or(Default::default(), |x| x.try_into().unwrap())
}

pub fn toml_to_string(val: &toml::Value) -> String {
    val.as_str().map_or(Default::default(), |x| x).to_string()
}
