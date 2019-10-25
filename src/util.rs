use toml;

fn toml_to_usize(val: &toml::Value) -> usize {
    val.as_integer().map_or(0, |x| x.try_into().unwrap())
}

fn toml_to_string(val: &toml::Value) -> String {
    val.as_str().map_or("", |x| x).to_string()
}
