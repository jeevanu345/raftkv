#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    use bincode::Options;
    let _ = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(1024 * 1024)
        .reject_trailing_bytes()
        .deserialize::<kv_state_machine::Command>(data);
});
