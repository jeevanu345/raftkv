#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = kv_state_machine::KvStateMachine::validate_snapshot(data, 0);
    let _ = raft_storage::backup::Backup::decode(data);
});
