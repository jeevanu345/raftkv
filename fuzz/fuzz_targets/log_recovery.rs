#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let cfg = raft_storage::segmented_log::SegmentedLogConfig {
        dir: dir.path().to_owned(),
        max_segment_bytes: 2 * 1024 * 1024,
        sync_each_append: false,
    };
    let entries = (1..=3)
        .map(|index| raft_core::Entry::normal(1, index, vec![index as u8; 32]))
        .collect::<Vec<_>>();
    {
        let log = raft_storage::SegmentedLog::open(cfg.clone()).unwrap();
        log.append(&entries).unwrap();
    }
    let file = dir.path().join("00000000000000000001.log");
    let mut bytes = std::fs::read(&file).unwrap();
    let mode = data.first().copied().unwrap_or(0) % 4;
    match mode {
        0 => {
            let at = data.get(1).copied().unwrap_or(0) as usize % (bytes.len() + 1);
            bytes.truncate(at);
        }
        1 => {
            let at = data.get(1).copied().unwrap_or(0) as usize % bytes.len();
            bytes[at] ^= data.get(2).copied().unwrap_or(1).max(1);
        }
        2 => bytes.extend_from_slice(data),
        _ => bytes = data.to_vec(),
    }
    std::fs::write(&file, bytes).unwrap();
    if let Ok(log) = raft_storage::SegmentedLog::open(cfg) {
        // Corruption may discard a tail, but must never synthesize new commands.
        if mode < 2 {
            assert!(log.last_index() <= 3);
        }
        for index in 1..=log.last_index() {
            let entry = log.read(index).unwrap().unwrap();
            assert_eq!(entry.index, index);
            if mode < 3 && index <= 3 {
                assert_eq!(entry, entries[index as usize - 1]);
            }
        }
    }
});
