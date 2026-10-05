use criterion::{black_box, criterion_group, criterion_main, Criterion};
fn benchmarks(c: &mut Criterion) {
    let mut node = raft_core::RaftNode::new(
        raft_core::RaftConfig::default(),
        raft_core::HardState::default(),
        raft_core::RaftLog::new(),
    );
    c.bench_function("raft_tick", |b| b.iter(|| black_box(node.tick())));
    c.bench_function("raft_step", |b| {
        b.iter(|| {
            black_box(node.step(raft_core::Message::AppendEntries {
                from: 2,
                term: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                request_id: 1,
            }))
        })
    });
    let entry = raft_core::Entry::normal(1, 1, vec![1; 1024]);
    c.bench_function("log_serialization", |b| {
        b.iter(|| bincode::serialize(black_box(&entry)).unwrap())
    });
    c.bench_function("crc_framing", |b| {
        b.iter(|| crc32c::crc32c(black_box(&entry.data)))
    });
    let dir = tempfile::tempdir().unwrap();
    let sm = kv_state_machine::KvStateMachine::open(dir.path().join("kv")).unwrap();
    let mut index = 0;
    let set = kv_state_machine::Command::Set {
        key: b"key".to_vec(),
        value: vec![1; 1024],
        expire_at_ms: None,
    };
    c.bench_function("state_machine_set_durable", |b| {
        b.iter(|| {
            index += 1;
            sm.apply(index, &set).unwrap()
        })
    });
    c.bench_function("state_machine_get", |b| {
        b.iter(|| sm.get(black_box(b"key")).unwrap())
    });
    c.bench_function("state_machine_incr_durable", |b| {
        b.iter(|| {
            index += 1;
            sm.apply(
                index,
                &kv_state_machine::Command::Incr {
                    key: b"counter".to_vec(),
                    delta: 1,
                },
            )
            .unwrap()
        })
    });
    c.bench_function("snapshot_serialization", |b| {
        b.iter(|| sm.snapshot().unwrap())
    });
    let log = raft_storage::SegmentedLog::open(raft_storage::segmented_log::SegmentedLogConfig {
        dir: dir.path().join("log"),
        max_segment_bytes: 1024 * 1024,
        sync_each_append: true,
    })
    .unwrap();
    let mut log_index = 0;
    c.bench_function("append_entry_durable", |b| {
        b.iter(|| {
            log_index += 1;
            log.append(&[raft_core::Entry::normal(1, log_index, vec![1; 64])])
                .unwrap()
        })
    });
    let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    c.bench_function("resp_decode_parse_encode", |b| {
        b.iter(|| {
            let mut codec = resp_server::codec::RespCodec;
            let mut data = bytes::BytesMut::from(input.as_slice());
            let frame = tokio_util::codec::Decoder::decode(&mut codec, &mut data)
                .unwrap()
                .unwrap();
            let _ = resp_server::commands::parse(&frame);
            tokio_util::codec::Encoder::encode(&mut codec, frame, &mut data).unwrap();
        })
    });
}
criterion_group!(benches, benchmarks);
criterion_main!(benches);
