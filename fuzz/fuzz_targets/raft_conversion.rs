#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    use prost::Message;
    if let Ok(request) = raft_net::pb::raft::AppendEntriesRequest::decode(data) {
        let message = raft_net::convert::append_request_from_pb(request);
        let _ = raft_net::convert::append_request_to_pb(&message);
    }
});
