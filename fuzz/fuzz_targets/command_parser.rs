#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let frame = resp_server::codec::RespFrame::Array(
        data.split(|b| *b == 0)
            .take(100)
            .map(|p| resp_server::codec::RespFrame::Bulk(Some(p.to_vec())))
            .collect(),
    );
    let _ = resp_server::commands::parse(&frame);
});
