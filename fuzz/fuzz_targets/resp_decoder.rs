#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let mut decoder = resp_server::codec::RespCodec;
    let mut bytes = bytes::BytesMut::from(data);
    let _ = tokio_util::codec::Decoder::decode(&mut decoder, &mut bytes);
});
