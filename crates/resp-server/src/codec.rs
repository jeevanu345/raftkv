//! RESP2 codec. Decodes inline + multibulk; encodes simple/error/integer/bulk/array.

use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// A single RESP frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespFrame {
    /// `+...\r\n`
    Simple(String),
    /// `-...\r\n`
    Error(String),
    /// `:N\r\n`
    Integer(i64),
    /// `$N\r\n<N bytes>\r\n` or nil.
    Bulk(Option<Vec<u8>>),
    /// `*N\r\n...`
    Array(Vec<RespFrame>),
}

impl RespFrame {
    /// Convenience: build a simple OK reply.
    pub fn ok() -> Self { Self::Simple("OK".into()) }
    /// Convenience: build a nil bulk reply.
    pub fn nil() -> Self { Self::Bulk(None) }
    /// Convenience: build an error.
    pub fn err(s: impl Into<String>) -> Self { Self::Error(s.into()) }
    /// Convenience: build an integer.
    pub fn int(n: i64) -> Self { Self::Integer(n) }
    /// Convenience: build a bulk string.
    pub fn bulk(b: impl Into<Vec<u8>>) -> Self { Self::Bulk(Some(b.into())) }
}

/// Tokio codec implementing RESP2.
#[derive(Default, Clone)]
pub struct RespCodec;

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn parse_int(buf: &[u8]) -> io::Result<i64> {
    std::str::from_utf8(buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 integer"))?
        .parse::<i64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad integer"))
}

fn try_decode(buf: &[u8]) -> io::Result<Option<(RespFrame, usize)>> {
    if buf.is_empty() { return Ok(None); }
    match buf[0] {
        b'+' | b'-' | b':' | b'$' | b'*' => decode_typed(buf),
        _ => decode_inline(buf),
    }
}

fn decode_typed(buf: &[u8]) -> io::Result<Option<(RespFrame, usize)>> {
    let kind = buf[0];
    let line_end = match find_crlf(&buf[1..]) {
        Some(p) => p + 1,
        None => return Ok(None),
    };
    let body = &buf[1..line_end];
    match kind {
        b'+' => {
            let s = std::str::from_utf8(body)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8"))?
                .to_string();
            Ok(Some((RespFrame::Simple(s), line_end + 2)))
        }
        b'-' => {
            let s = std::str::from_utf8(body)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8"))?
                .to_string();
            Ok(Some((RespFrame::Error(s), line_end + 2)))
        }
        b':' => {
            let n = parse_int(body)?;
            Ok(Some((RespFrame::Integer(n), line_end + 2)))
        }
        b'$' => {
            let n = parse_int(body)?;
            let after_hdr = line_end + 2;
            if n < 0 { return Ok(Some((RespFrame::Bulk(None), after_hdr))); }
            let n_us = n as usize;
            if buf.len() < after_hdr + n_us + 2 { return Ok(None); }
            let payload = buf[after_hdr..after_hdr + n_us].to_vec();
            Ok(Some((RespFrame::Bulk(Some(payload)), after_hdr + n_us + 2)))
        }
        b'*' => {
            let n = parse_int(body)?;
            let mut consumed = line_end + 2;
            if n < 0 { return Ok(Some((RespFrame::Array(vec![]), consumed))); }
            let mut out = Vec::with_capacity(n as usize);
            for _ in 0..n {
                match try_decode(&buf[consumed..])? {
                    Some((f, c)) => { out.push(f); consumed += c; }
                    None => return Ok(None),
                }
            }
            Ok(Some((RespFrame::Array(out), consumed)))
        }
        _ => unreachable!(),
    }
}

fn decode_inline(buf: &[u8]) -> io::Result<Option<(RespFrame, usize)>> {
    let line_end = match find_crlf(buf) {
        Some(p) => p,
        None => return Ok(None),
    };
    let line = &buf[..line_end];
    let mut parts: Vec<RespFrame> = Vec::new();
    let s = std::str::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 inline"))?;
    for tok in s.split_whitespace() {
        parts.push(RespFrame::Bulk(Some(tok.as_bytes().to_vec())));
    }
    Ok(Some((RespFrame::Array(parts), line_end + 2)))
}

impl Decoder for RespCodec {
    type Item = RespFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match try_decode(src.as_ref())? {
            Some((frame, consumed)) => {
                src.advance(consumed);
                Ok(Some(frame))
            }
            None => Ok(None),
        }
    }
}

fn encode_into(frame: &RespFrame, out: &mut BytesMut) {
    match frame {
        RespFrame::Simple(s) => {
            out.put_u8(b'+');
            out.extend_from_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        RespFrame::Error(s) => {
            out.put_u8(b'-');
            out.extend_from_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        RespFrame::Integer(n) => {
            out.put_u8(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.put_slice(b"\r\n");
        }
        RespFrame::Bulk(None) => {
            out.put_slice(b"$-1\r\n");
        }
        RespFrame::Bulk(Some(b)) => {
            out.put_u8(b'$');
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            out.extend_from_slice(b);
            out.put_slice(b"\r\n");
        }
        RespFrame::Array(arr) => {
            out.put_u8(b'*');
            out.extend_from_slice(arr.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            for f in arr { encode_into(f, out); }
        }
    }
}

impl Encoder<RespFrame> for RespCodec {
    type Error = io::Error;
    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode_into(&item, dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(f: RespFrame) {
        let mut codec = RespCodec;
        let mut buf = BytesMut::new();
        codec.encode(f.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, f);
    }

    #[test]
    fn roundtrip_all() {
        roundtrip(RespFrame::Simple("OK".into()));
        roundtrip(RespFrame::Error("ERR foo".into()));
        roundtrip(RespFrame::Integer(42));
        roundtrip(RespFrame::Bulk(None));
        roundtrip(RespFrame::Bulk(Some(b"hello".to_vec())));
        roundtrip(RespFrame::Array(vec![
            RespFrame::Bulk(Some(b"SET".to_vec())),
            RespFrame::Bulk(Some(b"k".to_vec())),
            RespFrame::Bulk(Some(b"v".to_vec())),
        ]));
    }

    #[test]
    fn inline_command_decodes() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from(&b"PING\r\n"[..]);
        let f = codec.decode(&mut buf).unwrap().unwrap();
        match f {
            RespFrame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                if let RespFrame::Bulk(Some(b)) = &parts[0] { assert_eq!(b, b"PING"); }
                else { panic!("not bulk"); }
            }
            _ => panic!("not array"),
        }
    }
}
