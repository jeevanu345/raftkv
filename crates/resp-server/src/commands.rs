//! Parse RESP frames into commands.

use crate::codec::RespFrame;

/// Parsed RESP command.
#[derive(Debug, Clone)]
pub enum ClientCommand {
    /// PING
    Ping(Option<Vec<u8>>),
    /// ECHO msg
    Echo(Vec<u8>),
    /// GET key
    Get(Vec<u8>),
    /// SET key value [EX seconds]
    Set {
        /// Key.
        key: Vec<u8>,
        /// Value.
        value: Vec<u8>,
        /// TTL seconds, if any.
        ex_seconds: Option<u64>,
    },
    /// DEL key1 key2 ...
    Del(Vec<Vec<u8>>),
    /// EXISTS key1 ...
    Exists(Vec<Vec<u8>>),
    /// INCR key
    Incr(Vec<u8>),
    /// DECR key
    Decr(Vec<u8>),
    /// MSET k1 v1 ...
    MSet(Vec<(Vec<u8>, Vec<u8>)>),
    /// MGET k1 ...
    MGet(Vec<Vec<u8>>),
    /// EXPIRE key seconds
    Expire { key: Vec<u8>, seconds: u64 },
    /// TTL key
    Ttl(Vec<u8>),
    /// PERSIST key
    Persist(Vec<u8>),
    /// FLUSHDB
    FlushDb,
    /// DBSIZE
    DbSize,
    /// INFO [section]
    Info(Option<String>),
    /// CLUSTER NODES / CLUSTER INFO
    ClusterNodes,
    /// CLUSTER INFO
    ClusterInfo,
    /// COMMAND
    Command,
    /// CLIENT subcommands (best-effort)
    ClientNoop,
    /// SELECT n  (best-effort: only DB 0 supported)
    Select(u64),
    /// QUIT
    Quit,
    /// Unsupported / unknown
    Unknown(String),
}

fn extract_bulk(f: &RespFrame) -> Option<&[u8]> {
    if let RespFrame::Bulk(Some(b)) = f { Some(b.as_slice()) } else { None }
}

fn upper(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

/// Parse a top-level RESP frame as a client command.
pub fn parse(frame: &RespFrame) -> ClientCommand {
    let parts: Vec<&[u8]> = match frame {
        RespFrame::Array(items) => items.iter().filter_map(extract_bulk).collect(),
        _ => return ClientCommand::Unknown("expected array".into()),
    };
    if parts.is_empty() { return ClientCommand::Unknown("empty".into()); }
    let cmd = upper(parts[0]);
    let args = &parts[1..];
    match cmd.as_str() {
        "PING" => ClientCommand::Ping(args.first().map(|b| b.to_vec())),
        "ECHO" if args.len() == 1 => ClientCommand::Echo(args[0].to_vec()),
        "GET" if args.len() == 1 => ClientCommand::Get(args[0].to_vec()),
        "SET" if args.len() >= 2 => {
            let mut ex: Option<u64> = None;
            let mut i = 2;
            while i < args.len() {
                match upper(args[i]).as_str() {
                    "EX" if i + 1 < args.len() => {
                        if let Ok(n) = std::str::from_utf8(args[i+1]).unwrap_or("").parse::<u64>() {
                            ex = Some(n);
                        }
                        i += 2;
                    }
                    "PX" if i + 1 < args.len() => {
                        if let Ok(n) = std::str::from_utf8(args[i+1]).unwrap_or("").parse::<u64>() {
                            ex = Some(n / 1000);
                        }
                        i += 2;
                    }
                    _ => i += 1,
                }
            }
            ClientCommand::Set { key: args[0].to_vec(), value: args[1].to_vec(), ex_seconds: ex }
        }
        "DEL" if !args.is_empty() => ClientCommand::Del(args.iter().map(|b| b.to_vec()).collect()),
        "EXISTS" if !args.is_empty() => ClientCommand::Exists(args.iter().map(|b| b.to_vec()).collect()),
        "INCR" if args.len() == 1 => ClientCommand::Incr(args[0].to_vec()),
        "DECR" if args.len() == 1 => ClientCommand::Decr(args[0].to_vec()),
        "MSET" if !args.is_empty() && args.len() % 2 == 0 => {
            let mut pairs = Vec::with_capacity(args.len() / 2);
            for chunk in args.chunks(2) {
                pairs.push((chunk[0].to_vec(), chunk[1].to_vec()));
            }
            ClientCommand::MSet(pairs)
        }
        "MGET" if !args.is_empty() => ClientCommand::MGet(args.iter().map(|b| b.to_vec()).collect()),
        "EXPIRE" if args.len() == 2 => {
            let secs = std::str::from_utf8(args[1]).unwrap_or("").parse::<u64>().unwrap_or(0);
            ClientCommand::Expire { key: args[0].to_vec(), seconds: secs }
        }
        "TTL" if args.len() == 1 => ClientCommand::Ttl(args[0].to_vec()),
        "PERSIST" if args.len() == 1 => ClientCommand::Persist(args[0].to_vec()),
        "FLUSHDB" => ClientCommand::FlushDb,
        "DBSIZE" => ClientCommand::DbSize,
        "INFO" => ClientCommand::Info(args.first().map(|b| String::from_utf8_lossy(b).to_string())),
        "CLUSTER" if !args.is_empty() => match upper(args[0]).as_str() {
            "NODES" => ClientCommand::ClusterNodes,
            "INFO" => ClientCommand::ClusterInfo,
            _ => ClientCommand::Unknown(format!("CLUSTER {}", upper(args[0]))),
        },
        "COMMAND" => ClientCommand::Command,
        "CLIENT" => ClientCommand::ClientNoop,
        "SELECT" if args.len() == 1 => {
            let n = std::str::from_utf8(args[0]).unwrap_or("0").parse::<u64>().unwrap_or(0);
            ClientCommand::Select(n)
        }
        "QUIT" => ClientCommand::Quit,
        _ => ClientCommand::Unknown(cmd),
    }
}
