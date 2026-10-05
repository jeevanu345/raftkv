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
        /// TTL milliseconds, if any.
        ttl_ms: Option<u64>,
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
    /// Millisecond TTL.
    PTtl(Vec<u8>),
    /// Incremental bounded key enumeration.
    Scan {
        cursor: String,
        pattern: String,
        count: usize,
    },
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
    if let RespFrame::Bulk(Some(b)) = f {
        Some(b.as_slice())
    } else {
        None
    }
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
    if parts.is_empty() {
        return ClientCommand::Unknown("empty".into());
    }
    let cmd = upper(parts[0]);
    let args = &parts[1..];
    match cmd.as_str() {
        "PING" => ClientCommand::Ping(args.first().map(|b| b.to_vec())),
        "ECHO" if args.len() == 1 => ClientCommand::Echo(args[0].to_vec()),
        "GET" if args.len() == 1 => ClientCommand::Get(args[0].to_vec()),
        "SET" if args.len() >= 2 => {
            let ttl_ms = if args.len() == 2 {
                None
            } else if args.len() == 4 {
                let multiplier = match upper(args[2]).as_str() {
                    "EX" => 1000,
                    "PX" => 1,
                    _ => return ClientCommand::Unknown("SET syntax error".into()),
                };
                match std::str::from_utf8(args[3])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .and_then(|n| n.checked_mul(multiplier))
                    .filter(|n| *n > 0)
                {
                    Some(n) => Some(n),
                    None => return ClientCommand::Unknown("invalid expiry".into()),
                }
            } else {
                return ClientCommand::Unknown("SET syntax error".into());
            };
            ClientCommand::Set {
                key: args[0].to_vec(),
                value: args[1].to_vec(),
                ttl_ms,
            }
        }

        "DEL" if !args.is_empty() => ClientCommand::Del(args.iter().map(|b| b.to_vec()).collect()),
        "EXISTS" if !args.is_empty() => {
            ClientCommand::Exists(args.iter().map(|b| b.to_vec()).collect())
        }
        "INCR" if args.len() == 1 => ClientCommand::Incr(args[0].to_vec()),
        "DECR" if args.len() == 1 => ClientCommand::Decr(args[0].to_vec()),
        "MSET" if !args.is_empty() && args.len() % 2 == 0 => {
            let mut pairs = Vec::with_capacity(args.len() / 2);
            for chunk in args.chunks(2) {
                pairs.push((chunk[0].to_vec(), chunk[1].to_vec()));
            }
            ClientCommand::MSet(pairs)
        }
        "MGET" if !args.is_empty() => {
            ClientCommand::MGet(args.iter().map(|b| b.to_vec()).collect())
        }
        "EXPIRE" if args.len() == 2 => {
            let secs = match std::str::from_utf8(args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => n.max(0) as u64,
                None => return ClientCommand::Unknown("EXPIRE invalid integer".into()),
            };
            ClientCommand::Expire {
                key: args[0].to_vec(),
                seconds: secs,
            }
        }
        "TTL" if args.len() == 1 => ClientCommand::Ttl(args[0].to_vec()),
        "PTTL" if args.len() == 1 => ClientCommand::PTtl(args[0].to_vec()),
        "SCAN" if !args.is_empty() => {
            let cursor = String::from_utf8_lossy(args[0]).into_owned();
            let mut pattern = "*".to_owned();
            let mut count = 10;
            if (args.len() - 1) % 2 != 0 {
                return ClientCommand::Unknown("SCAN syntax error".into());
            }
            for option in args[1..].chunks(2) {
                match upper(option[0]).as_str() {
                    "MATCH" => pattern = String::from_utf8_lossy(option[1]).into_owned(),
                    "COUNT" => match std::str::from_utf8(option[1])
                        .ok()
                        .and_then(|n| n.parse::<usize>().ok())
                        .filter(|n| *n > 0)
                    {
                        Some(n) => count = n,
                        None => return ClientCommand::Unknown("SCAN invalid count".into()),
                    },
                    _ => return ClientCommand::Unknown("SCAN syntax error".into()),
                }
            }
            ClientCommand::Scan {
                cursor,
                pattern,
                count,
            }
        }
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
            let n = std::str::from_utf8(args[0])
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);
            ClientCommand::Select(n)
        }
        "QUIT" => ClientCommand::Quit,
        _ => ClientCommand::Unknown(cmd),
    }
}
