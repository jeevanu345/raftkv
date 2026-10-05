# Security configuration and boundaries

Local defaults intentionally permit unauthenticated development. Remote deployments require deliberate configuration of each trust plane and network restriction.

| Plane | Enforcement |
|---|---|
| Peer | Optional mTLS, cluster CA, unique node certificates, registered SHA-256 fingerprints bound to claimed sender IDs |
| RESP | Optional TLS; shared AUTH token or username/password users; read/write permissions and permitted key prefixes |
| HTTP/admin gRPC | Bearer tokens and viewer/operator/admin roles; optional HTTPS; HTTP session cookie, origin validation and durable audit |
| Metrics | Health and Prometheus routes only; no administrative API on the metrics listener |
| Lab | Separate localhost process and simulator state; no live cluster storage/runtime access |

Viewer can inspect cluster/events/snapshots and SCAN key metadata. Key values and command execution require admin. Operator can create snapshots and transfer leadership. Admin can mutate keys, membership, flush the database and download a backup. Restore is offline with an explicit new bootstrap identity.

```toml
[[admin_users]]
name = "observer"
token = "REPLACE_WITH_SECRET"
role = "viewer"
[[admin_users]]
name = "operations"
token = "REPLACE_WITH_SECRET"
role = "operator"
[[client_users]]
name = "tenant-reader"
password = "REPLACE_WITH_SECRET"
read = true
write = false
key_prefixes = ["tenant:"]
```

Prefix-scoped clients cannot use global SCAN/DBSIZE/FLUSHDB. Every referenced key in multi-key commands must be allowed. Tokens/passwords are not audit targets. Use a Secret provider/environment for real credentials. This project does not implement encrypted-at-rest files or constant-time password comparison; it is not a security-audited production database.

Dashboard sign-in uses a transient token to obtain an HttpOnly, SameSite=Strict 12-hour cookie. HTTPS cookies are Secure. Cookie-authenticated mutations check supplied Origin against the API host or configured allowed origins; bearer clients do not rely on cookies. Same-origin HTTPS hosting is recommended. The UI stores no persistent token. Authentication/authorization must be consistently configured on all nodes so forwarded caller identities retain the same role.

`audit.jsonl` contains timestamp, identity, action, target, node, term, result and requestId. HTTP mutations/backup and gRPC admin operations record attempt/outcome. An attempted audit write failure rejects the operation; an outcome write failure after mutation is logged and cannot undo a committed operation. Audit logs need external rotation/retention; their current local file is not tamper-proof. Backups contain values and should be protected as sensitive data. SHA-256 detects corruption, not an attacker able to rewrite both content and checksum.

Do not use the plaintext Docker/local example configs as remote security policy. Review NetworkPolicy namespaces, reverse-proxy authorization, certificate SANs/rotation and Secret permissions before deploying. Browser/server token expiry, automated certificate rotation and per-user tenant namespaces are outside the implemented local identity model.

## Dependency maintenance exceptions

The lockfile patches the crossbeam-epoch, HTTP/2, TLS, anyhow and protobuf advisories found by the initial audit. Prometheus uses protobuf 3.7.2. `deny.toml` explicitly accepts four **maintenance-only** advisories: bincode 1 (persisted format compatibility), fxhash and instant (sled transitive dependencies), and rustls-pemfile (tonic's transitive dependency). No vulnerability advisory is ignored. These exceptions need migration planning and continued review; passing the configured gate does not make the project independently security-audited.
