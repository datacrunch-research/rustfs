# Internode RDMA data transport

**Use this when:** you want node-to-node shard reads and writes to move over RDMA (RoCE or InfiniBand) instead of the internode HTTP stream routes, or you are diagnosing why a cluster fell back to `tcp-http`.

The `rdma` internode data transport moves the bytes of `read_file_stream` and `put_file_stream` between RustFS nodes through reliable-connected queue pairs over `libibverbs` and `librdmacm`. Everything else stays where it is: the gRPC control plane, directory walks, namespace scanner streams, capability probes, and the public S3 listener. Erasure coding, bitrot verification, quorum, disk-health tracking, and the authenticated `put_file` commit (nonce, server epoch, staging file, body digest trailer) are unchanged; only the transport under `InternodeDataTransport` differs.

The transport is Linux-only and compiled behind the `rdma` Cargo feature, which links the shim in `crates/ecstore/csrc/rdma_shim.c` against the system `libibverbs` and `librdmacm`. Default builds contain no RDMA code.

```bash
cargo build --release --bin rustfs --features rdma
```

## How a transfer works

Each node runs an RDMA listener next to its HTTP server. A coordinator that needs a shard opens (or reuses) a connection to the shard node, reserves a window of registered buffer slots, and sends an authenticated request message naming the disk, volume, path, offset, and length together with the window. The shard node reads the file straight into its own registered staging slots, RDMA-writes each chunk into the coordinator's window, and sends a small data notice per chunk. The coordinator hands the landed slot to the erasure decoder as an owned chunk without copying it; the slot's credit returns to the shard node only when the decoder has dropped that chunk, so a slot is never rewritten while any reference to its bytes exists. Writes run the same engine in the other direction: the shard node advertises the window, the coordinator copies the writer's bytes once into staging slots, and the shard node commits the landed slots through the existing authenticated put_file path.

Request messages are signed with the shared RPC secret over a canonical target under `/rustfs/rpc/rdma/`, so a signature captured on HTTP is not valid on RDMA and vice versa. Writes carry a nonce that goes through the same replay cache as the HTTP route. Payload bytes are not encrypted on the wire; RDMA traffic is plaintext unless the fabric provides IPsec or MACsec.

## Settings

Set every variable on every node and restart. Values are read once at startup.

| Setting | Default | Effect |
|---|---|---|
| `RUSTFS_INTERNODE_DATA_TRANSPORT` | `tcp-http` | `rdma` selects the RDMA backend for outbound shard streams. Any other unknown value still fails closed; `rdma` on a build without the feature fails closed with a message naming the feature. |
| `RUSTFS_RDMA_SERVER_ENABLE` | `true` when the transport is `rdma` | Starts the RDMA listener. Set it to `true` on nodes that still use `tcp-http` outbound to enable the server first during a rolling upgrade. |
| `RUSTFS_RDMA_PORT_OFFSET` | `1000` | Added to a node's internode HTTP port to derive its RDMA listener port. Must be identical on all nodes. |
| `RUSTFS_RDMA_LISTEN_ADDRS` | hosts of the local endpoints | Comma-separated IP addresses to bind instead of resolving the local endpoint hosts. Use it when the RDMA NIC address differs from the endpoint host. |
| `RUSTFS_RDMA_CHUNK_SIZE` | `1048576` | Bytes per registered slot and per RDMA write (4 KiB to 16 MiB). |
| `RUSTFS_RDMA_STREAM_SLOTS` | `4` | Slots per stream window on each side (1 to 64). The window bounds the bytes in flight per stream. |
| `RUSTFS_RDMA_POOL_BYTES` | `268435456` | Pinned, registered memory per process shared by all streams. Must hold at least two windows. It bounds concurrent RDMA streams; a stream that cannot reserve a window falls back to TCP. |
| `RUSTFS_RDMA_FALLBACK_TCP` | `true` | Reissue a request over `tcp-http` when RDMA setup fails before any payload byte moved. With `false` such requests fail. |
| `RUSTFS_RDMA_CONNECT_TIMEOUT_MS` | `2000` | Address and route resolution plus connection establishment timeout. |
| `RUSTFS_RDMA_STALL_TIMEOUT_SECS` | `30` | Longest pause without progress tolerated on one stream before it fails. |

## Fallback and failure behavior

Fallback is explicit and observable. Only setup failures fall back: the peer's RDMA port refuses or times out, the address does not resolve, the local pool has no free window, or the peer answers that it does not support the protocol. After a setup failure the coordinator keeps that peer on `tcp-http` for ten seconds, logs one `rdma_transport_fallback` warning per backoff window, and counts the event under `rustfs_system_network_internode_operation_classified_errors_total{backend="rdma",classification="tcp_fallback"}`. A peer running a release without the RDMA listener therefore works unchanged; the warning repeats every ten seconds for as long as the mismatch lasts.

Failures after the first payload byte do not switch transports. They map to the same disk error classes as HTTP stream failures: a lost connection is a retryable connection reset and the remote-disk reader resumes from the delivered offset over a fresh connection; a stalled stream is a body stall; a missing file, missing volume, or corrupt file is the typed disk error. Quorum, disk-health, and heal decisions see what they would see with `tcp-http`.

Write streams require the authenticated `put_file` capability. Toward a peer that only offers the legacy route the write uses `tcp-http`.

## Metrics

Existing internode operation metrics carry `backend="rdma"` for RDMA transfers: request counts, sent and received bytes, errors, durations, stall timeouts, and authentication failures. The classification label on `rustfs_system_network_internode_operation_classified_errors_total` records why an RDMA attempt failed (`connect_timeout`, `connection_refused`, `pool_exhausted`, `remote_error`, `stall_timeout`, `resolve`, `unsupported`) and `tcp_fallback` when a request was reissued over TCP.

## Requirements and limits

- Every node needs an RDMA-capable NIC on the internode network with the kernel RDMA stack loaded, `libibverbs` and `librdmacm` installed, and a locked-memory limit large enough for `RUSTFS_RDMA_POOL_BYTES` plus per-connection control buffers.
- The listener binds the IP addresses the local endpoint hosts resolve to. Those addresses must belong to the RDMA device (for RoCE, the IP of the RoCE netdev). Use `RUSTFS_RDMA_LISTEN_ADDRS` when they do not.
- All nodes must share the same port offset, chunk size, and slot count; a peer whose slot size exceeds the local chunk size rejects the transfer.
- One RDMA device per listener address is used; multiple NICs are addressed by listing one IP per NIC, and the coordinator reaches each peer through the address its endpoint host resolves to. Registered memory is not NUMA-pinned.
- Received chunks are views of registered slots. A consumer that holds chunks (for example a decoder waiting on other shards) holds those slots and withholds their credits, so the window and the pool, not an extra copy, bound memory; size `RUSTFS_RDMA_STREAM_SLOTS` and `RUSTFS_RDMA_POOL_BYTES` for the concurrent streams and the longest chunk retention you expect. Only the sending side of a write copies the caller's bytes once into staging memory.
- The listener runs on the same process as the HTTP server and is not a readiness boundary; requests are rejected with a typed error until local disks are initialized.

## Validation on one host

A node with one RoCE port can exercise the whole path against itself. `rping` must succeed on the RoCE address first:

```bash
rping -s -a <roce-ip> -p 7174 -C 3 &
rping -c -a <roce-ip> -p 7174 -C 3
```

The engine tests under `crates/ecstore/src/cluster/rpc/rdma/` and the disk-handler tests in `rustfs/src/storage/rpc/rdma_service.rs` run only when `RUSTFS_RDMA_TEST_ADDR` names a local RDMA-capable IP address:

```bash
RUSTFS_RDMA_TEST_ADDR=<roce-ip> cargo nextest run -p rustfs-ecstore --features rdma -E 'test(rdma)'
RUSTFS_RDMA_TEST_ADDR=<roce-ip> cargo nextest run -p rustfs --features rdma -E 'test(rdma)'
```

For an end-to-end check, start four `rustfs` processes on the RoCE address with distinct ports and `RUSTFS_INTERNODE_DATA_TRANSPORT=rdma`, upload and download objects larger than one erasure block, compare hashes, and confirm that `rustfs_system_network_internode_operation_recv_bytes_total{backend="rdma"}` grows while the `tcp_fallback` classification stays at zero.
