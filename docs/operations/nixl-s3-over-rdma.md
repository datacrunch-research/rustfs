# S3-over-RDMA on NIXL/UCX

RustFS can serve S3 GETs and PUTs over RDMA with NIXL (NVIDIA Inference Xfer
Library) and its UCX backend instead of the cuObject library described in
[cuobject-s3-over-rdma.md](cuobject-s3-over-rdma.md). The request stays an
ordinary signed S3 call; the client adds its NIXL agent name, agent metadata
and registered buffer in headers, the server moves the bytes with NIXL WRITE
(GET) or READ (PUT) transfers, and the reply carries the same
`x-amz-rdma-reply` and `x-amz-rdma-bytes-transferred` headers as the cuObject
transport. UCX addresses every device by the GID, LID and queue-pair numbers
in the agent metadata, so the GPU-side InfiniBand rails work without IPoIB or
a subnet administrator and the CPU-side RoCE ports work without rdma_cm.

The server-side pipeline (segment fan-out, staging chunks, shard arena,
per-write stages) is the one the cuObject transport uses; only the transfer
layer differs. The two transports are compile-time alternatives: a build with
the `nixl` feature uses NIXL for every RDMA request, even when `cuobject` is
also enabled.

## Build

- Cargo feature `nixl` on the `rustfs` crate (`--features rdma,nixl`); the
  `rustfs-nixl` crate depends on the `nixl-sys` binding from the NIXL
  repository at tag `v1.5.2`.
- `NIXL_PREFIX` points at a prefix with `include/` (the NIXL C++ headers of
  that tag) and `lib/` holding `libnixl.so`, `libnixl_build.so`,
  `libnixl_common.so`, `libserdes.so`, `libstream.so` and
  `lib/plugins/libplugin_UCX.so`. The `nixl-cu13` wheel ships the libraries
  (under `site-packages/.nixl_cu13.mesonpy.libs/`); the headers come from the
  repository. `bindgen` needs `libclang` at build time.
- At run time the server needs `LD_LIBRARY_PATH` on that `lib/` and
  `NIXL_PLUGIN_DIR` on `lib/plugins`.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `RUSTFS_NIXL_ENABLE` | `false` | Serve requests that carry the NIXL headers over NIXL. |
| `RUSTFS_NIXL_AGENT_NAME` | `rustfs-<first local endpoint host>` | This node's agent name; must be unique in the cluster. |
| `RUSTFS_NIXL_UCX_DEVICES` | all devices | Comma-separated UCX device names (`mlx5_9,mlx5_10`; NIXL adds the port); pin the agent to the rails that reach the clients. With `UCX_MAX_RMA_RAILS=2` in the server's environment, UCX stripes every write across two of these ports and the client must have registered its buffer on both of them, or the transfer fails and the GET is declined. |
| `RUSTFS_NIXL_NUM_WORKERS` | `1` | UCX progress workers of the agent. |
| `RUSTFS_NIXL_CHUNK_BYTES` | `16 MiB` | Staging chunk, one transfer descriptor at most. |
| `RUSTFS_NIXL_POOL_BYTES` | `1 GiB` | Registered staging memory. |
| `RUSTFS_NIXL_MAX_INFLIGHT` | `256` | Transfers in flight per node. |
| `RUSTFS_NIXL_TRANSFER_TIMEOUT_SECS` | `60` | A transfer that has not completed by then fails the request instead of hanging it. |

The pipeline settings `RUSTFS_CUOBJECT_GET_FANOUT`,
`RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES`, `RUSTFS_CUOBJECT_GET_FANOUT_BUDGET`,
`RUSTFS_CUOBJECT_INFLIGHT_WRITES` and `RUSTFS_CUOBJECT_SHARD_ARENA_BYTES`
apply unchanged.

## Wire contract

Request headers (GET, PUT, UploadPart):

| Header | Value |
|---|---|
| `x-rustfs-nixl-agent` | The client's agent name (printable ASCII, up to 128 bytes). |
| `x-rustfs-nixl-metadata` | The client's agent metadata (`get_local_md`), base64. May be omitted once this server has loaded that agent; a server that does not know the agent declines the request. A restarted client sends new metadata, which replaces the old. |
| `x-rustfs-nixl-buffer` | `<dram|vram>:<device>:<address>:<length>`: the registered buffer the object (or the requested range) lands in from offset 0, or is read from for a PUT. Address and length are decimal or `0x` hex. |

Reply headers: `x-amz-rdma-reply` (`200`, `206` for a range, `501` when the
transfer could not run), `x-amz-rdma-bytes-transferred`. The HTTP body is
empty; the reply is sent after the last transfer completed, so the client may
read its buffer as soon as the response arrives. The server never needs the
client to load the server's metadata.

## Implementation notes

- `Agent::post_xfer_req` in the `nixl-sys` binding returns `true` while the
  transfer is still in progress and `false` when it completed before
  returning. The server polls `get_xfer_status` only in the first case. The
  opposite reading treats every transfer as complete at post time, refills
  the staging chunk before the NIC has read it, and the client receives the
  next chunk's bytes at the earlier offset; the reply also arrives before the
  data. Repeated rounds of the same object hide that, so a verification run
  uses one round into a zeroed buffer.
- A client that never polls its agent needs NIXL's progress thread (the
  binding's `Agent::new` enables it; `new_configured` only with
  `enable_prog_thread`), because the server's endpoint finishes its UCX
  connection handshake only when the client's worker progresses. Without it
  every transfer to that client stays in progress until the timeout.
- The binding serializes every agent call with a lock of its own; the agent
  is nevertheless created with reader-writer synchronization so correctness
  does not rest on that detail.
- `nixl-reuse` in the `rustfs-bench` repository is a RustFS-free check of the
  chunk-reuse contract: a writer fills a remote buffer from a small pool of
  chunks, refilling each as soon as NIXL reports its transfer complete, and
  the target verifies every word against its offset.

## Validation

`nixl-get` in the `rustfs-bench` repository fetches an object into GPU or
host memory through this contract and, with `--verify`, compares it with a
plain HTTP GET and lists which object block each wrong 1 MiB block holds.
Verify with a single round: later rounds rewrite the same bytes and mask a
transfer that completed early:

```bash
export LD_LIBRARY_PATH=$NIXL_PREFIX/lib:/usr/local/cuda/lib64 NIXL_PLUGIN_DIR=$NIXL_PREFIX/lib/plugins
nixl-get --endpoint http://10.20.9.57:9000 --bucket models --key bench/rand-2g-s4m.bin \
  --mem gpu --gpu 4 --devices mlx5_9 --rounds 1 --verify
```

Results on the staging cluster are in the benchmark record
([rdma-get-optimization-benchmarks.md](rdma-get-optimization-benchmarks.md),
"NIXL/UCX on the GPU-side InfiniBand rails").
