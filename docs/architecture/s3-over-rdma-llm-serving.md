# S3 over RDMA for LLM serving: RustFS data path and SGLang integration

**Use this when:** you change the RDMA or NIXL data path, the large-PUT path, or a per-bucket ETag setting, or you build or run a client that loads model weights or offloads KV cache to RustFS over RDMA, such as the SGLang weight loader and HiCache backend described here.

This document describes the design and the invariants of the S3-over-RDMA work on branch `rainj-me/nixl-transport` and of the two SGLang clients built on it. It does not repeat settings or the wire format. Those live in the operations guides:

- [nixl-s3-over-rdma.md](../operations/nixl-s3-over-rdma.md): NIXL build, settings, wire contract, device check, internode NIXL, large-PUT tuning, per-bucket ETag API.
- [cuobject-s3-over-rdma.md](../operations/cuobject-s3-over-rdma.md): the earlier cuObject transport.
- [rdma-get-optimization-benchmarks.md](../operations/rdma-get-optimization-benchmarks.md): the measurement record behind every server-side number below.

## Goals

- Move object bytes between RustFS and GPU or host memory with one-sided RDMA, while the request stays an ordinary signed S3 call. Authentication, authorization, range resolution, quota and erasure coding stay unchanged.
- Serve the two LLM-serving workloads at line rate:
  - **Model weight loading:** about 1.5 TB per replica, read many times.
  - **KV-cache offload:** many 10-60 MB objects written once per prefill and read back by any node.
- Run the whole data plane, including internode shard traffic, on GPU-side InfiniBand rails that have no IP addresses.

## Server components

| Layer | Code | Role |
|---|---|---|
| Client last hop, cuObject | `crates/cuobject/src/lib.rs`, `rustfs/src/app/object/cuobject.rs` | First transport: `x-amz-rdma-token`, NVIDIA cuObject server library. Kept as a compile-time alternative. |
| Client last hop, NIXL | `crates/nixl/src/lib.rs` (`RdmaToken::from_headers`, `ensure_remote`), `crates/nixl/src/mem.rs` | NIXL/UCX transport selected by the `nixl` feature. The client names a registered `dram` or `vram` buffer; the server WRITEs (GET) or READs (PUT) it. |
| Shared pipeline | `rustfs/src/app/object/cuobject.rs` | Segment fan-out of large GETs, staging chunks, shard arena, completion handling. The same pipeline serves both transports; only the transfer layer differs. |
| Device health | `crates/nixl/src/devices.rs` | Startup check that leaves out RDMA devices that are down, slow or flapping. |
| Internode, NIXL | `crates/ecstore/src/cluster/rpc/nixl_internode.rs`, `rustfs/src/storage/rpc/nixl_read.rs` | `RUSTFS_INTERNODE_DATA_TRANSPORT=nixl`: signed control request over HTTP, data over NIXL, for shard reads and writes. |
| Transport selection | `crates/ecstore/src/cluster/rpc/internode_data_transport.rs` | `tcp-http` or `nixl`. Internode RDMA runs over NIXL only; the earlier verbs-based `rdma` backend was removed, and a node still configured with `rdma` fails at startup. |
| Erasure read path | `crates/ecstore/src/erasure/codec/shard_buf.rs`, `crates/ecstore/src/erasure/codec/shard_emit.rs`, `crates/ecstore/src/erasure/coding/bitrot.rs` | Two buffer generations per shard slot, zero-copy emit from registered shard buffers, bitrot verification off the decode task. |
| PUT path | `rustfs/src/app/object/put.rs`, `rustfs/src/app/multipart_usecase.rs`, `crates/rio/src/etag_reader.rs` (`with_md5_offload`), `crates/rio/src/hash_reader.rs` (`EtagAlgorithm`) | Zero-copy RDMA PutObject body, read-ahead, zero-copy UploadPart chunks, ETag MD5 on a blocking-pool thread, per-bucket XXH3 ETags. |
| Per-bucket ETag | `crates/ecstore/src/bucket/etag.rs` (`set_bucket_xxh3_etag`, `bucket_uses_xxh3_etag`), `rustfs/src/admin/handlers/etag.rs` | `etag.json` bucket-metadata entry; admin `GET/PUT/DELETE /rustfs/admin/v3/bucket-etag/{bucket}`. |
| Per-bucket durability | `crates/ecstore/src/bucket/durability.rs` | Existing `strict`, `relaxed` or `none` override. A KV-cache bucket is the intended user of a relaxed tier. |

## Design decisions

- **Signed S3 request, RDMA payload.** The client sends a normal signed S3 request whose body is empty. Three headers name its NIXL agent, its agent metadata and one registered buffer. The reply comes back only after the last transfer completed, so the client may use its buffer as soon as the response arrives. Clients sign with `UNSIGNED-PAYLOAD`, because the payload never crosses HTTP.
- **NIXL/UCX replaced cuObject as the default.** UCX addresses devices by GID, LID and queue-pair numbers carried in the agent metadata. That works on the GPU-side InfiniBand rails without IPoIB or a subnet manager, and on RoCE without `rdma_cm`. cuObject stays as a compile-time alternative.
- **GPU or host memory.** A `vram:<gpu>:<addr>:<len>` buffer works when the client's NIC can reach GPU memory (GPUDirect over dma-buf with the open NVIDIA driver; `nvidia_peermem` is not needed). The server does not need to know which kind it is writing into.
- **Ranges are first class.** A ranged GET lands at offset 0 of the named buffer. Clients split large objects into ranged requests spread over servers to raise parallelism.
- **Internode traffic on the same rails.** With `nixl` internode transport, every shard read and write between servers also moves over NIXL. The control request stays on the internode HTTP connection and binds the transfer with a signed metadata digest (and a one-time nonce for writes), so a replayed request cannot redirect a transfer.
- **Large PUTs are CPU-bound, not link-bound.** The fixes went to the CPU path: offloaded MD5, zero-copy bodies and allocator settings. The admission limit stays at 32 large PUTs per server; raising it to 64 or 128 gained nothing.
- **Per-bucket settings instead of process-wide switches.** XXH3 ETags and relaxed durability suit a KV-cache bucket and break assumptions of ordinary S3 clients, so both are bucket properties. A request that carries Content-MD5 always gets an MD5 ETag.

## Invariants

- `Agent::post_xfer_req` in `nixl-sys` returns `true` while a transfer is in progress. Treating `true` as done refills staging chunks under the NIC and corrupts data. Verify any transport change with one round into a zeroed buffer, because repeated rounds mask the bug.
- Every agent runs NIXL's progress thread, including agents that only receive RDMA writes; peers otherwise declare it dead.
- A client exports its agent metadata after registering every buffer it will name. The server reloads metadata when its hash changes, so a request that still carries an older copy can make the server swap back to it. Register all buffers before the first transfer.
- A registered buffer that an abandoned request named stays out of the pool longer than the peer's transfer timeout.
- Direct-I/O shard writers must be shut down at the end of every NIXL write segment (`finish_segment_file` in `rustfs/src/storage/rpc/nixl_read.rs`); otherwise the trailing sub-alignment bytes are lost.
- A multipart upload keeps the ETag algorithm its bucket had when the upload was created.

## SGLang client integrations

Both clients are SGLang plugins and live in SGLang's tree, not in this repository. On the staging pods they are commits `f3f94cbb7a` (weight loader) and `b7d862b065` (KV offload) on top of SGLang `main` at `1050729dc1`, in `python/sglang/private/`:

| File | Role |
|---|---|
| `rustfs_nixl.py` | Python NIXL client: registered buffers, ranged GET and PUT, HEAD, SigV4 with `UNSIGNED-PAYLOAD`, per-thread keep-alive connections. |
| `rustfs_gpu_stream.py` | Weight streaming core, independent of SGLang. |
| `private_model_loader.py` | `--load-format private` model loader. |
| `rustfs_hicache.py` | HiCache L3 storage backend. |

### Model weights straight into GPU memory

Enabled with `--load-format private`. `--model-path` points at a local directory holding only the config, tokenizer and `model.safetensors.index.json`. No checkpoint byte touches disk, tmpfs or host memory.

- **Plan.** Rank 0 reads every shard's safetensors header with small ranged GETs. It splits the checkpoint into units of whole, contiguous tensors (512 MB target) and lays them out in a per-GPU staging ring (16 GB). Every rank receives the same plan.
- **Fetch.** Unit `u` belongs to rank `u % world`. The owner fetches it from RustFS into its own HBM: several ranged NIXL GETs spread over the servers, on the NIC closest to that GPU.
- **Share.** Every rank consumes units in order. An NCCL broadcast from the owner fills the same ring region on every GPU, and the tensors are yielded (cloned by default) to the model's own weight loaders.
- **Reuse.** A ring region is refetched only after a CUDA event shows this rank's reads of the previous occupant finished.

On the staging B300 nodes, only `mlx5_0`, `mlx5_7`, `mlx5_10` and `mlx5_13` reach the RustFS servers, so GPUs 0-1, 2-3, 4-5 and 6-7 use them in pairs (`RUSTFS_LOADER_RAILS`). An earlier design staged shards in `/dev/shm`. It was dropped because tmpfs pages count against the pod's memory limit, and a 1.56 TB checkpoint exceeds a 1 TiB pod.

### HiCache L3 storage backend

Enabled with `--hicache-storage-backend dynamic` and extra config `{"module_path": "sglang.private.rustfs_hicache", "class_name": "RustfsHiCacheStorage", "interface_v1": 1, "hicache_storage_pass_prefix_keys": true, "endpoints": ..., "bucket": ..., "rails": ..., "group_pages": 32}`, with `--hicache-mem-layout page_first`.

- **Object names.** Names mirror SGLang's file backend:
  - The suffix is the model name, the PP and CP shard, and with DCP `_tp{tp}_dcp{rank}_{size}_page{logical page}`.
  - MLA KV is shared across TP ranks and written by the DCP-shard writer.
  - Mamba state is sharded by TP rank, so every rank writes its own.
- **KV groups.** A full group of `group_pages` storage pages at an aligned absolute position is one object, `<hash of the group's last page><suffix>.g<N>`. Page hashes chain from the sequence start, so that one hash identifies the whole prefix.
  - Pages outside a full group in a backup call become single-page objects.
  - With DCP 8, a storage page is 512 sequence tokens (64 per rank), so 32 pages are exactly one 16384-token prefill chunk.
  - A group can only form inside one backup call, i.e. one chunk. Larger groups need a larger `--chunked-prefill-size` or buffering across calls.
- **Mamba snapshots.** A snapshot is one object per page, holding all of its buffers back to back. SGLang only needs the snapshot at the end of the prefix it restores.
- **Group alignment.** SGLang passes `prefix_keys=None` for a prefix that starts at the radix root (`operation.prefix_keys.copy() if operation.prefix_keys else None`), which is every prefix on a node that never saw the prompt. The backend then infers the alignment by probing the possible group ends one window at a time. A group object exists only under the hash of a page that truly ends a group, and a page's offset within it follows from its distance to that end, so an inferred lookup cannot read the wrong bytes. This inference was added after `b7d862b065` and is not yet committed on the staging pods.
- **Write-behind.** A backup call copies each object into its own slot of a registered pinned write-staging area, starts the PUT on a background pool and returns. When every slot is in flight, writers wait.
  - The copy is required, because SGLang may evict the host pages once the call returns.
  - An object still being written counts as existing, and a read of it waits for its PUT.
  - A failed background PUT only turns into a later miss.
- **Zero-copy reads.** Reads go straight into the host pool when the target pages are contiguous, and through a separate read-staging area otherwise.
- **Registration.** All host pools and staging areas are registered when SGLang reports the pool set complete (`on_host_pools_registered`), before the first transfer.

SGLang facts the backend depends on:

- **DCP allow-list.** With DCP, SGLang accepts only the `file` and `mooncake` storage backends. The plugin commit adds `dynamic` to that list in `cache_controller.py`. The real requirements, MLA KV plus optional Mamba state and a page-first layout, are met.
- **Replays need new tokens.** A hybrid Mamba model restores only up to a state snapshot strictly before the last token. Replaying the exact same prompt therefore reuses only the previous chunk's snapshot. Real follow-up turns, and benchmarks, must append new tokens.
- **Offload is paced by prefill.** Offloading runs on SGLang's backup thread, one batch per chunk. In practice the offload rate equals prefill's output rate.
- **Compiled extensions must match the Python source.** SGLang's Rust extensions (`mem_cache` tree core, `rust_extensions`) are build artifacts in the source tree that git does not carry. A pod that receives new Python source with old extensions fails at startup (`TreeCoreInitParamsBinding ... mamba_host_only_checkpoint`).

## Measured on the staging cluster

All numbers are from 2026-11-07. The setup:

- **RustFS:** four servers with 16 NVMe drives in total, EC 12+4, NIXL on `mlx5_0/7/10/13`.
- **Clients:** 8x B300 nodes running Kimi-K3 with `--tp-size 8 --dcp-size 8 --ep-size 8`.
- **Buckets:** `kvcache` with XXH3 ETags and strict durability.

Server-side GET/PUT numbers are in the benchmark record.

Weight loading of Kimi-K3 (1.56 TB):

| Case | Stream rate | SGLang `Load weight` | Notes |
|---|---|---|---|
| One node, RustFS page cache evicted | 1.56 TB in 25-26 s (about 60 GB/s) | 45.7 s | 1561 GB read from the drives |
| One node, warm | 22 s (about 72 GB/s) | 33.5 s | no drive reads |
| Two nodes loading at once | 40-41 s per node (about 38 GB/s each, 76 GB/s total) | 50-51 s | |

On a 16-shard subset (255 GB), the 8-GPU stream checked byte-exact against plain HTTP GETs for 11,579 sampled tensors on each of the two checked ranks.

HiCache with Kimi-K3 (`group_pages=32`, write-behind on). The restore and GPU-hit columns are measured with the 512-token suffix appended:

| Context | Recompute | Restore from RustFS | GPU hit | Restores at once |
|---|---|---|---|---|
| 32K (16-page groups, synchronous PUTs) | 1.03 s | 0.47 s | 0.28 s | not measured |
| 128K | 4.64 s | 0.65 s | 0.31 s | 15 prompts in 4.43 s |
| 512K | 29.3 s | 1.62 s | 0.58 s | 4 prompts in 4.57 s |
| 1M (1,047,552 tokens) | 87.7 s | 2.94 s | 0.95 s | 3 prompts in 6.96 s |

- **Cross-node, 128K.** One node computed (4.73 s per prompt); another node it had never served restored 100% of each prefix from RustFS in 0.73 s. A third node restored eight prefixes at once in 3.16 s.
- **Object sizes per rank.** A KV page is 0.88 MB, a 32-page KV group 28.3 MB and a Mamba snapshot 56.2 MB.
- **Offload.** Offload ran at prefill pace: 0.14 GB/s per rank at 128K and 0.04-0.09 GB/s at 512K-1M. At most 6 background PUTs per rank were ever pending, far below the server's 32 large PUTs per node. There were no PUT or GET errors and no `SlowDown` replies.
- **Restore limit.** With many restores in flight, throughput levels off near 7 GB/s in total. A multi-process probe reached 13.6 GB/s PUT and 29 GB/s GET with 5 MB objects. The limit is the backend: existence checks on every page, and few GETs in flight per prefetch batch.
- **Per-request cost.** A 5 MB PUT takes 22-31 ms and a 5 MB GET about 10 ms (p50, one in flight). Small objects pay mostly per-request cost, which is why KV pages are grouped.

## Test record

Every test, check and benchmark run for this work, in order of the area it covers. Times are UTC. The staging cluster is the one described above unless an entry says otherwise. "One round into a zeroed buffer" and plain-HTTP comparison are the byte-exactness checks.

### RDMA GET path: cuObject, early NIXL and the benchmark record (2026-11-02 to 2026-11-06)

The full record, with builds and per-phase tables, is [rdma-get-optimization-benchmarks.md](../operations/rdma-get-optimization-benchmarks.md) ("Results by phase" and the sections after it). The milestones:

| Series | What was tested | Headline result |
|---|---|---|
| Phases 0-3d, cuObject, 4-tray GB300 cluster | Baseline, 4 MiB shards, pipelined RDMA writes, two buffer generations, off-thread bitrot, deeper pipe | Single RDMA GET into GPU 2.24 → 5.48 GB/s |
| Phase 4, object data cache | 48 MiB KV chunk latency, fill on PUT, cuObject PUT | Warm hit 11.8 ms p50 (was 21.4); RDMA PUT 0.39 GB/s single, 1.35 GB/s at x16 |
| Phase 4c, freeze drill | One node frozen under load | No failures; worst stall 10.6 s |
| Phases 5-7d, staging B300 cluster | Server-side GET fan-out and budget, zero-copy emit, idle-pool pre-warm, read-ahead, async transfers | Single GET up to 13.5-15.0 GB/s; 32 in flight 24.6 GB/s |
| NIXL on the IB rails | NIXL/UCX transport; transfer-completion fix (post result read inverted); page cache and positional serve; zero-copy emit restored; two front ends | Corrected pipeline: one server 12-15 GB/s per GET, 17-20 GB/s with 8-16 streams |
| Four servers on the IB rails | 16 drives, 12+4, NIXL internode reads | GET 77-112 GB/s |
| NIXL shard writes and cold loads | Internode writes over NIXL | PUT CPU-bound near 9 GB/s; cold loads 37-43 GB/s |
| Large PUT waits | THP compaction, runtime worker count | 16-stream PUT 12.5 → 26 GB/s with 128 workers and `MIMALLOC_ALLOW_THP=0` |
| 4k-token KV objects | 57 MB objects; MD5 offload, fill-on-put off, allocator purge; two and four servers | Four servers: 33-34 GB/s PUT, 31-34 GB/s first GET, 46-49 GB/s repeat GET |

### Server side: four servers on the IB rails (2026-11-05)

| Test | Setup | Result |
|---|---|---|
| GET matrix with faulty rails | 4 servers, NIXL internode reads | Host 25.9-47.8 GB/s, GPU 87.0 GB/s with 8 streams. UCX errors on faulty rails led to the progress-thread fix and to forgetting and reloading peers after a failed transfer. |
| Six healthy rails | Same | Host, 1 stream: 27.4-27.9 GB/s; host, 24 streams: 63.9-67.8 GB/s; GPU, 12 streams: 94.1-102.0 GB/s |
| Device health check | All 8 rails as candidates | Kept and excluded exactly the faulty rails: 110 excluded `mlx5_12` (not active) and `mlx5_8` (68 link drops); 139 excluded `mlx5_5` and `mlx5_12` (148 link drops). Warm-up 86.2 GB/s on the 4 agreed rails. |
| Byte-exact PUT and GET per server | 2 GiB, host and GPU, compared with plain HTTP | OK on all four servers; PUT 1.78-1.97 GB/s, GET 19.2-23.4 GB/s |
| Two client nodes | GPU, 8 streams per node | One node: 109.8-114.1 GB/s. Both nodes, 16 GPU streams: 95.1-97.1 GB/s; 32 host streams: 91.2-96.8 GB/s |
| Unit tests, clippy, guardrails | | `rustfs-nixl` 7 (including 4 device-check tests), config 12, ecstore 44 pass. A stale transport test failed at first (43/44) and was fixed. Clippy clean on `rustfs-nixl`, `rio`, `config`, `ecstore` (rdma) and `rustfs` (rdma, nixl); logging guardrails pass; symlink-escape test passes. |

### Server side: NIXL shard writes and cold loads (2026-11-05 to 2026-11-06)

| Test | Result |
|---|---|
| Writer unit tests | `nixl_internode` 3 pass; transport tests 45 pass; guardrails pass. Clippy on the `ecstore --tests` target fails on 3 unused test items in `store/rebalance.rs` that predate this work. |
| 4 GiB PUTs, TCP vs NIXL shard writes | 1 stream: 2.1 vs 2.3 GB/s; 8 streams: 8.5 vs 8.6 GB/s; 16 streams: 9.0 vs 6.5-9.5 GB/s |
| Reads, warm vs cold (shard files evicted from page cache) | 16 host streams: 50.3 vs 36.6 GB/s (137.5 GB from NVMe); 8 GPU streams: 122.5 vs 43.3 GB/s; 1 host stream cold 26.4 GB/s |

### Server side: large-PUT profiling (2026-11-06, four servers, 8 GiB multipart, 96 MiB parts)

| Test | Result |
|---|---|
| Allocator sweep, 16 streams | Baseline 8.5 GB/s with 4,017 compaction stalls on one node; `MIMALLOC_ALLOW_THP=0` 13.6 GB/s with none; plus purge delay 13.1; plus allocator reclaim off 14.4. Finding: transparent-huge-page compaction. |
| System-wide profile | 13.5 GB/s; `compress_md5_blocks` 18.9%, memcpy 7.8% |
| Drive ceiling | `dd` direct writes 5.0-5.1 GB/s per drive; PUT needs about 4.3 GB/s per node, so the drives are not the limit |
| Fill-on-put off; TCP internode writes | No effect at 8 GiB. TCP internode 7.3 GB/s (one CompleteMultipartUpload error), so NIXL shard writes are worth about 1.7×. |
| Stage timings per part | 1 stream: encode 286 ms, `rename_part` 36.5 ms. 16 streams: encode 411 ms, `rename_part` 405 ms, whole part 923 ms; aggregate 11.9-13.2 GB/s |
| Settings that did not help | 16 GiB staging pool 13.4 GB/s, 32 GiB 11.3; `UCX_RC_MAX_RD_ATOMIC=16` 11.3 GB/s with 2 of 16 streams failing; 4 NIXL workers 12.5 |
| NIXL diagnostic builds | Queue wait 58-93 µs, post 12-32 µs, completion 237-1864 µs, about 2 transfers pending. Finding: NIXL is not the limit; MD5 and encoding starve the 16 runtime workers. |
| Runtime workers | 64 workers: 16 streams 24.6 GB/s. 128 workers: 26.1 GB/s. Single stream unchanged at about 2 GB/s. |
| 128 workers, byte-exact | 8/8 PUT checks (host and GPU, all servers) OK; 16 streams 26.3 GB/s, 28 streams 27.0 GB/s |
| Admission limit at 8 GiB | 64: 23.5-26.1 GB/s. 128: 21.2 GB/s, then 503s at 16 parts and 28 streams. |
| Body read-ahead | Read-ahead 1: 22.0-22.5 GB/s; read-ahead 4: 21.7-25.9 GB/s, 27.0 at 28 streams |
| Pre-commit checks | Clippy clean (`io-metrics`, `config`, `ecstore` rdma, `rustfs --tests`); `startup_observability` 3/3; config 36/36; guardrails pass |
| Final PUT check | 8/8 byte-exact; 16 streams 22.1 GB/s, 28 streams 25.6 GB/s. A self-review limited zero-copy to UploadPart, where it cannot leak staging chunks into the object cache. |

### Server side: KV-sized objects and durability (2026-11-06)

| Test | Setup | Result |
|---|---|---|
| 64-token pages, strict durability | Four servers, 884,736-byte objects | 1 in flight: 49 objects/s, p50 20.0 ms; 64 in flight: 604 objects/s (0.53 GB/s). A 503 at 16 in flight and errors with 4 clients. **Contaminated:** node 121 was stuck terminating and 139 had a 6,503-item heal backlog. The durability-`none` round never ran. |
| Durability `none` vs default, 57 MB objects | Two servers (139, 141), 8 drives | Byte-exact in both modes. PUT, 4 clients × 16: 9.24 vs 9.46 GB/s. PUT, 1 in flight: p50 188 vs 184 ms. GET, 4 clients: 37.65 vs 31.31 GB/s. Finding: durability is not the bottleneck at 57 MB. |

### Server side: KV-object PUT optimization, two servers, 57 MB objects (2026-11-06)

| Phase | Tests and checks | Result |
|---|---|---|
| 0, baseline | Byte-exact check | 1 in flight 0.27 GB/s (p50 202 ms); 16 in flight 3.37 GB/s |
| 1, MD5 offload, conditional zero-copy PutObject | `etag_reader` 14/14 (3 new); `rio` 222/222; clippy clean; bench ETag check. Byte-exact and multipart 2 GiB checks pass. | Fill-on-put on: 0.38 / 4.06 / 10.96 GB/s at 1 / 16 in flight / 4 clients. Fill-on-put off: 0.48 / 5.20 / 11.95 GB/s. Profile at 17.41 GB/s: `compress_md5_blocks` 16.83%, spinlock slow path 11.54%. |
| 1, allocator purge delay | 4 clients × 16 in flight | Default 15.46-15.55 GB/s; 1 s 15.82; never 18.49-19.35 (a second pass: 13.42 vs 15.14). Never-purge recommended. |
| 1, admission limit | 4 clients × 32 in flight | Limit 32: 503 `SlowDown`; limit 64: 18.93 GB/s. The default of 32 is kept. |
| 1, unit tests on Linux | `rio` 222, config 36, clippy clean | Two `put.rs` tests failed (`object_progress_tracks_zero_byte_and_zero_copy_put_lock_waits`, `cancelled_put_releases_durable_quota_reservation`). The second passes alone; the first fails on the base commit too, so it predates this work. |
| 2, XXH3 ETags | `rio` 224 (2 new); config 36; clippy; every pass checks bytes and ETags | Single stream 105 → 54-57 ms (0.50-0.52 → 0.94-0.97 GB/s); 16 in flight 6.6 → 8.4-9.2 GB/s; 4 clients 16.9-18.1 → 17.0-22.1 GB/s |
| 3, O_DIRECT shard writes | First try failed verification. Root cause: the NIXL segment writer called `flush()` instead of `shutdown()`, losing the sub-alignment tail. Regression test `finishing_a_segment_writes_the_sub_alignment_tail` added (passes). | After the fix, all checks pass. 4-client PUT: buffered 6.71-7.47 GB/s, direct 16.02-20.64 GB/s. GET 23.7-28.5 GB/s either way. An inference deployment shared two servers, so compare within a pass only. Neither XXH3 nor direct I/O was judged a significant gain; direct I/O is kept for co-location with inference. |
| Infrastructure | `ib_send_bw`, `ib_write_bw`, `ib_read_bw` on b300-121 | About 392 Gb/s per CPU RNIC, about 98 GB/s with both; PCIe Gen5 x16 confirmed |

### Server side: four servers, 57 MB objects (2026-11-06, 128 workers, fill-on-put off, never-purge, direct I/O, XXH3)

| Test | Result |
|---|---|
| Byte-exact and ETag check through each server | OK, 2.84-3.06 GB/s |
| Single client | 1 in flight 0.82 GB/s (65 ms); 16 in flight 8.58 GB/s |
| 4 clients | PUT 28.51 GB/s, GET 27.08 GB/s (verified) |
| 8 clients | PUT 33.41 GB/s (p50 170 ms), GET 30.71 GB/s (verified) |
| Admission limit, 8 clients | 32: PUT 33.4 GB/s. 64: PUT 21.71 GB/s (p50 355 ms), GET 37.52. 128: PUT 25.26 GB/s (p50 744 ms), GET failed with 503. The limit stays at 32. |
| Cold vs warm GET, 8 clients | PUT 34.39 GB/s; cold GET 33.52 GB/s (p50 136 ms); warm 45.76 GB/s at 16 in flight, 48.72 GB/s at 32 |
| Large-object GET recheck | 16 host streams × 8 GiB still 77.8 GB/s. The gap to 46-49 GB/s on 57 MB objects is per-object: no fan-out below 192 MiB, plus fixed per-object cost. |

### Server side: S3 compatibility and per-bucket ETag (2026-11-06)

| Test | Result |
|---|---|
| `sdk_smoke.py` (boto3 1.34.46, plain HTTP, all four servers) | 104/104 checks. Covers PutObject at 3 KB, 10 MiB and 100 MiB with no, correct and wrong Content-MD5; 3-part multipart; ETag equals MD5; GET bytes. Rerun 104/104 after the per-bucket ETag change. |
| Per-bucket ETag unit tests and lint | Clippy clean on `rio`, `rio-v2`, `utils`, `config`, `ecstore` (rdma), `rustfs`. `rio` 225; ecstore 8 (configuration, registry, metadata sync, MinIO blob, old decoder) plus 2 round-trip tests; `rustfs` 46 (admin handler, route registration, multipart pin). |
| `etag_bucket_test.py`, end to end | Three runs to green. First: 12/60 failed, because boto3 before 1.36 sends Content-MD5, which keeps MD5 by design; the test now strips it. Second: 3/60 failed, a propagation race in the admin GET; the test now polls (propagation takes 0.06 s). Third: 60/60 pass, covering XXH3 over boto3 and over NIXL, MD5 in other buckets, Content-MD5 still enforced, and the multipart algorithm pin. |

### Server side: operational issues found while testing

- **503 `SlowDown` storm during the model upload (2026-11-06).** The cause was client concurrency: 64 workers × 8 parts, against the 32-per-server admission gate. Also:
  - The upload pod froze under the namespace's 1 GiB default memory limit.
  - `/data00` was full on two servers, so 2 of 16 shard writes failed and parts crawled.
  - Fixed by a lower client concurrency, a 64 GiB debug pod and cleaned drives. The 76 stale multipart uploads left behind expire after 24 h.
- **503s right after a restart.** Scripts now wait for a successful HEAD before measuring.
- **Earlier recurring issues:**
  - The build script reported success on a failed build and skipped untracked files; it was fixed.
  - 32 clients × 8 GiB exceeded a 256 GiB client pod and were OOM-killed.

### Commits and the checks that verified them

| Commit | Change | Verified by |
|---|---|---|
| `6a542944d` | Read the `post_xfer_req` result correctly | Single-round byte-exact GETs; `nixl-reuse` harness |
| `afb8dc62d`, `d07c6203e` | Completion thread; zero-copy arena emit restored; positional internode reads | Erasure tests (86 and 102); clippy; full GET matrix |
| `858d499d7` | Internode NIXL reads, device check, progress thread | `rustfs-nixl` 7, config 12, ecstore 44, guardrails; device check; per-server byte-exact PUT and GET; two client nodes |
| `80f7b8027` | Internode NIXL shard writes | Writer tests 3, transport tests 45, guardrails; per-server checks; cold and warm loads |
| `4ee02601d`, `a2e97d28b` | PUT stage timings; body read-ahead and zero-copy UploadPart | Clippy; `startup_observability` 3; config 36; 8/8 byte-exact PUTs |
| `8ec82a888` | MD5 offload; conditional zero-copy PutObject body | `rio` 222 (`etag_reader` 14); config 36; clippy; phase 1 and purge runs with ETag checks. Ships with the pre-existing `put.rs` test failure noted above. |
| `7b23ced7a` | XXH3 ETags (process-wide; replaced by `a0fb45125`) | `rio` 224; config 36; clippy; phase 2 runs with ETag checks |
| `82055477d` | Shut down the shard writer at the end of each NIXL write segment | Regression test; clippy; phase 3 runs with verified read-back |
| `a0fb45125` | Per-bucket ETag algorithm | Clippy on 6 crates; `rio` 225; ecstore 8+2; `rustfs` 46; 60-check end-to-end; 104-check SDK smoke test |

The earlier commits on this branch (cuObject transport, read path, pre-warm, read-ahead, listener fix) were verified by the runs in the benchmark record and the unit tests listed with them there.

Gaps in this record: the raw `ib_read_bw` output for the PUT investigation, per-row results of the first purge comparison and the phase 0 four-client aggregate were lost to parser errors or overwritten logs. They are not included.

### GPU-direct weight loading (2026-11-06 to 2026-11-07)

| Test | Setup | Result |
|---|---|---|
| Python NIXL client, first `/dev/shm` design | One 0.8 GB shard and `config.json` into an mmap'd file | Byte-exact, 1.71 GB/s |
| SGLang start, Kimi-K3.1 | `/dev/shm` loader, user's PD-decode flags | Rejected by SGLang before load: Kimi-K3.1 with DCP does not allow PD disaggregation |
| SGLang start, Kimi-K3 | `/dev/shm` loader | Pod OOM-killed at 23:34:47: tmpfs pages count against the 1 TiB pod limit. Design replaced by GPU-direct loading. |
| `probe_vram.py` | First Kimi-K3 shard (2.34 GB), one rail, 8 ranged streams, GPU 0 vs pinned host | GPU 15.33 GB/s, host 20.34 GB/s; both copies identical, first 4 KiB identical to HTTP. GPUDirect works via dma-buf without `nvidia_peermem`. |
| `test_gpu_stream.py`, 8 shards, verify on | 120.4 GB, 8 GPUs, NCCL broadcast | 0 mismatches in 5,405 sampled tensors on each of two ranks; 16.5 GB/s, limited by the sampling copies |
| `test_gpu_stream.py`, full checkpoint, verify off | 1.56 TB; units in flight per rank × streams per unit | 2×4: 27.6 s (56.6 GB/s); 4×4: 25.4 s (61.4 GB/s); 4×8: 24.2 s (64.5 GB/s, kept as default) |
| `test_gpu_stream.py`, 16 shards, verify on | 255.5 GB, new header probe | 0 mismatches in 11,579 sampled tensors on each of two ranks |
| SGLang cold start, b300-110 | RustFS page cache evicted before launch | Stream 25-26 s (59-62 GB/s per rank); `Load weight` 45.7 s; fired up 5 min 20 s after launch (mostly kernel compilation); 1561 GB read from drives |
| SGLang warm start, b300-110 | No eviction | Stream 22-23 s (about 72 GB/s); `Load weight` 33.5 s; fired up 1 min 43 s; 0 GB from drives |
| Two nodes at once, b300-121 and b300-139, first run | Same flags | Streams 32.7 s and 35.5 s (47.7 and 44.0 GB/s); `Load weight` 52.7 s and 56.5 s. Both crashed afterwards: SGLang's compiled tree-core extension was from 0.5.23 (`unexpected keyword argument 'mamba_host_only_checkpoint'`). Fixed by copying the `main`-built extensions from b300-110. |
| Two nodes at once, second run | Same | Streams 41.3 s and 40.3 s (37.8 and 38.7 GB/s); `Load weight` 50.8 s and 49.7 s |

### HiCache backend: standalone checks (`test_hicache_backend.py`)

Fake MLA and Mamba host pools, real RustFS, one process. The check list grew with each fix; the final run has 14 checks:

- writes drain;
- every page written and found;
- a lookup stops at the first missing page;
- scattered reads are byte-identical;
- another node with no `prefix_keys` finds the full prefix, and a prefix that starts mid-group, and reads it byte-identical;
- Mamba writes and reads back, including a read straight after a write-behind write;
- `exists_v2` applies the trailing-page rule.

| Version | Pages | Result |
|---|---|---|
| First version, 16-page groups, synchronous PUTs | 320 KB × 1024 | 9/9 pass; PUT 0.19 GB/s, GET 1.24 GB/s |
| 32-page vs 16-page groups, synchronous | 896 KB × 1024 | All pass; PUT 0.99 vs 1.44 GB/s, GET 2.37 vs 2.81 GB/s |
| Write-behind, 32 vs 16 pages | 896 KB × 1024 | 11/11 pass; drained PUT 2.24 vs 2.81 GB/s, GET 2.24 vs 2.93 GB/s |
| Cross-node alignment inference | 896 KB × 512 and × 1024 | 14/14 pass at both sizes (first attempt: 2 failures, one real gap and one test bug, both fixed) |

### RustFS request cost for KV-sized objects

| Probe | Setup | Result |
|---|---|---|
| `probe_put_latency.py`, `kvcache` (strict durability, MD5 ETags) | Python client, one process, one rail, 5 MB objects | PUT p50 31.3 ms single, 2.90 GB/s at 32 in flight, 3.05 GB/s at 64; GET p50 10.4 ms single, 3.99 GB/s at 32 in flight |
| Same, 256 KB objects | | PUT p50 16.1 ms, GET p50 5.5 ms single; 0.22 / 0.20 GB/s at 32 in flight |
| Same, scratch bucket `hicache-probe` (durability `none`, XXH3) | 5 MB | PUT p50 22.1 ms single, 3.40 GB/s at 32 in flight; GET 10.1 ms, 3.71 GB/s |
| Same, `hicache-probe` | 256 KB | PUT p50 14.3 ms, GET p50 5.2 ms single |
| Rust client `nixl-get put-objects` / `get-objects` | One endpoint, one rail, 5 MB | PUT 0.24 / 3.01 / 3.04 GB/s at 1 / 32 / 64 in flight; GET 0.31 / 4.96 / 5.80 GB/s. Same ceiling as the Python client, so the client is not the limit. |
| 8 concurrent Python probes | One per rank and rail, 32 in flight each, 5 MB | Total PUT 13.6 GB/s, GET 29.2 GB/s |
| ETag check on stored HiCache objects in `kvcache` | One 14,155,776-byte KV group, one 56,171,520-byte Mamba snapshot | ETag equals the content MD5 for both: the bucket had no XXH3 setting. XXH3 was then set on `kvcache`. |

### HiCache in SGLang, one node (b300-110)

Kimi-K3 with the user's `sglang serve` flags, minus `--disable-radix-cache`, plus HiCache (`--hicache-size 48`, `write_through`, prefetch `wait_complete`).

`hicache_bench.py` has four passes:
1. cold prefill;
2. `/flush_cache`, then L3 restore;
3. GPU hits;
4. `/flush_cache`, then all prompts restored at once.

A final baseline pass computes the 512 new tokens alone.

| Run | Result |
|---|---|
| First start | Refused: SGLang allows only `file` or `mooncake` storage with DCP. Fixed by allowing `dynamic` in `cache_controller.py`. |
| 32K × 15, exact replay, 16-page groups | Cold 1.03 s; replay restored only 50% (16,384 tokens). Finding: a Mamba model restores only to a snapshot before the last token, so replays must append new tokens. Second flush returned HTTP 400 while backups were in flight; the bench now retries. |
| 32K × 15, replay + 512 new tokens | Cold 1.03 s; L3 0.47 s (98% cached); GPU hit 0.28 s. Concurrent pass crashed in the bench (name shadowing), fixed afterwards. |
| 128K × 15, 16-page groups, synchronous PUTs, MD5 | Cold 4.64 s; L3 0.65 s; GPU hit 0.31 s; 15 at once 7.77 s; baseline 0.24 s; 0 failed PUTs or GETs |
| 128K × 15, 32-page groups, write-behind, XXH3 | Cold 4.64 s; L3 0.65 s; GPU hit 0.31 s; 15 at once 4.43 s; baseline 0.25 s. At most 6 write-behind PUTs pending: offload already kept pace with prefill (0.14 GB/s per rank). |
| 512K × 4 | Cold 29.3 s; L3 1.62 s; GPU hit 0.58 s; 4 at once 4.57 s; offload 0.09 GB/s per rank, at most 2 pending |
| 1M (1,047,552 tokens) × 3 | Cold 87.7 s; L3 2.94 s; GPU hit 0.95 s; 3 at once 6.96 s; offload 0.04-0.07 GB/s per rank; 0 failed PUTs or GETs; no `SlowDown` replies |

### HiCache across nodes (b300-121, b300-139, b300-110)

| Test | Result |
|---|---|
| Plugin files on all three pods | Identical checksums (and identical patched `cache_controller.py`) |
| Page-hash determinism | The same token list hashes identically on all three pods |
| First cross-node run, 8 × 128K: b300-121 writes, b300-139 and b300-110 read | Writer cold 4.66 s p50 (first request 65 s, likely first-run kernel compilation). Readers 0% cached: b300-139 recomputed in 7.5 s on average, b300-110 restored all eight together in 23.5 s. |
| Controlled prompt, one 32K prompt | Writer stored exactly the expected objects (two `.g32` groups, two Mamba snapshots). The backend's own lookup, given the reader's keys, found all 64 pages. The reader still missed. |
| Reader debug log (`RUSTFS_HICACHE_DEBUG=1`) | Reader asked for the right page hashes but with `p0=None`: SGLang passes no `prefix_keys` for a root prefix. Fixed with alignment inference. |
| Restart of b300-139 for the debug run | First attempt refused (`memory capacity is unbalanced`): the old server had not released its GPUs. The launch script now waits for idle GPUs. |
| Cross-node rerun, 8 × 128K, fixed backend | Writer 4.73 s; b300-139 restored 100% in 0.73 s (p50 0.70 s); GPU hits 0.32 s; b300-110 restored all eight together, 100%, in 3.16 s; reader logs show `kv 256 final 256` |

## Open items

- **TTL and capacity eviction for KV-cache buckets.** Lifecycle expiry works only in whole days, counted from creation. Options are a TTL check in the client using `Last-Modified`, or a per-bucket TTL in RustFS with read-path enforcement and a background reaper. Eviction by total size is a separate feature.
- **Stable per-model namespace in KV object names.** Use an explicit model ID and revision plus the KV dtype instead of the local model path, and a bucket per model.
- **Restore throughput.** Probe Mamba snapshots only at chunk boundaries, and keep more GETs in flight per prefetch batch.
- **Groups larger than one chunk.** 64-page groups need `--chunked-prefill-size 32768` or buffering across backup calls.
- **Burst cap.** Cap write-behind PUT threads (for example 8 per rank) so that a burst cannot exceed the 32-per-server admission limit.
- **Upstreaming.** Move the client code out of the private SGLang tree into a maintained location, and turn the HiCache DCP allow-list change into a capability the backend declares.
