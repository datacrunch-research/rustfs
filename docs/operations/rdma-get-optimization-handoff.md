# RDMA GET optimization: handoff (2026-11-04)

State of the work on branch `rainj-me/rdma-support`, written for continuing on
another cluster (Intel CPUs with InfiniBand/RoCE and GPUs).

## Committed

| Commit | What |
| --- | --- |
| `039443eef` | Phase 1 server: cuObject S3-over-RDMA GET (`--features cuobject`) |
| `beab70707` | Phase 0 baseline, bench tooling `scripts/bench/rdma-get/`, stage-summary file setting |
| `ec92a122b` | Phase 2: `RUSTFS_ERASURE_SHARD_BYTES` (4 MiB shards, 48 MiB block for new objects) |
| `914bec288` | Phase 3: pipelined cuObject RDMA writes (`RUSTFS_CUOBJECT_INFLIGHT_WRITES`) |
| `fb18e317f` | 3b: two shard-buffer generations per slot (prefetch regression) |
| `f570dfcd4` | 3c: bitrot hashing off the decode task (`RUSTFS_BITROT_VERIFY_OFFLOAD_MIN_BYTES`) |
| `1e48898c4`, `53c178def` | Phase 4: cache latency record, fill-on-PUT (`RUSTFS_OBJECT_DATA_CACHE_FILL_ON_PUT`) |
| `126da9908` | Phase 4c: node-freeze drill, timeout characterization |
| `6e9233d4e` | abort-path fix, raisable duplex pipe, `[profile.fast]` |
| `24cc25eee` | cuObject PUT and UploadPart over RDMA |
| `8d0b192cb` | GET sink: decoded bytes straight into staging (zero-copy emit, step 1) |
| (this tree) | cold-fill sink fix, cuObject GET fan-out with fair-share budget |

SGLang side (repo /mnt/shared/rain/sglang): `4465e68cb8` NIXL object loader
with `inflight` ranged GETs. Needs NIXL >= 1.6.0 (source build; the 1.5.2
wheel lacks the generic `x-amz-rdma-token` client).

Results are in `docs/operations/rdma-get-optimization-benchmarks.md`
(single RDMA GET 2.24 -> 5.48 GB/s; loader 7.8 GB/s; 16 streams 9-11 GB/s;
48 MiB KV chunk 12 ms first GET after PUT; RDMA PUT 0.39 GB/s single,
1.35 GB/s x16, limited by the PUT pipeline).

## Done on the staging cluster (2026-11-04)

The work moved to the Kubernetes cluster `llm-inference-staging-euc1`
(4 x B300 nodes, Intel Xeon 6767P, inbox kernel RDMA stack); see the
"Staging cluster port" and "Phase 5 (revisited)" sections of the benchmark
record for the environment, the fabric findings, and the numbers.

- The GET sink (step 1 of zero-copy emit) is committed (`8d0b192cb`). A
  regression found here is fixed in the same tree: with
  `RUSTFS_OBJECT_DATA_CACHE_MODE=fill_materialize_enabled`, a cache miss on a
  cacheable object materialized the body for the cache and the sink swallowed
  those bytes (`less data available than what was requested` on RDMA GETs of
  KV chunks). The cold-fill producer now drops the sink.
- Server-side stripe fan-out for cuObject GETs (Phase 5, revisited): the sink
  offers a segment factory, the legacy duplex read path runs up to
  `RUSTFS_CUOBJECT_GET_FANOUT` (default 8) block-aligned segment producers per
  GET, each with its own staging chunks and RDMA writes; a segment is at least
  `RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES` (192 MiB), and each node shares a
  producer budget `RUSTFS_CUOBJECT_GET_FANOUT_BUDGET` (16) fairly among its
  open GETs (8 / 4 / 2 / 1 segments for 1 / 4 / 8 / 16 streams). Single
  stream 2 GiB into GPU: 4.4 -> 8.8 GB/s on 16 drives; the 16-stream
  aggregate needs the budget (first pass without it collapsed to 3.7 GB/s from
  a reader-setup stampede).
- Non-cuobject builds of `rustfs` compile again (the PUT-over-RDMA commit had
  left `rustfs_cuobject` references ungated).
- Cluster facts that matter for the next steps: the CPU-side RDMA NIC is a
  bond of two separate CX-7 cards (no RoCE LAG, port groups cannot reach each
  other over RoCE, rdma_cm picks `mlx5_6` by default); b300-121's PCIe switch
  above `mlx5_6` and two of its NVMe drives trained at Gen5 x2. RDMA now runs
  on `mlx5_11` through `devlink ... enable_roce false` on every node (not
  persistent across reboots) and the cluster uses two drives per node (6+2).
  cuFile needs `CUFILE_DMABUF_ENABLE=true` (no peermem on the inbox stack).

- Zero-copy emit step 2 is in this tree: `ShardBuf` (heap or registered
  arena block) replaces `Vec<u8>` shard buffers in the erasure read path, the
  erasure coder, integrity proof, decode engine and bitrot reader are generic
  over the container, and a `ShardEmitter` offered through the sink writer
  (task-local for the decode loop) lets the cuObject handler RDMA-write data
  shards in place from an arena registered with the first cuObject server
  (`RUSTFS_CUOBJECT_SHARD_ARENA_BYTES`, 0 = off). Single stream 10.3 -> 13.9
  GB/s, 16 x 8 GiB host 21.6 -> 24.6 GB/s; byte-exact on all checks.
- Client GPU placement: the storage NIC is on NUMA 1, so the receiving GPU
  must be one under that socket (GPU 4-7 here; 5-7 GB/s into GPU 0 vs 10.8
  without and 13.9 with zero-copy into GPU 4). RDMA into any GPU through the
  CPU root complex caps at 13-14 GB/s per client; full rate needs the GPU's
  own IB NIC.
- `RUSTFS_RDMA_IDLE_CONNS_PER_PEER` (default 16, was a constant): every
  shard read is a stream on its own connection and under 16 producers per
  node the surplus dialed fresh connections per read (`rdma_fresh_connect`
  debug events count them). Not yet measured with a deeper pool.

- b300-121's PCIe fault was confirmed as a hardware error; the storage
  cluster is now b300-139 and b300-141 with four drives each (6+2), client on
  b300-110. The idle-connection pool depth (64, 256) changed nothing and the
  dial counter stayed low, so connection dials are not the stripe-read cost.
- Multi-GPU client: four processes into one GPU or into host memory reach
  13-17 GB/s, four processes into four GPUs 5-6 GB/s; concurrent peer-to-peer
  writes into several GPUs through the CPU root complex are the limit, so a
  multi-GPU loader at full rate needs the GPUs' own InfiniBand NICs.

## Next steps

1. Reader setup and stripe read under load (10 -> 80 ms per part, 4 -> 20
   ms per stripe with 16 producers per node): profile the shard-stream open
   path and the remote read RPC; connection dials are excluded. Also try
   `RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT` above 2 now that stripes are 24
   MiB and buffers come from the arena (the Gen5 drives sit at a quarter of
   their rate, a queue-depth symptom).
2. Zero-copy write submissions are batched per stripe (scatter-gather, up to
   10 ranges per RDMA operation) and every library transfer waits on a
   channel permit (`RUSTFS_CUOBJECT_NUM_DCIS`, 128): the first zero-copy
   build issued one blocking call per 4 MiB shard with thirteen in flight per
   segment, exhausted the channels under load (`EBUSY`, failed segments) and
   raised CPU per GiB (3.8 vs 2.4 s/GiB). Still open: the library's
   asynchronous mode (submit plus poll) to avoid a blocking thread per
   transfer, and the multi-GPU client numbers with the batched build.
3. Internode zero-copy: receive remote shards straight into the arena block
   (register the arena with the transport's protection domain, carry the
   bitrot hash separately) and read local shards from disk into registered
   memory; a CPU-per-GiB refinement once the producer pipeline is fast.
4. The remaining plan items are unchanged: affinity routing (4), the KV-cache
   tier (4b), the peer circuit breaker and lock quorum (4c), the registered
   cache arena (6) for the warm KV hit.

## Porting to an Intel cluster

- Build: no aarch64 linker issue on x86; the `release` profile (thin LTO,
  one codegen unit) still takes 20+ minutes per edit of `ecstore` or
  `rustfs`; use `--profile fast` for iteration (`lto = false`,
  `codegen-units = 16`, `incremental = true`).
- Server deps: libibverbs/librdmacm dev, and NVIDIA's cuObject server
  library (`libcuobjserver`, `libcuobjserver-dev` from the CUDA apt repo;
  `CUOBJSERVER_ROOT=<prefix>` for the build, `LD_LIBRARY_PATH` at run time).
- Client deps: CUDA toolkit with `libcuobjclient`, a `cufile.json` whose
  `rdma_dev_addr_list` names the client's RDMA IP (`CUFILE_ENV_PATH_JSON`),
  boto3 (clients must set `response_checksum_validation=when_required` for
  bodiless RDMA replies), and for the SGLang loader a NIXL >= 1.6.0 build
  with aws-sdk-cpp and the cuObject client.
- InfiniBand vs RoCE: the internode transport uses rdma_cm (works on both);
  cuObject uses DC transport, also both. A cuObject token/registration must
  stay under 4 GiB; use ranged GETs for larger buffers.
- Cluster scripts: `scripts/bench/rdma-get/node4.sh` / `stop4.sh` take the
  tray IPs from `HOSTS` and the local RDMA IP from the `bond0.225` interface;
  adjust both for the new hosts. Recommended settings for the RDMA GET path:
  `RUSTFS_ERASURE_SHARD_BYTES=4194304 RUSTFS_RDMA_CHUNK_SIZE=4194304
  RUSTFS_RDMA_POOL_BYTES=8589934592 RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=67108864
  RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT=2`, plus the cache and fill-on-PUT
  settings and the moderate detection timeouts listed in the benchmark
  record (Phase 4c).
- Re-upload benchmark objects on the new cluster
  (`scripts/bench/rdma-get/kvlat.py` with `UPLOAD=1`, and the 2 GiB / 8 GiB
  random objects with 96 MiB parts) so they use the 4 MiB-shard layout.
