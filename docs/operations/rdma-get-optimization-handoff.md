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

## Next steps

1. Zero-copy emit step 2 (the handoff's original item): RDMA-write straight
   from the shard buffers. The cuObject shim registers arbitrary memory
   (`rfs_cuobj_register(ptr, len)`), so the practical route is a registered
   shard-buffer arena behind `ShardBufferPool` with ownership handed to the
   sink until the DMA completes, rather than copying into staging chunks.
   This removes the last copy per stripe (about 5 ms of one core per 48 MiB,
   now overlapped) and is mainly a CPU-per-GiB win; it does not change the
   stripe-level parallelism the fan-out provides.
2. Reader setup under load: the stage summary shows `reader_setup` at 80 ms
   per part with 16 streams (10 ms idle); profile the shard-stream open path
   (remote opens, lock RPC) before adding more concurrency.
3. The remaining plan items are unchanged: affinity routing (4), the KV-cache
   tier (4b), the peer circuit breaker and lock quorum (4c), the registered
   cache arena (6).

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
