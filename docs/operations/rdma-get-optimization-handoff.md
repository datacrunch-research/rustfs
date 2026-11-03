# RDMA GET optimization: handoff (2026-11-03)

State of the work on branch `rainj-me/rdma-support`, written for continuing on
another cluster (Intel CPUs with InfiniBand/RoCE and GPUs).

## Committed (HEAD `24cc25eee`)

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

SGLang side (repo /mnt/shared/rain/sglang): `4465e68cb8` NIXL object loader
with `inflight` ranged GETs. Needs NIXL >= 1.6.0 (source build; the 1.5.2
wheel lacks the generic `x-amz-rdma-token` client).

Results are in `docs/operations/rdma-get-optimization-benchmarks.md`
(single RDMA GET 2.24 -> 5.48 GB/s; loader 7.8 GB/s; 16 streams 9-11 GB/s;
48 MiB KV chunk 12 ms first GET after PUT; RDMA PUT 0.39 GB/s single,
1.35 GB/s x16, limited by the PUT pipeline).

## Uncommitted work in progress: decoder-to-staging sink (zero-copy emit, step 1)

Diff saved at `target/bench/sink-wip.diff` (7 files, +341/-21) and left in the
working tree. What it does: a `GetObjectSink` option (`ObjectOptions::get_object_sink`)
lets the legacy duplex GET producer write decoded bytes into a caller sink
instead of the pipe; the cuObject handler builds a `StagingSink` (an
`AsyncWrite` over staging chunks that submits full chunks as RDMA writes) and
finishes it after the producer's terminal. Gated in ecstore to plain objects
(not compressed, not encrypted). Removes one 48 MiB copy per stripe and the
pipe stall.

Status: compiles, clippy and guards pass, and the last build was verified
byte-exact with the direct client (`VERIFY=1 driver.py`): 2 GiB object on
4 MiB shards into GPU memory, the 988 MB model file, a 20 MB range, and a
1 MiB-block object. Single-stream RDMA GET of the 2 GiB object: **6.33 GB/s**
(2.8x the Phase 0 baseline, up from 5.48 GB/s) with the default settings,
before any tuning of prefetch or in-flight writes. The full benchmark chain
(16- and 32-stream rows, variants) was not run. Next steps:

1. Rebuild on the new cluster (`cargo build --profile fast -p rustfs --features rdma,cuobject`),
   restart with `RUSTFS_BIN=target/fast/rustfs`, re-run the verification
   above once.
2. Benchmark with `scripts/bench/rdma-get/bench4.sh` (variants: defaults,
   `RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT=2`, `RUSTFS_CUOBJECT_INFLIGHT_WRITES=6`,
   `RUSTFS_CUOBJECT_CHUNK_BYTES=33554432`) and add the row to the benchmark
   record.
3. Commit as "cuobject: write decoded bytes straight into staging (GET sink)".

Step 2 of zero-copy emit (the plan's item 3 proper): allocate the shard
buffer pool from cuObject-registered memory and RDMA-write from the shard
slots (12 writes of 4 MiB per stripe), refcounting slots until the DMA
completes. This is also the Phase 6 arena.

## Not done from the plan

- 4 affinity routing and the optional Valkey hint.
- 4b KV-cache tier with Valkey (2-3 weeks).
- 4c code: peer circuit breaker (worst stall is 2x the RPC/write-stall budget;
  moderate timeouts give 10.6 s), lock quorum, full-fan-out audit, capped heal.
- 5 skipped by its gate; 6 registered cache arena; container-image track.
- PUT pipeline throughput (0.4 GB/s single, 1.3 GB/s per node aggregate) is
  the KV write-path ceiling, outside the GET plan.

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
