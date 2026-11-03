# RDMA GET optimization: benchmark record

Cluster: 4 GB300 trays (tray06/07/17/18), one RustFS node per tray with 4 local
disks, EC 12+4, RoCE bond `mlx5_bond_0` (2 x 100 Gb/s per node), internode
transport `rdma`, cuObject S3-over-RDMA enabled. Client: tray17, direct cuObject
client into GPU memory unless noted. Object: `models/bench/rand-8g.bin`
(8 GiB random bytes, 1 MiB erasure block) unless noted. Each number is the best
of three rounds. Plan: `tmp/rustfs-optimization-plan.md`.

## Link baseline (ib_write_bw, tray18 -> tray17, 1 MiB messages)

| Queue pairs | Average |
| --- | --- |
| 1 | 98 Gb/s (12.2 GB/s) |
| 8 | 196 Gb/s (24.5 GB/s) |

The link is not the single-stream limit.

## Phase 0: baseline and existing switches

Binary: `039443eef` (1 MiB erasure block, cuObject chunk 64 MiB, one write in
flight). Single-stream numbers use the 2 GiB object into GPU memory; the
parallel columns are 16 ranged GETs of the 2 GiB object into GPU memory and
16 / 32 ranged GETs of the 8 GiB object into pinned host memory, spread over
the four trays. CPU is storage-node CPU seconds per GiB during the
single-stream runs (RDMA and HTTP mixed).

| Variant | Single RDMA GET | Single HTTP GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| baseline (defaults) | 2.24 GB/s | 2.30 GB/s | 7.17 GB/s | 9.14 GB/s | 9.22 GB/s | 1.02 |
| `RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT=2` | 2.28 | 2.45 | 7.36 | 8.80 | 9.17 | 1.00 |
| `RUSTFS_GET_BITROT_DECODE_OVERLAP_ENABLE=true` | 2.84 | 2.53 | 7.24 | 9.24 | 9.39 | 0.82 |
| `RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=67108864` | 2.89 | 2.41 | 7.43 | 9.24 | 9.17 | 0.96 |
| `RUSTFS_GET_CODEC_STREAMING_ROLLOUT=on` | 2.80 | 2.50 | 7.70 | 9.03 | 9.15 | 0.84 |
| prefetch + overlap + duplex | 2.77 | 2.44 | 7.48 | 9.20 | 9.18 | 0.90 |
| all four switches | **3.11** | 2.65 | 7.20 | 8.97 | 8.76 | **0.77** |

Per-stage timings of one single-stream 2 GiB GET (sampled stage summary,
`RUSTFS_OBS_GET_STAGE_LOCAL_SUMMARY_*`; the object has 64 multipart parts of
32 MiB, so 64 decode runs and 2,048 stripes of 1 MiB):

| Stage | Baseline | All four switches |
| --- | --- | --- |
| whole GET (`rdma_transfer`) | 922 ms | 701 ms |
| `stripe_read` (2,048 stripes, serial) | 428 ms, 0.21 ms each | 428 ms, 0.21 ms each |
| `emit` (decode + write into the pipe) | 470 ms, 0.23 ms each | 309 ms, 0.15 ms each |
| `duplex_backpressure` (pipe full) | 465 ms | 306 ms |
| `reader_setup` (64 parts, 16 shard streams each) | 366 ms, 5.7 ms each | 388 ms, 6.1 ms each |
| metadata | 10 ms | 8 ms |

Stripe read and emit run back to back, about 0.44 ms per 1 MiB stripe, which
is the 2.2 GB/s ceiling. Two costs are independent of the stripe loop and
will dominate once it is faster: per-part reader setup (opening 16 shard
streams per multipart part, 5.7 ms each) and the pipe backpressure from the
single in-flight RDMA write. Gate 1: stripe read dominates, so the order
stays Phase 1 -> 2 -> 3, with larger multipart parts as part of Phase 2.

A single stream is latency-bound in the stripe loop (one 1 MiB stripe in
flight), not link- or disk-bound: sixteen independent ranged GETs of the same
object reach 9 GB/s from the same client. The four existing switches together
lift a single stream by 39 percent and cut storage CPU per GiB by a quarter;
prefetch alone does nothing because its depth-1 overlap is already bounded by
the 4 MiB duplex pipe.

## Phase 1: client-side parallel ranged GETs

Direct cuObject client, one token per range, all ranges in flight, no server
change (numbers from the baseline binary):

| Object, memory | 1 stream | 4 | 8 | 16 | 32 |
| --- | --- | --- | --- | --- | --- |
| 988 MB model file, GPU, one host | 1.99 GB/s | 3.41 | 3.95 | 4.33 | 4.29 |
| 988 MB model file, GPU, four hosts | | | 3.60 | 4.52 | 4.06 |
| 8 GiB, pinned host, four hosts | 2.1 | | | 9.1-11.7 | 9.2 |

The 988 MB file is too small to amortize connection and token setup; the
8 GiB object shows the real aggregate, within 25 percent of the single
queue-pair link rate.

The SGLang loader (`--load-format rdma`, NIXL OBJ backend) got an `inflight`
option: that many ranged GETs of `staging_bytes` each stay in flight per
file (default 16 x 64 MiB). Synthetic 3 GiB safetensors model (32 tensors of
96 MiB, 96 MiB multipart parts), one GPU, transfer time reported by the
loader itself:

| `inflight` x `staging_bytes` | accelerated (RDMA into VRAM) | HTTP into pinned host |
| --- | --- | --- |
| 1 x 64 MiB | 1.15 GB/s | |
| 2 x 64 MiB (old double buffer) | 2.45 GB/s | |
| 16 x 64 MiB | **7.79 GB/s** | 8.31 GB/s |
| 32 x 32 MiB | 6.03 GB/s | |

Exit criterion met: one model load runs within 15 percent of the 16-stream
direct-client aggregate (7.8 vs 9.1 GB/s). The accelerated path still pays
about 2.4 s once per process to bring up the cuObject fabric, which dominates
small models; the HTTP path then needs a host-to-GPU copy that the RDMA path
does not.

## Phase 2: 4 MiB shards (48 MiB erasure block)

`RUSTFS_ERASURE_SHARD_BYTES=4194304` with `RUSTFS_RDMA_CHUNK_SIZE=4194304`
and `RUSTFS_RDMA_POOL_BYTES=2147483648`; the bench objects were re-uploaded
with 96 MiB multipart parts (`bench/rand-2g-s4m.bin`, `bench/rand-8g-s4m.bin`),
so each part holds two stripes. Same client and object sizes as Phase 0.

| Variant | Single RDMA GET | Single HTTP GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| 4 MiB shards, defaults | 2.57 GB/s | 2.01 GB/s | 6.43 GB/s | **11.21 GB/s** | 7.79 GB/s | 0.94 |
| + `RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT=2` | 1.93 | 1.49 | 6.51 | 9.07 | 8.20 | 1.06 |
| + prefetch + overlap + duplex 64 MiB | 2.38 | 1.63 | 6.37 | 8.36 | 7.93 | 0.98 |

Per-stage timings of one single-stream GET (2 GiB, 48 MiB stripes):

| Stage | Defaults | overlap + duplex 64 MiB |
| --- | --- | --- |
| whole GET (`rdma_transfer`) | 1,043 ms (2.04 GB/s) | 820 ms (2.60 GB/s) |
| `reader_setup` per 96 MiB part | 8.8 ms | 9.9 ms |
| `stripe_read` per 48 MiB stripe | 12.4 ms (3.9 GB/s) | 10.2 ms |
| `emit` per stripe (bitrot, decode, copy into the pipe) | 14.4 ms (3.3 GB/s) | 15.2 ms |

Reading: the expected 2-3x did not appear because the stripe loop was never
dominated by fixed per-stripe overhead; stripe read and emit both scale with
bytes (0.21 + 0.23 ms per MiB in Phase 0, 12.4 + 14.4 ms per 48 MiB here) and
still run back to back. With the big block the three serial costs are now of
the same order: per-part reader setup, the shard reads, and emit. Larger
blocks did help the aggregate (16 x 8 GiB: 11.2 GB/s, up from 9.1) and keep
CPU per GiB flat while cutting bitrot hashes and stripe round trips 48x.
Depth-1 prefetch hurts at this block size (64 MiB stripe buffers allocated
per stripe), which is the next thing to fix in the decode loop. The setting
stays in because it is a prerequisite for aligned 4 MiB shard I/O and for
the pipelining work; a 2x single-stream gain needs the read and emit stages
to overlap, not a bigger block.

## Phase 3: pipelined cuObject RDMA writes

`RUSTFS_CUOBJECT_INFLIGHT_WRITES=3` staging chunks per GET (default), chunk
default lowered to 16 MiB; the write of chunk N runs on its own task while
the decoder fills N+1. Same 4 MiB-shard objects and settings as Phase 2
unless noted.

| Variant | Single RDMA GET | Single HTTP GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| pipelined writes, defaults | **3.17 GB/s** | 2.04 GB/s | 6.80 GB/s | 9.00 GB/s | 8.42 GB/s | **0.84** |
| + duplex 48 MiB | 2.84 | 1.68 | 7.01 | 8.64 | 2.96 | 0.98 |
| + duplex 48 MiB + bitrot overlap | 2.48 | 1.44 | 6.86 | 9.08 | 8.47 | 1.12 |
| 6 x 8 MiB chunks + duplex + overlap | 2.87 | 1.72 | 6.82 | 8.54 | 7.79 | 1.02 |
| 1 MiB-block objects, duplex + overlap | 2.95 | 2.03 | 8.05 | 9.44 | 9.34 | 1.11 |

Per-stage timings (two single-stream GETs of the 2 GiB, 48 MiB-stripe object):

| Stage | Phase 2 | Phase 3 |
| --- | --- | --- |
| `emit` per stripe | 14.4 ms | 6.6 ms |
| `duplex_backpressure` per wait | 1.2 ms | 0.55 ms |
| `stripe_read` per stripe | 12.4 ms | 10.6 ms |
| `reader_setup` per decode run | 8.8 ms | 10.4 ms |

The write no longer stalls the decoder: emit halved and backpressure waits
halved. What remains per 48 MiB stripe is the shard read (10.6 ms) and a
reader setup of about 10 ms that is paid once per decode run, and the
summary shows two decode runs per 96 MiB part, so it is paid per stripe.
Single-stream run-to-run spread is about 0.3 GB/s on this cluster, which
is why the larger duplex pipe shows no gain here. Gate 2: a model load is
within 20 percent of the link rate through the loader (Phase 1), so Phase 5
server-side fan-out is skipped; the next single-stream work is the reader
setup and the stripe read/emit overlap in the decode loop.

## Phase 3b: two buffer generations per shard slot

Depth-1 prefetch reads stripe N+1 while stripe N is emitted, but the shard
buffer pool kept one buffer per slot, so every prefetched read allocated and
page-faulted a fresh 4 MiB buffer and the returned one was dropped: 64 MiB of
fresh pages per stripe. The pool now keeps two generations per slot.

| Variant (4 MiB-shard objects, pipelined writes) | Single RDMA GET | Single HTTP GET | CPU s/GiB |
| --- | --- | --- | --- |
| defaults, 96 MiB parts | 2.95 GB/s | 1.50 GB/s | 1.03 |
| defaults, 480 MiB parts | 2.23 | 1.55 | 1.07 |
| prefetch 2, 96 MiB parts | 2.74 (was 1.93 before the fix) | 1.55 | 1.04 |
| prefetch 2, 480 MiB parts | 2.88 | 1.81 | 1.08 |
| prefetch 2 + duplex 48 MiB | 2.89 | 1.90 | 0.90 |
| prefetch 2 + duplex 48 MiB + bitrot overlap | 2.98 | 2.03 | 0.85 |
| same switches, 1 MiB-block objects | 2.67 | 1.99 | 0.98 |

Prefetch no longer regresses, but no switch combination moves a single
stream past about 3 GB/s, and larger multipart parts do not help, so neither
per-part reader setup nor read/emit overlap is the ceiling. The stage data
says what is: within one stripe the first shard lands after 0.9 ms and the
twelfth after 10.5 ms, i.e. the twelve shard reads complete about 0.9 ms
apart, the time to bitrot-hash one 4 MiB shard on one core. Shard
verification runs on the decode task, serially, so `stripe_read` is CPU
bound at roughly 4.6 GB/s of hashing and `emit` adds the reconstruct and
copy on the same core. The next step is to verify shards on separate
cores.

## Phase 3c: shard bitrot verification off the decode task

Frames of at least `RUSTFS_BITROT_VERIFY_OFFLOAD_MIN_BYTES` (default 2 MiB;
the benchmark binary used 1 MiB, identical for 4 MiB shards) are hashed on the
blocking pool, so the twelve shards of a stripe verify on twelve cores instead
of one after another on the decode task.

| Variant (4 MiB-shard objects, pipelined writes) | Single RDMA GET | Single HTTP GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| off-thread verification, defaults | **4.65 GB/s** | 2.16 GB/s | 7.04 GB/s | 9.15 GB/s | 8.25 GB/s | 0.97 |
| + prefetch 2 | 4.12 | 1.96 | 6.96 | 10.54 | 8.53 | 1.04 |
| + prefetch 2 + duplex 48 MiB + bitrot overlap | 3.40 | 1.91 | 6.99 | 10.37 | 8.51 | 1.09 |
| offload disabled (`...=0`), prefetch 2 | 2.99 | 2.00 | 6.94 | 9.55 | 8.42 | 0.84 |
| 1 MiB-block objects (stay inline), prefetch 2 | 3.60 | 2.53 | 8.38 | 9.43 | 9.29 | 0.72 |

Per-stage timings (three single-stream GETs of the 2 GiB, 48 MiB-stripe
object; 3.8-5.0 GB/s run to run):

| Stage | Phase 3 | Phase 3c |
| --- | --- | --- |
| `stripe_read` per stripe | 10.6 ms | 4.4 ms |
| `emit` per stripe | 6.6 ms | 6.5 ms |
| `reader_setup` per decode run | 10.4 ms | 9.7 ms |

A single RDMA stream is now 2.1x the Phase 0 baseline (4.65 vs 2.24 GB/s)
and runs above the HTTP path, which still copies its body through the
response. The next ceilings, in order: the reader setup, which the stage
summary shows is paid twice per 96 MiB part (two decode runs per part, about
10 ms each), and the emit stage (reconstruct plus the copy into the pipe,
6.5 ms per 48 MiB, one core). The HTTP path shows the same decode-side
gains only partially because its body copy stays serial.

## Phase 4: object data cache, KV-chunk latency

Cache on every node (`RUSTFS_OBJECT_DATA_CACHE_ENABLE=true`,
`RUSTFS_OBJECT_DATA_CACHE_MODE=fill_materialize_enabled`,
`RUSTFS_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES=67108864`,
`RUSTFS_OBJECT_DATA_CACHE_MAX_BYTES=34359738368`), fill on GET. 32 objects of
48 MiB on 4 MiB shards (one stripe each), one RDMA GET at a time into GPU
memory from tray17, cold = first GET of each object, warm = five more rounds
(`scripts/bench/rdma-get/kvlat.py`):

| Cache | cold p50 | cold p99 | warm p50 | warm p99 | warm per-GET rate |
| --- | --- | --- | --- | --- | --- |
| off | 23.0 ms | 53.1 ms | 21.4 ms | 29.3 ms | 2.35 GB/s |
| on, first pass | 36.4 ms (fill) | 119.0 ms | 11.9 ms | 19.9 ms | 4.22 GB/s |
| on, second pass | 12.6 ms | 22.5 ms | 11.7 ms | 19.7 ms | 4.31 GB/s |

A hit skips the shard reads, bitrot and decode, but the body still goes
through the cuObject staging copy (48 MiB into 16 MiB chunks, then the RDMA
write), which is what the 11.8 ms is: the cold < 15 ms / warm < 5 ms targets
need the fill-on-PUT and the registered cache arena of Phase 6 (RDMA write
straight from the cache entry), not more cache. Filling on GET costs the
first reader about 13 ms extra. The cached 2 GiB object is not affected
(entries are capped at 64 MiB); single stream stayed at 4.77 GB/s.

With `RUSTFS_OBJECT_DATA_CACHE_FILL_ON_PUT=true` (Phase 4 code change: the
streaming PUT keeps its plaintext frames and inserts them after the commit)
the first GET after the PUT is already a hit:

| Cache | first GET p50 | first GET p99 | warm p50 | warm p99 |
| --- | --- | --- | --- | --- |
| on, fill on GET | 36.4 ms | 119.0 ms | 11.9 ms | 19.9 ms |
| on, fill on PUT | **11.8 ms** | 21.5 ms | 11.0 ms | 21.2 ms |

Also found in this phase: with `RUSTFS_RDMA_POOL_BYTES=2147483648` the
32-stream runs exhausted the internode registered pool on the storage nodes
(`rdma_transport_fallback ... pool exhausted`, 10 s TCP backoff), which is
why 32 in flight scored below 16 in the earlier tables. Use 8 GiB per node
for that concurrency, as the settings reference says.

## Results by phase

| Phase | Build | Single RDMA GET into GPU | Single HTTP GET | 16 ranged RDMA GETs in flight | 32 in flight | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| 0 baseline | 039443eef | 2.24 GB/s | 2.30 GB/s | 9.14 GB/s (8 GiB, host) | 9.22 GB/s | 1.02 |
| 0 all switches | 039443eef | 3.11 GB/s | 2.65 GB/s | 8.97 GB/s | 8.76 GB/s | 0.77 |
| 1 loader, 16 in flight | 039443eef | (model load 7.79 GB/s) | | | | |
| 2 4 MiB shards | ec92a122b | 2.57 GB/s | 2.01 GB/s | 11.21 GB/s | 7.79 GB/s | 0.94 |
| 3 pipelined RDMA writes | 914bec288 | 3.17 GB/s | 2.04 GB/s | 9.00 GB/s | 8.42 GB/s | 0.84 |
| 3b buffer pool fix (prefetch 2) | fb18e317f | 2.74-2.98 GB/s | 1.55-2.03 GB/s | | | 0.85-1.04 |
| 3c off-thread bitrot verification | f570dfcd4 | **4.65 GB/s** | 2.16 GB/s | 9.15 GB/s | 8.25 GB/s | 0.97 |
| 4 object data cache (48 MiB KV chunk) | f570dfcd4 | warm hit 11.8 ms p50 (4.2 GB/s) vs 21.4 ms off | | | | |
| 4 fill on PUT | (phase 4) | first GET after PUT 11.8 ms p50 (was 36.4) | | | | |
