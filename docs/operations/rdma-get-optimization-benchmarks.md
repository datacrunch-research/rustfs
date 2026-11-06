# RDMA GET optimization: benchmark record

Cluster: 4 GB300 trays (tray06/07/17/18), one RustFS node per tray with 4 local
disks, EC 12+4, RoCE bond `mlx5_bond_0` (2 x 100 Gb/s per node), internode
transport `rdma`, cuObject S3-over-RDMA enabled. Client: tray17, direct cuObject
client into GPU memory unless noted. Object: `models/bench/rand-8g.bin`
(8 GiB random bytes, 1 MiB erasure block) unless noted. Each number is the best
of three rounds. Plan: `tmp/rustfs-optimization-plan.md`.

## Where things stand (2026-11-03)

| Benchmark (plan target) | Phase 0 | Now |
| --- | --- | --- |
| Single RDMA GET into GPU, 2 GiB (>= 8 GB/s after Phases 2-3) | 2.24 GB/s | 5.48 GB/s (2.4x) |
| Model load through the SGLang loader (within 20% of the 16-stream aggregate) | 1.15 GB/s (1 in flight) | 7.8 GB/s, 16 in flight (met) |
| 48 MiB KV chunk GET cold / warm p50 (< 15 ms / < 5 ms) | 23 ms / 21 ms | 11.8 ms first GET after PUT / 11 ms warm |
| 64 concurrent GETs across clients (no TCP fallback) | | 2 clients x 32 streams: 17 GB/s, no fallback with an 8 GiB internode pool |
| One node down during load (stall < 5 s, no 503) | 25 s worst stall | 10.6 s worst stall, no failures (moderate timeouts) |

The single-stream target of 8 GB/s is not reached: with the pipe one block
deep and prefetch on, the read and emit of consecutive stripes overlap, and
what remains is the two 48 MiB copies per stripe (decoder into the pipe,
consumer into the cuObject staging) which the zero-copy emit work removes. The warm KV target needs the registered cache arena (Phase 6).

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

## Phase 4c: one node down (freeze drill)

`scripts/bench/rdma-get/drill.py` freezes the rustfs process on tray18 with
SIGSTOP for 40 s (a black hole: the host answers ARP and TCP keeps the
connections, nothing is reset) while one client runs a GET loop (96 MiB
uncacheable object on 4 MiB shards, RDMA into host memory, 0.2 s apart) and
a PUT loop (8 MiB objects, 1 s apart). No request failed in any run; the
numbers are latencies while the node was frozen.

| Settings | GET p50 frozen | GET worst | PUT p50 frozen | PUT worst | single-stream GET after |
| --- | --- | --- | --- | --- | --- |
| defaults (connect 3 s, RPC 30 s, disk read 10 s, write stall 30 s, RDMA stall 30 s) | 67 ms | 5.1 s | 57 ms | 24.9 s | 4.6 GB/s |
| fast (connect 1 s, RPC 3 s, HTTP/2 keepalive 1/3 s, TCP keepalive 2 s, disk read 2 s, write stall 3 s, RDMA stall 2 s, lock RPC 1 s, reprobe 2 s) | 66 ms | 6.9 s | 57 ms | 6.4 s | 2.7 GB/s |
| moderate (connect 1 s, RPC 5 s, disk read 5 s, write stall 5 s, RDMA stall 5 s, lock RPC 1 s, reprobe 2 s; keepalives default) | 59 ms | 10.1 s | 53 ms | 10.6 s | 3.8 GB/s |

Reads already engage parity within one shard-read timeout, so the GET
stall is bounded by the disk read timeout plus one RPC round; writes wait
for the frozen peer until the write-stall budget, which is the 25 s. The
worst case is twice the budget in each set (two rounds against the frozen
peer before it is treated as offline), so the plan's 5 s target needs the
peer circuit breaker from the Phase 4c task list, not smaller timeouts:
the 2 s budgets already cost single-stream throughput (2.7 GB/s) while the
moderate set stays within the normal 3.8-5.0 GB/s spread. The moderate set
is the recommended one until the breaker lands. Before and after the freeze the
latencies were identical to a healthy cluster; the returning node needed
no manual action.

Two clients at once (tray17 and tray18, each 32 ranged RDMA GETs of the
8 GiB object spread over the four trays): 8.52 + 8.48 GB/s, about 17 GB/s
aggregate from four storage nodes, no TCP fallback (8 GiB internode pool).

## Phase 3d: a pipe one block deep

`RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE` used to act only as a cap: the adaptive
sizing topped out at 8 MiB for large objects, so the earlier 48 and 64 MiB
runs changed nothing. An explicit value above the 4 MiB default now raises
the pipe for objects over 256 MiB. Binary: `fast` profile (no LTO, 16
codegen units), which measured the same as the release build (4.97 vs
4.65 GB/s is within the run-to-run spread). Also includes the abort-path
fix for pipelined writes (a staging chunk is owned by its blocking write
until the DMA returns).

| Variant (4 MiB-shard objects) | Single RDMA GET | Single HTTP GET | 16 x 2 GiB GPU | 16 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- |
| defaults (8 MiB pipe) | 4.97 GB/s | 2.96 GB/s | 7.22 | 9.11 | 0.87 |
| 64 MiB pipe | 3.53 | 1.83 | 7.07 | 9.08 | 1.19 |
| 64 MiB pipe + prefetch 2 | **5.48** | 2.75 | 6.98 | 9.28 | 0.96 |
| 128 MiB pipe + prefetch 2 | **5.48** | 2.75 | 7.02 | 9.19 | 0.89 |
| prefetch 2 (8 MiB pipe) | 4.22 | 2.14 | 7.26 | 9.10 | 1.04 |

With a pipe that holds a whole 48 MiB stripe the decoder's emit no longer
blocks on the consumer, and with prefetch the next stripe's read overlaps
it: 5.48 GB/s, 2.4x the baseline. A big pipe alone is slower (the decoder
runs ahead and the two copies then contend), and 128 MiB adds nothing over
64 MiB. The remaining chain is the two 48 MiB copies per stripe (decoder
into the pipe, consumer into staging) on two cores; removing them is the
zero-copy emit work.

Recommended settings for the RDMA GET path, in addition to the ones from
Phases 2 and 3: `RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=67108864` and
`RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT=2`.

## Phase 4 (write path): cuObject PUT

`PutObject` and `UploadPart` accept `x-amz-rdma-token` with an empty body:
RustFS RDMA-reads the object from the client's buffer into staging, one
16 MiB chunk ahead of the writer, and runs the ordinary PUT pipeline on it.
Verified byte-exact from GPU and pinned host memory (direct client,
`putdriver.py` / `mpdriver.py`), and the first GET after an RDMA PUT with
fill-on-PUT is a cache hit (10-15 ms, same as after an HTTP PUT).

| Write | RDMA PUT | HTTP PUT (same client) |
| --- | --- | --- |
| single 1 GiB object | 0.39 GB/s (2.8 s) | 0.26 GB/s (4.2 s) |
| multipart 4 x 96 MiB parts, sequential | 1.07 s per part | |
| 16 concurrent 48 MiB objects | 0.58 s each, 1.35 GB/s aggregate | 0.70 s each, 1.10 GB/s |

The RDMA read is not the limit (a 48 MiB chunk lands in about 4 ms); the
PUT pipeline is: a 48 MiB object takes about 0.2 s alone and 0.58 s with 16
in flight, so one storage node drains about 1.3 GB/s of KV chunks. The
plan's 20 GB/s per node prefill-drain target therefore needs PUT-side
work (erasure encode and shard write pipelining), which is outside the GET
plan; this phase gives the KV write path its RDMA transport and the
cache-hit on first read.

## Staging cluster port (2026-11-04): 4 x B300 on Kubernetes

Cluster: `llm-inference-staging-euc1`, nodes b300-110/121/139/141 (Intel Xeon 6767P, 256 CPUs, 8 x B300, driver 580.178 open kernel modules, Ubuntu 24.04 kernel 7.0 with the inbox RDMA stack). RustFS runs as one hostNetwork, privileged pod per node (namespace `inference-rustfs-rdma`, binary and scripts on a CephFS volume), four local NVMe drives per node under `/data0X/rustfs-rdma`, EC 12+4 over 16 drives, internode transport `rdma`, cuObject enabled, same settings as Phase 3d/4 (4 MiB shards, 8 GiB internode pool, 64 MiB pipe, prefetch 2, cache with fill-on-PUT). Client: a GPU pod on b300-121 (as requested) and one on b300-110; the storage node of the client's host is the endpoint, as on the trays. Binary: HEAD `8d0b192cb` (GET sink) plus the cold-fill fix below, `fast` profile.

Differences from the GB300 trays that matter for the numbers:

- CPU-side RDMA NIC is an 802.3ad bond of two separate CX-7 cards (`enp83s0np0` = `mlx5_6`, `enp204s0np0` = `mlx5_11`, 400 Gb/s each). RoCE over such a bond has no hardware LAG: rdma_cm picks `mlx5_6` for the bond IP on every node, and the two port groups cannot reach each other over RoCE (`ib_write_bw` between `mlx5_6` and `mlx5_11` never completes), so every node must use the same port. `ib_write_bw` node to node: 392 Gb/s on `mlx5_11`, 59 Gb/s on `mlx5_6` whenever b300-121 is one end.
- b300-121's PCIe switch `48:00.0` (above its `mlx5_6` and the NVMe drives at /data00 and /data01) trained at Gen5 x2 ("downgraded" in `LnkSta`), i.e. about 7.4 GB/s for that NIC and those two drives together; the other three nodes train x16 there. Until RDMA is moved to `mlx5_11` (devlink `enable_roce=false` on `pci/0000:53:00.0` on all four nodes, pending), every stripe read waits on four shards behind that link and a 121-hosted GET loops its cuObject write through it twice.
- The cuObject client needs cuFile's dmabuf registration (`CUFILE_DMABUF_ENABLE=true`, `rdma_dev_addr_list` under `properties` in `cufile.json`): `nvidia-peermem` cannot load against the inbox `ib_core` (no peer-memory API) and the GPUs report no PCI p2pdma support. Nothing else is needed on the host.
- b300-139 and b300-141 also serve an inference deployment (all 8 GPUs each) during these runs.

Found and fixed here: with `RUSTFS_OBJECT_DATA_CACHE_MODE=fill_materialize_enabled`, a cache miss on a cacheable object (<= 64 MiB) materializes the body for the cache before replying; the GET-sink build let the sink take those bytes, the materialization came up short and the RDMA GET failed with `less data available than what was requested` (HTTP GETs were unaffected). The cold-fill producer now drops the sink and the RDMA reply is served from the materialized body, like a cache hit. Byte-exact verification after the fix (121 client): 2 GiB on 4 MiB shards into GPU, the 988 MB file into GPU and into pinned host memory, a 20 MB range, and a 48 MiB KV chunk.

Results on the slow port group (before the devlink change), 110 client, best of three, defaults plus prefetch 2 (`benchk.sh`, the k8s port of `bench4.sh`):

| Variant | Single RDMA GET | Single HTTP GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| GET sink + cold-fill fix, defaults (prefetch 2) | 4.36 GB/s (3.0-4.4 spread) | 0.82 GB/s | 8.51 GB/s | **14.25 GB/s** | 12.79 GB/s | 2.09 |
| + `RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT=1` | 4.31 | 0.75 | 8.37 | 13.10 | 12.73 | 2.18 |
| + `RUSTFS_CUOBJECT_INFLIGHT_WRITES=6` | 3.18 | 0.78 | 8.32 | 13.16 | 12.47 | 2.13 |
| + `RUSTFS_CUOBJECT_CHUNK_BYTES=33554432` | 3.52 | 0.77 | 8.35 | 13.13 | 12.48 | 2.14 |

KV chunks (32 x 48 MiB, one RDMA GET at a time into GPU memory, cold = first GET after a server restart, warm = five more rounds):

| Client | cold p50 | cold p99 | warm p50 | warm p99 | warm per-GET rate |
| --- | --- | --- | --- | --- | --- |
| b300-110 (x16 link) | 49.8 ms | 120.4 ms | 15.5 ms | 26.4 ms | 3.25 GB/s |
| b300-121 (x2 link) | 57.9 ms | 90.5 ms | 20.2 ms | 38.9 ms | 2.49 GB/s |

Reading: none of the handoff's variants moves a single stream (all within the 3.0-4.4 GB/s run-to-run spread), which is expected while the stripe loop waits on the slow link. The 16-stream host aggregate is already above the tray numbers (14.3 vs 9-11 GB/s; 400 Gb/s ports and 16 drives), while a single stream (4.4 GB/s) and the warm KV hit (15.5 ms) trail the trays (6.33 GB/s, 11 ms) because the stripe's four shards from b300-121 cross its Gen5 x2 link and all RDMA still rides `mlx5_6`. A 121-hosted single stream is lower still (2.6-3.1 GB/s), as predicted from the loopback write through the same link. The HTTP column is the direct client's boto3 reader on this pod (0.8 GB/s); a plain streaming GET from the 121 pod reached 2.0 GB/s. CPU per GiB is about twice the Grace figure and needs a per-stage look once the fabric is fixed.

## Phase 5 (revisited): server-side stripe fan-out for cuObject GETs

Phase 5 was skipped on the trays because a model load already ran within 20
percent of the 100 Gb/s link through the loader; at 400 Gb/s that gate no
longer holds, and the stage data says why a single stream stalls: one decode
task handles one 48 MiB stripe at a time (read, verify, reconstruct, emit),
about 11 ms per stripe at 4.4 GB/s, while 50 GB/s needs a stripe every
millisecond. cuObject writes land at fixed client offsets, so the server can
split one GET into erasure-block-aligned segments and decode and write them
concurrently; only the reply waits for all of them.

`RUSTFS_CUOBJECT_GET_FANOUT` (default 8; 1 = sequential) is the number of
segments one GET may use. The sink offered to the storage layer carries a
segment factory; the legacy duplex read path plans at most that many
contiguous ranges cut only at block boundaries (so no stripe is read twice),
runs one producer per segment with its own staging chunks and RDMA writes,
and reports the first error. A GET that spans fewer than two blocks, a
compressed or encrypted object, a part-number read, or a cache hit stays on
the sequential path. The fan-out is also bounded by the staging chunks the
pool can give the GET at that moment, so under load GETs fall back to fewer
segments rather than thrash.

First pass on the staging cluster (110 client, slow port group, 1 GiB staging
pool, before the pool-wait fix), same binary with the knob at 1 as the control:

| Variant | Single RDMA GET | Single HTTP GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| fan-out 1 (control) | 3.10 GB/s | 0.76 GB/s | 8.31 GB/s | 13.29 GB/s | 12.84 GB/s | 2.23 |
| fan-out 8 | **8.79 GB/s** | 0.81 | 6.59 | 3.69 | 3.46 | 2.61 |
| fan-out 16 | **8.90 GB/s** | 0.81 | 7.00 | 3.24 | 3.60 | 2.59 |

A single stream doubles and crosses the plan's 8 GB/s target even on the slow
port group (988 MB file: 5.2 GB/s into GPU, 6.8 GB/s into pinned host; all
byte-exact checks pass with the fan-out). Sixteen segments bring nothing over
eight, so the next ceiling is outside the stripe loop. The many-stream rows
collapsed because 16 GETs x 8 segments x 3 chunks exceed the 64 chunks of the
1 GiB pool, and a segment without a chunk re-polled in a tight loop; the fix
parks such a segment on a waker and bounds the fan-out by the chunks the pool
can give the GET (second pass below).

Second pass (segments park on a waker when the GET has no free chunk; fan-out bounded by the chunks the pool can give the GET):

| Variant | Single RDMA GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- |
| fan-out 8, 8 GiB staging pool | 8.32 GB/s | 6.53 | 3.65 | 3.76 | 2.63 |
| fan-out 8, 1 GiB pool (default) | 9.17 | 6.75 | 3.65 | 3.76 | 2.73 |
| fan-out 16, 8 GiB pool | 9.29 | 6.74 | 3.68 | 3.75 | 2.75 |

Same aggregate with an 8x larger pool, so the pool was not the limit. The
stage summary of the 110 node during the 16 x 8 GiB run says what is:
`reader_setup` ran 96 times in one 5 s window at 1,155 ms each (about 10 ms
in a single stream) and `stripe_read` went from 4 to 20 ms. Sixteen ranged
GETs of 512 MiB spread over four nodes used to be 4 producers per node; with
8 segments each they became about 32, every one of them opening 16 shard
streams per part for one or two stripes, so the setups stampede each other.
Third pass: a segment must be at least `RUSTFS_CUOBJECT_GET_FANOUT_MIN_BYTES`
(default 192 MiB, four blocks), and a node-wide budget of extra segment
producers (`RUSTFS_CUOBJECT_GET_FANOUT_BUDGET`, default 16) is taken without
waiting and held for the GET, so many-stream load degrades to the sequential
path instead of multiplying producers.

### Fabric fix and two drives per node

With RoCE disabled on the enp83 function of every node (`devlink dev param
set pci/0000:53:00.0 name enable_roce value false cmode driverinit` and a
`devlink dev reload`; `mlx5_6` still lists but rdma_cm lands on `mlx5_11`:
`ib_write_bw -R` 121 to 110 went from 59 to 369 Gb/s; the setting does not
survive a reboot), and the cluster rebuilt on two drives per node (/data02 and
/data03, the x16 side on 121; 8 drives, `RUSTFS_STORAGE_CLASS_STANDARD=EC:2`,
so 6+2 with 24 MiB blocks; objects re-uploaded), same binary as the second
pass (fan-out 8, no budget yet), 110 client:

| Variant | Single RDMA GET | 988 MB GPU / host | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | warm KV p50 | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fan-out 8, 8 drives 6+2, RDMA on the x16 ports | 7.49 GB/s (7.1-7.5) | 7.8 / 9.1 GB/s | 5.66 | **20.78 GB/s** | 20.33 | 13.2 ms | 3.07 |

The many-stream aggregate rose by half once no stripe crossed the x2 link,
even with the producer stampede still in place; the single stream is a little
below the 16-drive run (8 drives and 24 MiB stripes mean more stripes and
reader setups per byte).

Third pass (minimum segment 192 MiB, node budget shared fairly among open
GETs), same fabric and layout as the row above, 110 client:

| Variant | Single RDMA GET | 988 MB GPU / host | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | warm KV p50 | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fan-out 8, budget 16 (defaults) | 7.18 GB/s (7.0-7.2) | 8.4 / 8.8 GB/s | 9.96 | 20.86 | 26.08 | 16.6 ms | 2.98 |
| fan-out 8, budget 8 | 8.07 (6.7-8.1) | | **10.38** | **22.28** | **26.92** | | 2.94 |
| fan-out 8, minimum 48 MiB, budget 64 | 8.66 (7.0-8.7) | | 7.94 | 16.82 | 16.37 | | 2.82 |

The fair share restores the many-stream rows (16 x 2 GiB GPU back above the
pre-fan-out 8.5 GB/s, 32 streams at a new high of 26-27 GB/s) while a single
stream keeps the fan-out gain; a 48 MiB minimum with a large budget brings
part of the stampede back. Budget 8 and 16 are within the run-to-run spread
of each other; the defaults stay at 192 MiB and 16. The warm KV hit is still
the sequential path (one stripe) and unchanged by the fan-out.

## Zero-copy emit, step 2: RDMA writes straight from the shard buffers

Step 1 (the GET sink) removed the duplex pipe; the decoder still copied each
stripe into cuObject staging chunks before the RDMA write. Step 2 removes
that copy for data shards:

- The erasure read path's shard buffers are a `ShardBuf` instead of a
  `Vec<u8>`: heap memory, or a block of a registered arena when one is
  installed and the shard fits (`RUSTFS_CUOBJECT_SHARD_ARENA_BYTES`, default
  0 = off; blocks of `RUSTFS_ERASURE_SHARD_BYTES`, registered with the first
  cuObject server at startup). Shards that do not fit, reconstruction
  scratch, and every non-cuObject path keep the heap.
- A sink writer can carry a shard emitter. The legacy duplex producer offers
  it to the decode loop for the whole read (task-local), and the emit stage
  hands over any data shard that lives in the arena instead of writing its
  bytes into the stream; the emitter RDMA-writes the shard in place, in
  stream order with any staged bytes, and the shard buffer goes back to the
  arena when the DMA returns. Shards that were emitted are not recycled into
  the decoder's pool; the next stripe takes fresh arena blocks.
- Partial shards at range edges, cache hits, compressed or encrypted objects
  and reconstruction output still go through staging, so the path is
  byte-exact by construction: the same bytes are sent, from a different
  address.

### Client GPU placement

The 16 x 2 GiB GPU rows above (about 10 GB/s, 0.6 GB/s per stream) and the
single-stream GPU rows were limited on the client, not in RustFS: the
storage NIC (`mlx5_11`, PCI cc:00.0) is on NUMA node 1 while the benchmark
allocated on GPU 0, which is on socket 0, so every RDMA write into VRAM
crossed the UPI link as a PCIe peer-to-peer transaction. With the buffer on
a socket-local GPU (`CUDA_VISIBLE_DEVICES=4`; `nvidia-smi topo -m` shows GPUs
4-7 under the same socket as the NIC) and the same servers:

| Client GPU | Single 2 GiB RDMA GET | 16 x 2 GiB GPU |
| --- | --- | --- |
| GPU 0 (other socket) | 5.4-6.8 GB/s | 8.85 GB/s |
| GPU 4 (NIC's socket) | **10.8-10.9 GB/s** | **12.9 GB/s** |

(Measured while the build pod was loading the same node; relative, not
best-of-three.) The runners now default to the socket-local GPU; GPU rows
below use it. A model loader should likewise register the receiving buffer on
a GPU under the NIC's socket, or use that GPU's own NIC.

### Stream sweep (fair-share budget 16, x16 ports, 2 drives per node, client GPU 4)

`n` ranged RDMA GETs of one object in flight, spread over the four nodes, best
of two (`sweep.sh`). The 8 GiB object cannot be split into fewer than four
registrations (a cuObject registration stays under 4 GiB), so its 1- and
2-stream points are missing.

| Streams | Segments per GET (budget 16) | 8 GiB into pinned host, aggregate (per stream) | 2 GiB into GPU 4, aggregate (per stream) |
| --- | --- | --- | --- |
| 1 | 8 | | 9.5 GB/s (9.5) |
| 2 | 8 | | 13.1 (6.6) |
| 4 | 4 | 21.4 GB/s (5.4) | 14.0 (3.5) |
| 8 | 2 | 20.8 (2.6) | 13.0 (1.6) |
| 16 | 1 | 24.1 (1.5) | 12.6 (0.8) |

The host aggregate reaches its plateau (about 21 GB/s) at four streams
already, because the fair share keeps 16 producers per node busy from two
streams up; the ceiling is the per-node producer pipeline (see the
reader-setup and remote shard-read latencies under load above), not the
client or the link. The GPU rows plateau at 13-14 GB/s for a different
reason: the storage NIC and the GPUs are not under one PCIe switch
(`nvidia-smi topo -m` shows `NODE`), so every RDMA write into VRAM is a
peer-to-peer transaction through the CPU root complex, which on this platform
tops out around that figure. Full-rate GPU-direct needs the GPU's own NIC
(the InfiniBand HCA that shares its PCIe switch, `PXB`), which is the
IPoIB/IB variant discussed in the handoff, not a RustFS change.

Results (same binary, x16 ports, 2 drives per node 6+2 with 4 MiB shards, 110
client on GPU 4, best of three; `RUSTFS_CUOBJECT_SHARD_ARENA_BYTES=8 GiB`
versus 0):

| Variant | Single 2 GiB RDMA GET into GPU | 988 MB GPU / host | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | warm KV p50 | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fan-out 8, arena off | 10.32 GB/s (9.1-10.3) | | 11.97 | 21.56 | 25.98 | | 2.43 |
| fan-out 8, arena on | **13.88 GB/s** (10.7-13.9) | 11.5 / 12.2 GB/s | 13.0 | **24.56** | 24.46 | 15.3 ms | 3.84 |
| fan-out 1, arena on | 5.67 (4.6-5.7) | | 12.06 | 24.91 | **27.95** | | **1.83** |

Reading: zero-copy emit adds about a third to a single stream (10.3 -> 13.9
GB/s, 6.2x the Phase 0 baseline) and a seventh to the 16-stream host
aggregate, and all byte-exact checks pass with the arena on. The GPU
aggregate rows stay at the client's PCIe peer-to-peer ceiling (13 GB/s, see
above). The warm KV hit is unchanged: a cache hit is served from the object
cache through staging, not from shard buffers (the cache arena of Phase 6).
Two things to look at next in this path: CPU per GiB rises with arena plus
fan-out 8 (3.8 s/GiB against 2.4) although it is lowest with arena plus
fan-out 1 (1.8), which points at the per-shard write submissions (one
blocking library call per 4 MiB shard instead of per 16 MiB chunk, thirteen
in flight per segment) rather than the copy itself; and the fan-out 1 rows
show the 32-stream aggregate highest when each GET is sequential, i.e. the
per-node producer pipeline, not the fan-out, sets the many-stream ceiling.

Found in this pass: the cuObject server library has `RUSTFS_CUOBJECT_NUM_DCIS`
(128) channels and every blocking transfer holds one; with thirteen
zero-copy shard writes in flight per segment, enough segments exhaust them
and the library returns `EBUSY` (`cuobject channel alloc failed: Device or
resource busy`), which failed the segment, cancelled its siblings and
surfaced to the client as `Operation canceled` (41 failures on one node
during the 32-stream rows, hidden by client retries, and a failed GET in the
first multi-GPU run). Transfers now wait on a permit per channel instead of
failing, and the emitter now batches a stripe's data shards into one
scatter-gather RDMA write (the library's `getRDMABufferFromSgList`, up to 10
ranges per operation): one library call and one channel per stripe for 6+2,
two for 12+4, instead of one per 4 MiB shard. The multi-GPU rows below are
from those builds.

### Internode idle-connection pool depth (refuted)

`RUSTFS_RDMA_IDLE_CONNS_PER_PEER` at 64 and 256 (batched zero-copy build,
fan-out 8, 110 client) changed nothing: single stream 11.3 / 11.5 GB/s, 16 x 8
GiB host 21.2 / 20.8 GB/s, 32 streams 24.7 / 25.3 GB/s, the same as the
default 16 on healthy rows, and the `rdma_fresh_connect` counter stayed under
128 dials for the whole run. The setting stays (it is the right knob for
clusters with more producers per peer), but the stripe-read slowdown under
load on this cluster was not connection dials.

### b300-121 removed

b300-121's PCIe fault was confirmed as a hardware error; several erratic rows
in this section (a 0.95 GB/s single-stream row, 60 s stalls in the
multi-GPU runs) line up with it holding two of every stripe's eight shards.
From here the storage cluster is b300-139 and b300-141 with four drives each
(8 drives, 6+2, 24 MiB stripes; objects re-uploaded) and the client stays on
b300-110, so every GET is served by a node that is not the client's.

### Two servers, four drives each (b300-139, b300-141), batched zero-copy

| Metric | Value |
| --- | --- |
| Byte-exact checks | 5 of 5 |
| Single 2 GiB RDMA GET into GPU 4 | 9.8 GB/s (7.9-9.8) |
| 988 MB into GPU / pinned host | 6.6 / 7.1 GB/s |
| 16 x 2 GiB GPU | 10.1 GB/s |
| 16 x 8 GiB host | 18.2 GB/s |
| 32 x 8 GiB host | 17.8 GB/s |
| 48 MiB KV hit, cold / warm p50 | 39 / 36 ms |
| CPU s/GiB | 1.96 |

Two serving nodes instead of three or four, each also hosting a running
inference deployment, so the aggregate rows are lower; the per-node figures
(about 9 GB/s each) match the earlier runs.

### Multi-GPU client (four processes, 2 GiB each, two servers)

| Destination | Aggregate (3 rounds) |
| --- | --- |
| four processes, all into GPU 4 | 13.0-17.5 GB/s |
| four processes, pinned host memory | 13.0-17.1 GB/s |
| four processes, one into each of GPUs 4-7 | **5.2-6.3 GB/s** |

Several client processes are fine and the servers are fine; concurrent
peer-to-peer writes from the storage NIC into several GPUs (each under its
own PCIe switch) through the CPU root complex run at a third of the
single-GPU rate. Together with the single-GPU ceiling of 13-14 GB/s this
means multi-GPU model loading at full rate needs each GPU's own InfiniBand
NIC (or one landing GPU plus NVLink), not a RustFS change.

Stream sweep on the two servers (`n` ranged GETs spread over 139 and 141,
best of two; the 8 GiB object cannot be split into fewer than four
registrations):

| Streams | 8 GiB into pinned host, aggregate (per stream) | 2 GiB into GPU 4, aggregate (per stream) |
| --- | --- | --- |
| 1 | | 10.6 GB/s |
| 2 | | 13.1 (6.6) |
| 4 | 15.9 GB/s (4.0) | 11.5 (2.9) |
| 8 | 11.9 (1.5) | 9.8 (1.2) |
| 16 | 18.1 (1.1) | 9.3 (0.6) |

With two serving nodes the host aggregate plateaus at 16-18 GB/s (about 9
GB/s per node, as before); the GPU rows sit at the client's peer-to-peer
ceiling.

Decode-loop settings on the two servers (batched zero-copy, fan-out 8):

| Variant | Single RDMA GET | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- |
| defaults (prefetch 2) | 9.8 GB/s | 18.2 | 17.8 | 1.96 |
| prefetch 4 | 9.1 | 16.5 | 15.2 | 1.90 |
| prefetch 8 | 9.3 | 15.0 | 15.0 | 1.99 |
| `RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE=true` | 8.0 | 15.7 | 17.5 | 1.95 |
| `RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE=true` | 8.0 | 16.4 | 18.1 | 1.93 |
| prefetch 4 + locality | 10.2 | 14.4 | 15.6 | 1.95 |

None of the stripe-loop knobs moves the numbers, which matches the stage
summary: under load a producer spends about 120 ms per part in
`reader_setup` against 6 ms per stripe read and 5 ms per emit, so the next
work is the per-part shard-stream open, not the loop. On the two-server
cluster the serving nodes dial hundreds of fresh internode connections per
run (the `rdma_fresh_connect` counter on b300-139 passed 512): with one peer
per node, 64 concurrent remote shard streams outrun 16 idle connections every
stripe round. The earlier pool-depth test read the counter on the
client-facing node of the four-node cluster, which did not see this.

With the legacy read path now recording the reader-setup sub-stages (a
single-stream window on b300-139, 181 stripes in 5 s):

| Sub-stage (per shard open unless noted) | n | avg |
| --- | --- | --- |
| `reader_setup` (per part) | 53 | 48.8 ms |
| `reader_setup_wait_quorum` (per part) | 53 | 48.7 ms |
| `reader_task_file_open` | 318 | 30.1 ms |
| `reader_open_stream` (remote shard) | 212 | 38.1 ms |
| `reader_stream_first_read` (remote, after open) | 212 | 0.7 ms |
| `reader_open_mmap_copy_success` (local shard) | 106 | 14.1 ms |
| `reader_mmap_copy_buffer` (local, blocking copy) | 106 | 10.6 ms |
| `stripe_read` | 181 | 8.7 ms |
| `emit` | 181 | 4.7 ms |

Two costs stand out. A remote shard open takes 38 ms while its first data
arrives 0.7 ms later, so it is the open (connection dial and accept, request
round trip) and not the read; and a local shard is opened through the mmap
copy path (`RUSTFS_OBJECT_MMAP_READ_ENABLE`, default on), which copies the
part's shard bytes into a heap buffer on a blocking task at open time, 10.6
ms per open, before the arena ever sees them. Both are measured next
(deeper idle pool on this cluster; mmap read off). Changing part size (96 ->
480 MiB) did not change throughput (8.8-9.9 GB/s single, 18.7 GB/s at 16
streams), so per-part setup is overlapped and the loop's remaining cost is
inside these opens plus emit.

Pool depth on the two servers:

| `RUSTFS_RDMA_IDLE_CONNS_PER_PEER` | Single RDMA GET | 16 x 8 GiB host | 32 x 8 GiB host | remote open avg | quorum wait avg |
| --- | --- | --- | --- | --- | --- |
| 16 (default) | 9.8 GB/s | 18.2 | 17.8 | 38 ms | 49 ms |
| 64 | 10.9 (9.6-10.9) | 19.0 | 20.7 | 38 ms | 49 ms |
| 256 | 9.4 (3.9-9.4) | 18.7 | 20.2 | 20 ms | 27 ms |

A deeper pool halves the remote open and the quorum wait but moves
throughput by 5-10 percent at most, and the dial counters stay low, so the
remaining 20 ms per remote open is the open request itself on the peer, not
the connection. What that "open" measures, from the transport code: the
client reserves a window, sends the stream request and waits for the first
event, which is the first 4 MiB chunk landing; the peer meanwhile opens the
part file and reads that chunk from its disk (through the same mmap copy
path when enabled) before the RDMA write. So the remote open latency is the
peer's first-chunk latency under load, and the local open latency is the
mmap copy; the next row turns the mmap path off on both sides.

### Reader setup: the remote open is the connection dial (2026-11-04)

The "20 ms open request on the peer" conclusion above was wrong; it came from
averages that mixed a warm pool with the first GET after a restart. Stage
timers inside the transport (`rdma_transport` path in the stage summary:
`dial_resolve`, `dial_setup`, `dial_establish`, `checkout_idle`,
`open_reserve`, `open_first_event` on the client; `serve_open`,
`serve_reserve`, `serve_first_fill` on the peer) split a remote shard open on
the two-server cluster (fan-out 8, 2 GiB object, 108 remote opens per GET):

| Stage (per remote open, avg) | first GET after restart | later GETs |
| --- | --- | --- |
| `dial_resolve` (address and route, blocking) | 12-25 ms | 8-12 ms |
| `dial_setup` (queue pair, control region) | 3 ms | 3 ms |
| `dial_establish` (connection-manager handshake) | 0.7-2.7 s | 47-57 ms (p50 57, most 20-100; 10-12 ms on an idle node) |
| peer accept (request event to established) | up to 5 s | 5 ms p50, 11 ms p90 |
| `open_reserve` (window slots and pool keys) | 307 ms | 0 |
| `open_first_event` (request to first chunk landed) | | 8 ms |
| peer `serve_open` (shard file open) | 527 ms | 0.4-0.7 ms |
| peer `serve_first_fill` (first 4 MiB from disk) | 6 ms | 6.5 ms |
| `reader_open_stream` (the whole open) | 1.5-2.0 s | 27-33 ms |
| `reader_setup_wait_quorum` | 1.5-2.0 s | 39-44 ms |

Three findings.

- Steady state: 32-40 of the 108 opens per GET dial a fresh connection (the
  pool held 16 idle connections per peer and a part opens 32 remote shards at
  once: 8 segments x 4 remote shards), and a dial costs 60-70 ms on a loaded
  node, of which the handshake itself is 50 ms (10 ms when the node is idle:
  the connection-manager events compete with the decode loops for the
  runtime). The peer's part is small: it opens the file in under a millisecond
  and reads the first chunk in 6.5 ms. So the open is the dial, and the
  quorum wait (6 of 8 shards) inherits it.
- First GET after a restart: 32 dials start at once; the peer's accepts of 31
  of them timed out at the 2 s connect timeout, the client saw
  `rdma connection establishment failed` five seconds in and put the peer on
  the 10 s TCP fallback, so the first GET ran at 0.3-0.4 GB/s. With a 15 s
  connect timeout no dial fails but each takes 1.7-2.7 s. The same GET also
  paid the registered pool: its 8 GiB are registered on the device at first
  use, under the pool lock, and 108 opens waited 33 s in total behind that
  one registration (`open_reserve` 307 ms average).
- Turning the mmap read path off on both nodes (`2srv4d-nommap` rows above)
  made throughput worse (5.6-7.4 GB/s single, 14 GB/s at 16 streams); the
  setting is not the lever here and the local open stays on the mmap copy.

The change in this tree: the listener registers the pool on its device at
startup; the idle pool keeps a minimum per peer
(`RUSTFS_RDMA_MIN_IDLE_CONNS_PER_PEER`, default 16) and refills it with at
most eight background dials whenever a stream takes a connection, remote
disks dial that minimum when they are created, and the pool default grows
to 64. A shard open therefore finds an idle connection unless the burst
exceeds the pool, and a dial storm no longer forms at the first GET.

With the pre-warm build (same harness, default pool 64 / minimum 16,
connect timeout 2 s):

| GET after restart | before | after |
| --- | --- | --- |
| first (cold pool) | 0.3-0.4 GB/s, 31 accept timeouts, TCP fallback | 6.2 GB/s, 44 dials, none failed |
| second, third | 7.3-8.2 GB/s | 11.7-11.8 GB/s |
| `reader_open_stream` avg (later GETs) | 27-33 ms | 9.3 ms |
| `reader_setup_wait_quorum` avg | 39-44 ms | 15-18 ms |
| dials inside a shard open | 32-40 per GET | 0 after the first GET (16 background refills per GET) |

Benchmark rows (fan-out 8, client GPU 4, two servers with four drives):

| Pool / minimum | Single RDMA GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- |
| 16 / none (before) | 9.8-10.9 GB/s | 9.0 | 18-19 | 20-21 | 2.4-2.6 |
| 64 / 16 (new defaults) | 12.0-12.7 (one round 6.2) | 12.9 | 20.4 | 22.5 | 1.86 |
| 256 / 64 | 12.0-12.5 | 12.1 | 20.8 | 20.7 | 1.86 |

Byte-exact on all verify checks (13.0 GB/s on the 2 GiB GPU GET). The
remaining 9 ms per remote open is the peer's first chunk (7.5 ms) and the
remaining quorum wait is that plus the local mmap copy; the aggregate rows
move little because 16 or 32 streams already overlapped the setup, and the
deeper pool adds nothing once the minimum covers a part's burst.

### Peer chunk read-ahead (2026-11-04)

With the pool pre-warmed, a remote open is 9 ms of which 7.5 ms is the
peer's first chunk: a ranged file stream reads 4 MiB chunks one after
another through tokio's file (two 2 MiB reads per chunk, each a blocking
round trip) with only the kernel's own read-ahead, so every chunk waits for
its own device round trips (`serve_fill`, now timed for every chunk, 6.5 ms
per 4 MiB; a stream delivered 600 MB/s). The ranged stream now queues
`posix_fadvise(WILLNEED)` in front of its position
(`RUSTFS_OBJECT_STREAM_READ_AHEAD_BYTES`, 16 MiB, topped up past the half
mark, on the blocking pool) and hands tokio a 4 MiB read buffer.

| Peer stage (avg) | read-ahead off | read-ahead 16 MiB |
| --- | --- | --- |
| `serve_fill` (every 4 MiB chunk) | 6.5 ms | 1.8-2.9 ms |
| `serve_first_fill` | 6.5-7.5 ms | 2.6-4.0 ms |
| `reader_open_stream` on the serving node | 9.3 ms | 10.1-13.8 ms (first build advised synchronously at open, 4-6 ms; now off-thread) |

Throughput did not move (same build, read-ahead on vs off: single 12.2 vs
12.5 GB/s, 16 x 2 GiB GPU 12.6 vs 11.7, 16 x 8 GiB host 21.2 vs 18.3, 32 x
8 GiB host 19.6 vs 22.1; all within the run-to-run spread), and the stripe
cadence on the serving node stayed at 7.9 ms per 24 MiB stripe
(`stripe_read_first_shard` 4.3 ms, quorum 7.9 ms) with the peer's chunks
now arriving in 2 ms. So the peer is not the limiter; the stripe read is
client-side work per shard (chunk copies into the shard buffer and the
bitrot verification round trip), serialized on the decode task. That is the
next item. Byte-exact on all verify checks.

Streaming the local shards too (mmap copy off, read-ahead on) is slower
again: every local open becomes a stream (162 opens per GET, 9-10 ms each),
the stripe read grows to 10.6-11 ms, single 10.3 GB/s, 16 x 8 GiB host 16.1,
32 x 8 GiB host 16.8. The mmap copy front-loads a local shard's bytes into
memory at open, which the per-stripe reads then get for free; the local
open stays on it.

### Stripe read: the shard copy moves beside the hash (2026-11-04)

Per shard and stripe the bitrot reader collected the chunks, waited for the
hash on the blocking pool and then copied 4 MiB into the shard buffer on
the decode task; the eight shard futures of a stripe share that task, so
eight copies (32 MiB) ran one after another per stripe. The copy now runs
inside the blocking task that verifies the shard, for the in-memory block,
the chunked (remote) frame and the scratch path alike, so a stripe's copies
and hashes spread over the blocking pool and the decode task only awaits.

| Serving node (avg) | before | after |
| --- | --- | --- |
| `stripe_read` (24 MiB stripe) | 7.9 ms | 4.9 ms |
| `stripe_read_first_shard` | 4.3 ms | 3.3 ms |
| `decode` per 128 MiB part | 41 ms | 33.6 ms |
| first GET after restart | 6.2-6.7 GB/s | 10.0 GB/s |
| later single GETs (same harness) | 11.7-12.0 GB/s | 13.7-14.6 GB/s |

Benchmark rows (fan-out 8, client GPU 4, two servers with four drives):
single 10.7-14.1 GB/s (verify run 14.2, the root-complex ceiling of this
client), 16 x 2 GiB GPU 13.3, 16 x 8 GiB host 19.1, 32 x 8 GiB host 23.1,
CPU 1.88 s/GiB; byte-exact on all verify checks. The aggregate rows did not
move: under 16-32 streams the stripe read holds at 5.3 ms but `emit` (the
cuObject RDMA write of a stripe) grows from 5.6 to 9.5 ms per stripe, and
`duplex_backpressure` shows up (1.6 ms, 12.5k samples), so with one segment
per stream the emit is now the cadence and the shared cuObject write path
is where the aggregate stops. The peer side is quiet throughout (chunk fill
2.3 ms, open 0.6 ms).

### Asynchronous cuObject transfers (2026-11-04)

Every RDMA write to the client was one blocking library call on the
blocking pool, holding a channel permit and a thread for the whole transfer
while the library polled its completion queue; under 16-32 streams the emit
of a stripe took 9.5 ms against 5.6 single-stream. The server library offers
an asynchronous mode (a handle at submission, a pollable completion
descriptor per channel, `processCompletions` to drain it), and the transfer
functions now use it (`RUSTFS_CUOBJECT_ASYNC_TRANSFERS`, default on): submit
on a channel of the transfer's own, register the channel's descriptor with
the runtime, wait for the completion carrying the handle, free the channel.
The blocking call stays as the fallback when the library offers no
descriptor, and `false` restores it. A completion's status is 0 for success
(the byte count only comes back from the synchronous call), which the first
build got wrong (every write reported as moving 0 bytes).

| Rows (same harness) | blocking (copy-offload build) | asynchronous |
| --- | --- | --- |
| single GET, two servers | 10.7-14.1 GB/s | 13.5-15.0 GB/s |
| 16 x 8 GiB host, two servers | 19.1 | 21.4 |
| 32 x 8 GiB host, two servers | 23.1 | 24.6 |
| 16 x 8 GiB host, one server | 15.7 | 18.4 |
| 32 x 8 GiB host, one server | 14.5 | 20.7 |
| `emit` per stripe under 16-32 streams | 9.5 ms | 8.1 ms |
| CPU s/GiB (mixed) | 1.88 | 1.98 |

One server now delivers 18-21 GB/s to the client at 16-32 streams and two
servers 21-25 GB/s. The GPU-target rows stay at the client's root-complex
ceiling (12-15 GB/s).

What bounds the 16-32 stream rows, measured afterwards:

- Not the fabric or the client's memory path: `ib_write_bw` (1 MiB writes,
  8 queue pairs) from one server into the client's host memory over
  `mlx5_11` runs at 392 Gb/s, and from both servers at once 179 + 179 Gb/s
  (45 GB/s into the client).
- Not one client process: two client processes with 8 streams each reach
  12.7 + 12.3 GB/s, four with 4 streams each about 19 GB/s in total, the
  same as one process with 16 streams.
- The rows are short, latency-bound transfers: the "16 x 8 GiB host" row is
  one 8 GiB object split into 16 ranged streams of 512 MiB (32 streams:
  256 MiB), each stream gets one producer segment under the fair-share
  budget (16 per node), and a segment decodes a 128 MiB part in 47 ms under
  load (2.7 GB/s), so a 512 MiB stream lasts about 0.4 s of which the
  metadata, reader setup and segment ramp are a visible share (slowest
  stream 0.42 s against a 0.48 s wall). Per-stream throughput in these
  rows is therefore the per-segment rate minus setup, not a fabric cap.
- A larger budget per node does not help: budget 64 (up to four segments
  per stream at 16 streams) gave 11.7 GB/s at 16 and 23.7 at 32 host
  streams, budget 128 19.6 and 17.1, single GETs 7.6-11.6, all at or below
  the budget-16 rows. More producers per node share the same node-wide
  ceiling (about 20 GB/s served per node at 16-32 streams, CPU 2.0
  s/GiB), so the next lever is that shared cost on the serving node
  (per-part decode latency, the blocking-pool round trips per stripe), not
  more segments.
- Not NUMA placement: both servers pinned with `taskset` to the NIC's
  socket (CPUs 64-127,192-255; the registered pools are first-touched
  there) gave 17.6 / 17.3 GB/s at 16 / 32 host streams, pinned to the far
  socket 20.8 / 17.2, unpinned 18.2 / 19.0 (two rounds each, same build).
  The spread between runs (plus or minus 15 percent) is larger than any
  socket effect, so cross-socket traffic is not the node cap. The bond
  does not spread RDMA either: a queue pair lives on one port's device,
  802.3ad hashing applies to TCP only, and RoCE LAG needs both ports on one
  card, so all RDMA of a node uses one 400G port (`mlx5_11`), which the
  raw test shows taking 45 GB/s.
- Not the channel count: `RUSTFS_CUOBJECT_NUM_DCIS` 32 gave 9.2 GB/s single
  and 17.3 / 17.3 at 16 / 32 host streams, 64 gave 9.8 and 15.8 / 14.1, 128
  (default) 14-15 and 21 / 25, and 256 collapsed to 0.9 single and 3.5 at
  both stream counts (the run after it, back on 128, collapsed the same way
  once and then recovered; cause not found). The default stays 128.
- Not the server's CPU: during 23 GB/s runs b300-139 peaks at 17 busy cores
  of 256 (10 user, 7 system).
- Stream length matters more than count: one 8 GiB object as 4 x 2 GiB
  streams gives 3.4-4.1 GB/s per stream (13.5-16.3 total), 8 x 1 GiB 1.5
  (12.0), 16 x 512 MiB 1.2-1.3 (19.3-20.3), 32 x 256 MiB 0.6-0.7 (17.9-22.6).
  Note that every `benchk` row on the staging cluster from the two-server
  rows onward ran without the shard arena (`RUSTFS_CUOBJECT_SHARD_ARENA_BYTES`
  is not in the base environment), that is, on the staging-copy emit path;
  the arena rows are measured separately below.

Byte-exact on all verify checks. The blocking control run on the same build
(`RUSTFS_CUOBJECT_ASYNC_TRANSFERS=false`, right after the asynchronous rows)
collapsed to 3.6 GB/s at 16 and 32 host streams, the same failure shape the
first fan-out pass showed, so the blocking path is also the fragile one
under load. The KV chunk rows did not change (cold and warm p50 both 31 ms
for 48 MiB, 1.6 GB/s): the warm GET is not served from the object data
cache at all on this cluster (no cache-path stages appear), which is the
next item for the KV use case.

### Listener drain fix: the collapses were stale accepts (2026-11-04)

Several runs in this record collapsed to 0.9 GB/s single-stream and 3.5
GB/s at 16-32 streams (the first fan-out pass, the blocking control, the
256-channel run and the runs after it). On the peer the RDMA listener was
failing every accept with `EINVAL`, so the serving node fell back to TCP for
internode shard reads for 10 s at a time. A syscall trace on the peer put
the failure in queue-pair setup for a connection request the dialer had
already abandoned: the listener's event channel is edge-triggered, the loop
treated an event that was not a connection request (a stale event for a
child id) as "nothing pending" and cleared readiness, and a failed accept
cleared it as well, so from then on every request was handled one arrival
late, after the dialer's 2 s timeout. With a 15 s dial timeout the failures
disappeared, which confirmed the timing. The loop now drains every queued
event (stale events and failed accepts included) before clearing readiness.
Two restarts at the default timeout: no accept failures on the peer, no TCP
fallback, single GETs 6.4-12.6 GB/s. The serving node still logs eight
failed accepts at startup, from the peer's warm-up dials arriving while the
8 GiB pool registration runs before the accept loop starts; the dialer
retries and nothing falls back.

Per-write timing of the cuObject path (`cuobject_emit` stages: `write_total`
and latency buckets per RDMA write, `write_bytes` as the average write size)
shows the writes themselves are not slow under load: on the staging-copy
path at 32 host streams the 16 MiB writes complete in 2.3 ms on average,
471 of 768 under 1 ms, 53 between 5 and 20 ms, none above 20 ms.

Rows after the fix (fan-out 8, asynchronous transfers, two servers):

| Emit path | Single RDMA GET | 16 x 2 GiB GPU | 16 x 8 GiB host | 32 x 8 GiB host | CPU s/GiB |
| --- | --- | --- | --- | --- | --- |
| staging copy (arena off, the default) | 10.9-12.8 GB/s | 12.9 | 19.1 | 20.4 | 2.04 |
| shard arena 8 GiB, asynchronous | 1.7-5.8 | 7.8 | 20.0 | 20.3 | 3.11 |
| shard arena 8 GiB, blocking transfers | 3.9-6.7 | 6.2 | 12.6 | 17.5 | 2.55 |

The zero-copy (arena) path has regressed in this tree, with either transfer
mode: single GETs run at 0.3-6.6 GB/s with multi-second stalls ("reader
setup is slow", "Disk operation is still pending"), the average write is
16.6 MB rather than the 24 MiB stripe batch (most emits are falling back to
the staging copy), and the per-part decode is 58 ms against 44-47 on the
staging path, while the writes themselves complete in 2 ms. The earlier
arena rows (13.9 GB/s single) predate the pool pre-warm, the read-ahead and
the copy-beside-the-hash change; which of them broke the arena path is not
yet isolated. The arena stays off by default and is the first item to fix.

## NIXL/UCX on the GPU-side InfiniBand rails (2026-11-04)

Caveat for every row in this section and the RoCE section after it: the
server build they were taken with reported each transfer complete at post
time (see "Transfer completion fix" below), so the reply preceded the last
in-flight chunks and the "byte-exact" checks passed only because they ran
three rounds of the same object. The rates are at most a few hundred MiB
per GET optimistic; the corrected single-round numbers are in that section.

The cuObject path (NVIDIA `libcuobjserver` on the server, `libcuobjclient`
or the NIXL `OBJ` plugin on the client) selects its device by IP, so it is
confined to the CPU-side RoCE NIC, and every write into a GPU crosses the
CPU root complex (13-14 GB/s per client, 5-6 GB/s for four GPUs at once).
The GPU-side InfiniBand NICs (eight XDR ports per node, one on each GPU's
PCIe switch) carry no IP: IPoIB cannot come up on this fabric because the
subnet manager refuses the broadcast multicast join (`status -22`) and the
subnet administrator does not answer queries at all, which also rules out
rdma_cm on those ports. NCCL and UCX work there because they exchange
LIDs, GIDs and queue-pair numbers out of band.

NIXL 1.5.2 (`nixl-cu13` wheel, Python module `nixl_cu13`, UCX backend)
installed on the client pod and both server pods; raw agent-to-agent
WRITE transfers of 1 GiB, 32 per pair, server host memory to the client,
`UCX_NET_DEVICES` pinned to the rail of the target GPU (mlx5_9/10/12/13
for GPU 4-7), start barrier shared through `/shared`:

| Transfer | per pair | aggregate |
| --- | --- | --- |
| one server -> GPU 4 (its rail) | 58.8 GB/s | 58.8 GB/s |
| one server -> GPU 4, 5, 6, 7 each on its rail | 57.8-58.8 | **228 GB/s** |
| two servers -> GPU 4-7 (two rails each) | 57.8-58.8 | 96 (start skew between the two servers; per-pair rate is the full rail) |
| two servers -> client host memory, 8 streams over 4 rails | 43.6-55.6 | 165 |

End to end through RustFS (branch `rainj-me/nixl-transport`, server built
with `--features rdma,nixl`, agent on rails mlx5_9/10/12/13, staging-copy
emit path, fan-out 8, `nixl-get` from the `rustfs-bench` repository):

| GET | rate | verified |
| --- | --- | --- |
| 2 GiB into host memory (rail mlx5_9) | 13.6 GB/s | byte-exact |
| 2 GiB into GPU 4 (rail mlx5_9) | 14.2 GB/s | byte-exact |
| 4 x 2 GiB into GPU 4-7 at once, one server | 12.5-15.9 GB/s per process alone, 3-7 while all four overlap | |
| 4 x 8 GiB into GPU 4-7 at once, one server | 7.0-8.2 GB/s per process with all four active | |
| 4 x 3 x 8 GiB into GPU 4-7, common start (`--start-at`), one server | **17.6 GB/s aggregate** over 5.85 s wall; per process 8.2-15.5 | |

So the GPU target no longer sits at the client's root-complex ceiling and
four GPUs at once get three times what the cuObject path gave them (5-6
GB/s); the per-process rates of unsynchronized runs must not be summed
(the processes overlap only partly), the synchronized aggregate of one
server is 17.6 GB/s, the same band as its 16-32 host-stream rows. What
bounds them now is the serving node's pipeline (the same
staging-copy emit and per-part decode measured above), not the client or
the fabric. The UCX inside the wheel reports no accelerated mlx5 transport
unless `UCX_MODULE_DIR` points at the wheel's `ucx/` modules; with the
modules a concurrent four-GPU run once stalled on one transfer (not
reproduced since), so a transfer timeout now fails such a GET instead of
hanging it.

The same transport on the CPU-side RoCE ports (RoCE re-enabled on
53:00.0, agent on `mlx5_6,mlx5_11`, client naming both devices,
`UCX_MAX_RMA_RAILS=2` on both ends):

| GET over RoCE | rate | ports used (server `port_xmit_data`) |
| --- | --- | --- |
| 2 GiB into GPU 4, one port (mlx5_11) | 16.0 GB/s, byte-exact | one |
| 2 GiB into host memory, two ports | 14.2-15.2 GB/s, byte-exact | |
| 2 GiB into GPU 4, two ports | 15.2-15.8 GB/s, byte-exact | |
| 8 GiB into host memory, two ports | 15.6 GB/s | **4160 MiB on mlx5_6 + 4167 MiB on mlx5_11** |
| 8 GiB into GPU 4, two ports | 16.8 GB/s | 0 on mlx5_6, 8329 MiB on mlx5_11 |
| 4 x 8 GiB into GPU 4-7, one or two ports | 7-17 GB/s per process | |

UCX pairs the two RoCE ports correctly (port A with the peer's port A) and
stripes large puts into host memory 50/50 across them once
`UCX_MAX_RMA_RAILS` is 2 (its default is 1; the protocol table shows
"ucp_put*(multi) ... 50% on mlx5_11 and 50% on mlx5_6"), but puts into CUDA
memory stay on one lane in this UCX, so GPU targets do not gain a second
port here. Through the root complex, the NIXL path into one GPU is 16 GB/s
where cuObject sat at 13-14, and four GPUs over one port reach the same
26-30 GB/s as over the GPU-side rails: the serving pipeline is the limit
in every configuration. The server's UCX runs the `rc_verbs` transport
(NIXL warns that accelerated mlx5 support is missing) while the client,
from the same libraries, runs `rc_mlx5`; not yet understood, and worth
fixing before the pipeline gets faster.

One rail into one GPU delivers four times the cuObject root-complex path,
four GPUs scale linearly (228 GB/s out of one server's host memory), and no
subnet administrator, IPoIB address or RoCE setting is involved. UCX can
use the CPU-side RoCE ports the same way (devices named, GIDs exchanged
out of band), which would also make both bonded ports usable as two rails
once RoCE is re-enabled on 53:00.0; not yet measured.

## Transfer completion fix: the NIXL post result was read inverted (2026-11-05)

Single-round verification of NIXL GETs into a fresh buffer failed on every
transport configuration (verbs and mlx5, bundled and system rdma-core, one
or two paths, agent synchronization off, reader-writer or strict), while
three rounds of the same object passed. The `nixl-get` provenance
diagnostic showed each wrong 1 MiB block holding the object's data from 16,
32 or 48 MiB later, one to three staging chunks ahead, and with the client
registered on two RoCE ports against a one-port server half the buffer
stayed zero. A RustFS-free harness (`nixl-reuse` in `rustfs-bench`: a writer
fills a remote 2 GiB buffer from eight 16 MiB chunks, refilling each as soon
as NIXL reports its transfer complete; the target checks every 8-byte word
against its offset) reproduced it and reported 94-127 GB/s on a 400 Gb/s
port with every transfer "completed at post". The cause is the `nixl-sys`
binding's `post_xfer_req`, which returns `true` while the transfer is still
in progress; the server and the harness read it as "completed" and never
polled. With that one boolean fixed (and the harness target given NIXL's
progress thread, which a passive client needs for the server's endpoint to
finish its UCX handshake):

| after the fix | result |
| --- | --- |
| `nixl-reuse`, 1 thread, 8 chunks, one RoCE port | 20.1 GB/s, every word correct, completion latency 0.35-15 ms |
| `nixl-reuse`, 8 threads, 8 or 64 chunks | 30.7-35.4 GB/s, every word correct |
| GET 2 GiB into host memory, one port, single round | 11.9-13.6 GB/s, byte-exact (x3) |
| GET 2 GiB into GPU 4, one port, single round | 9.9-11.9 GB/s, byte-exact (x3) |
| GET 2 GiB into host memory or GPU 4, server and client on both ports, single round | 10.7-12.0 GB/s, byte-exact (x4) |
| GET 8 GiB into host memory / GPU 4, both ports, 3 rounds | 16.2 / 17.3 GB/s |
| GET 8 GiB into GPU 4 over rail mlx5_9 | 17.3 GB/s |
| 4 x 8 GiB into GPU 4-7 over the four rails, synchronized | 19.0 GB/s aggregate |
| 4 x 8 GiB into GPU 4-7 over the two RoCE ports, synchronized | 9.2 GB/s aggregate (one process at 3.4) |

So the pipeline numbers stand within their error (one server still delivers
17-19 GB/s), but every earlier NIXL row was taken with replies that preceded
the data. Two constraints surfaced on the way: with `UCX_MAX_RMA_RAILS=2`
the server stripes each write across both ports and a client registered on
one port fails the transfer (the GET is now declined with reply 501 instead
of landing half a buffer), and the client's agent must run NIXL's progress
thread. The transport-level theories tested and refuted on the way (the two
libibverbs copies and the rdma-core 48/50 ABI skew, UCX multi-path, TX CQ
moderation, the zero-copy threshold, protocol v1, agent synchronization,
an endpoint warm-up read) are recorded in the harness scripts, not here.

## Where the corrected pipeline spends its time (2026-11-05)

With real completions, one server on one RoCE port (`mlx5_11`) serves 2 GiB
GETs at 12-15 GB/s and 8-16 concurrent 8 GiB host-memory streams at 17-20
GB/s aggregate (total bytes over the window from the first start to the
last end); two ports give 19-23 GB/s. The standalone `nixl-reuse` writer
moves 35 GB/s over the same port from one process, so the serving pipeline
is the limit, as before. Environment sweeps on the one-port, 8-stream row,
each a restart with the previous winners kept:

| change | 8 x 2 GiB aggregate | effect on the stages |
| --- | --- | --- |
| baseline (1 GiB staging pool) | 15.8 GB/s | `duplex_backpressure` 38.6 ms per shard write: segments wait for staging chunks |
| `RUSTFS_NIXL_POOL_BYTES` 8 GiB | 20.8 GB/s | backpressure 0.95 ms |
| + `RUSTFS_OBJECT_MMAP_READ_ENABLE=false` | 17-19 GB/s | `reader_setup` 56 -> 24 ms: the local shard's 22 ms mmap copy at open is gone |
| + idle pool 64 min / 256 max per peer | 19 GB/s | `reader_setup` 10.6 ms, `reader_task_file_open` 5.6 ms, dials 1313 -> 192 per 15 min |
| `RUSTFS_CUOBJECT_INFLIGHT_WRITES=8` | 18.0 GB/s | none |
| `RUSTFS_NIXL_NUM_WORKERS=4` | 19.5 GB/s | none |
| `RUSTFS_NIXL_CHUNK_BYTES` 32 MiB | 20.5 GB/s | none |
| fan-out 16, budget 64 | 13.1-14.5 GB/s | every stage slower (stripe read 24 ms, emit 13 ms): more segments contend |
| stripe prefetch 2, with or without decode overlap | 18-19 GB/s | none |
| `RUSTFS_RUNTIME_WORKER_THREADS` 32 / 64 / 128 | 18.5-19.2 GB/s | none (the runtime already had 127 workers) |
| shard arena 4 GiB (zero-copy emit) | 15.7 GB/s | byte-exact, no stalls; emit 7.2 ms, no faster than the staging copy |

After the pool, mmap and idle-pool changes a part costs `reader_setup` 9-15 ms
plus `decode` 52-74 ms (per 24 MiB stripe: `stripe_read` 8-10 ms with the
slowest shard at 9.6 ms, `emit` 5.5-6.2 ms, strictly in sequence), so a
segment moves 1.5-2.5 GB/s and the fan-out budget of 16 segments per node
bounds the node near 20 GB/s; raising the budget makes every segment
slower instead. The server burns 63 cores for 20 GB/s (237 runtime and
blocking threads, 19 of them at a full core), and the NIXL binding takes a
process-wide write lock on every call while every in-flight write had a
task spinning on its status, which is the contention signature observed.
The completion thread (one thread posts and polls every transfer, tasks
wait on a oneshot) is the next change measured below.

Client-side notes from the same runs: sixteen `nixl-get` processes
starting together segfault in UCX's shared-memory transport setup
(`uct_mm_iface_t_new` -> `ucs_mpool_grow`) because the pod's `/dev/shm` is
64 MiB; `UCX_TLS=^posix,sysv,cma,xpmem,knem` on the client avoids the
shared-memory transports and all sixteen complete (positive `UCX_TLS` lists
make the NIXL backend fail to start). The aggregate over several
unsynchronized 2 GiB streams is dominated by per-process start-up (HEAD,
agent, metadata, first-request handshake), so the multi-stream rows use 8
GiB objects.

### Page cache, mmap copy and the positional internode serve path (2026-11-05)

Every served shard read evicted its pages (`RUSTFS_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE`
defaults to true), so each GET went back to NVMe; the drives read 7-8 GB/s
each with direct I/O but a buffered 1 MiB fill cost 2 ms and every attempt
to put more reads in flight (deeper internode windows, larger chunks, a
bigger fan-out budget) queued at the drives and made the node slower. With
reclaim off the peer keeps 320 GiB of page cache and a cached 1 MiB read
costs 0.17 ms, yet the served fills still took 1.7 ms: the stream server
filled its 1 MiB slots one after another through an `AsyncRead` over
`tokio::fs::File` (a blocking-pool round trip and two copies per read). The
server now opens a local shard file for positional reads and fills every
free slot of the peer's window with its own blocking `pread` straight into
the registered slot, announcing the chunks in offset order
(`RdmaRequestHandler::read_positional`, `push_positional`). One port,
`mlx5_11`, 8 GiB host-memory streams, two rounds, aggregate over the window:

| server | 1 stream | 8 streams | 16 streams | 4 GPU streams |
| --- | --- | --- | --- | --- |
| reclaim on (every read from NVMe) | 13.9 GB/s | 16.7-19.2 GB/s | 19.8-23.9 GB/s | 11.7 GB/s |
| reclaim off | 15.2 GB/s | 20.9 GB/s | 24.7 GB/s | 14.3 GB/s |
| reclaim off, mmap copy on (cached, 14 ms per part) | 13.6 GB/s | 21.8-22.7 GB/s | 25.9-26.0 GB/s | |
| + positional serve path, 4 slots | 17.0 GB/s | 23.8 GB/s | 27.7 GB/s | |
| + positional serve path, 8 slots | 17.7 GB/s | 24.4 GB/s | 26.5 GB/s | 16.6 GB/s |
| + positional serve path, 16 slots | 16.3 GB/s | 14.4 GB/s | 18.8 GB/s | 10.8 GB/s |

The served fill is 1.3 ms on average (now overlapped four or eight deep
per stream instead of serial; sixteen deep floods the blocking pool and
loses a third of the aggregate), `stripe_read` is 6.9-7.9 ms per 24 MiB
stripe and the slowest shard 4.3-5.2 ms, so the per-segment cost is now
`stripe_read` plus the 6 ms staging copy, and the completion thread holds
the NIXL write at 2.7 ms per 10 MB.
### Zero-copy emit restored: the decode converted every shard to the heap (2026-11-05)

The arena emit never fired because `Erasure::decode_data` ran the codec's
`reconstruct_data` on every stripe even with all data shards present, and
that path moves every shard into a `Vec` (a copy out of the arena block)
and hands back heap buffers; only the bridge engines had the data-complete
early return. With the early return in both `reconstruct_data`
implementations the counters show every shard buffer still arena-backed
after the bitrot read, every shard offered to the emitter taken, and every
write leaving as `write_zero_copy`. One port, `mlx5_11`, reclaim off, mmap
copy on, positional serve path, shard arena 8 GiB, 8 GiB objects:

| | before (staging copy) | zero-copy arena emit |
| --- | --- | --- |
| single GET into host memory | 17.0-17.7 GB/s | 27.8-30.2 GB/s |
| 8 streams, aggregate | 23.8-24.4 GB/s | 35.5-36.0 GB/s |
| 16 streams, aggregate | 26.5-27.7 GB/s | 35.6 GB/s |
| 4 GPU streams, aggregate | 16.6 GB/s | 25.2 GB/s |
| emit per 24 MiB stripe | 6.1-6.7 ms | 0.96 ms |
| decode per 128 MiB part | 50-55 ms | 30.8 ms |
| NIXL write (now 7.3 MB scatter-gather average) | 2.7 ms | 1.2-3.3 ms |

The first arena run also hit the 60 s NIXL transfer timeout on about 26
scatter-gather writes (failed GETs and a 100 ms tail on the write average)
during its 16-stream and four-GPU rows; three following runs (sequential
GETs plus 8 streams, with one UCX path, with one write in flight) had no
timeout, so it is load-dependent and still open. A timed-out transfer used
to let the request task drop its arena blocks while the NIC could still
read them; the buffers now travel with the transfer on the completion
thread and stay there until the library completes it.

Full matrix with that build (8 GiB objects, two rounds, aggregate over the
window, no transfer timeout in either variant):

| server ports | 1 stream | 4 | 8 | 16 | 4 GPU | 8 GPU |
| --- | --- | --- | --- | --- | --- | --- |
| one (`mlx5_11`) | 28.1 GB/s (31.7 last round) | 28.2 | 37.6 | 39.4 | 25.5 | 35.2 |
| two (`mlx5_6,mlx5_11`, `UCX_MAX_RMA_RAILS=2`, clients on both) | 30.7 | 28.3 | 36.7 | 36.2 | 25.0 | 35.0 |

One port is at the 40 GB/s goal with 16 streams; the second port adds
nothing because the serving pipeline, not the NIC, is the limit: with the
emit at 0.8 ms per stripe, `reader_setup` is back to 30-33 ms per part
(the part's readers open while the previous part is already done) and
`stripe_read` 5.8-6.0 ms, and 946 of 30780 writes still take over 50 ms.
Pipelining the next part's reader setup inside a segment and the fan-out
budget are the next levers toward 80 GB/s.

### Two front-end servers, both RoCE ports (2026-11-05)

Clients on b300-110 split their streams round-robin over b300-139 and
b300-141, every server and client on `mlx5_6,mlx5_11` with
`UCX_MAX_RMA_RAILS=2`, shard arena on, page-cache reclaim off, 8 GiB
objects, two rounds, aggregate over the window (two repetitions after a
restart, plus one earlier run on servers that had been up for a day):

| streams | one server (139) | two servers (139 + 141) |
| --- | --- | --- |
| 8 host | 38.6 / 39.7 GB/s (22.2 on the long-running servers) | 25.2 / 35.2 / 43.2 GB/s |
| 16 host | 39.7 / 44.4 GB/s (21.9) | 34.8 / 50.0 / 49.2 GB/s |
| 24 host | | 31.1 / 28.6 GB/s |
| 8 GPU (4-7) | | 32.5 / 31.2 / 39.6 GB/s |

Every single-round check through either server is byte-exact and no
transfer timed out. Two front-ends peak at 50 GB/s but more often land
below one server, and get worse with more streams. 32 clients exceed the
client pod's 256 GiB memory limit (32 x 8 GiB buffers) and are killed.

The client and the fabric are not the limit. `nixl-reuse` writers into the
client's two ports move 94.0 GB/s from 139 alone, 93.7 GB/s from 141 alone
and 48.0 + 45.8 = 93.8 GB/s from both at once (ECN marks and CNPs appear,
throughput holds).

The likely cause is rail imbalance on the servers. UCX stripes each client
write 50/50 over the two ports and the write completes when its slower half
lands, but the internode shard traffic (rdma_cm on the bond IP) uses
`mlx5_11` only. With two front-ends every node sends its peer half the data
shards over `mlx5_11` on top of its share of client writes: during a 16-stream
run 139 sent 118.7 GB on `mlx5_11` and 69.8 GB on `mlx5_6`, 141 sent 168.2 GB
and 69.8 GB. With one front-end, 139's `mlx5_11` carries only client writes
(the internode traffic arrives on it) and the two halves stay even. The
two-server runs also show 12-20% of NIXL writes over 50 ms against 3% with
one server. Next: put the internode transport on both ports or on the
port the client stripe uses less, or weight the client stripe, and rerun
this table.

## Four servers on the GPU-side InfiniBand rails (2026-11-05)

Layout: RustFS servers on b300-110, b300-121, b300-139 and b300-141, four
NVMe drives each (16 drives, one 12+4 set), `RUSTFS_INTERNODE_DATA_TRANSPORT=nixl`
so internode shard reads also move over NIXL, and every node's agent on the
same four XDR rails (`mlx5_0`, `mlx5_7`, `mlx5_10`, `mlx5_13`). The client
runs on b300-110 (also a server): host-memory clients register the four
rails, GPU clients GPUs 0, 2, 5 and 7, each on its own rail. Page-cache
reclaim off, mmap read on, shard arena and staging pool 8 GiB each.

Rail health decided the layout. `mlx5_5` is down on 139, `mlx5_12` went down
on 110 during the runs, 110's `mlx5_8` dropped its link 4 times in 4 idle
minutes (68 since boot) and `mlx5_9` 22 times, and 139's `mlx5_12` 148 times;
121 and 141 are clean. Lanes on those rails failed UCX endpoints, NIXL then
dropped the whole peer ("remote agent ... was disconnected after transfer
request creation") and clients saw "Destination is unreachable". The four
rails above are healthy on every node and saw no link drop during the run.
The startup device check (`RUSTFS_NIXL_DEVICE_CHECK`) now leaves such
devices out on its own.

Correctness: a 2 GiB NIXL PUT and a NIXL GET through each of the four
servers, from and into host memory and GPU 0, all byte-exact against a
plain HTTP GET (16 checks). No transfer timeout, refused internode read or
dropped peer in the whole run.

| GetObject, 8 GiB objects, aggregate over the window | GB/s |
| --- | --- |
| 1 host stream (four rails) | 29.2 |
| 4 / 8 / 16 host streams | 77.7 / 77.4 / 77.1 |
| 24 host streams | 87.3 |
| 8 / 16 host streams, one rail each | 80.2 / 78.3 |
| 4 GPU streams (GPUs 0, 2, 5, 7) | 97.9 |
| 8 GPU streams (two per GPU) | 111.5 |

| PutObject, 4 GiB objects, 96 MiB parts, 8 in flight per client | GB/s |
| --- | --- |
| 1 host stream | 2.1 |
| 4 / 8 / 16 host streams | 6.5 / 8.5 / 9.0 |
| 4 GPU streams | 6.6 |

GETs reach the 80 GB/s target with host memory and pass 110 GB/s into GPUs.
PUTs move their parts client-to-server over NIXL (the client node's rails
carry the data as the servers pull it), but the erasure-coded shard writes
between servers still use TCP/HTTP over the Ethernet bond, which caps the
cluster near 9 GB/s; NIXL internode writes are the next step for PUT.
Per-part `reader_setup` is 26-39 ms and `stripe_read` 12-25 ms per 48 MiB
stripe (three quarters of every stripe's data shards are remote), and about
a third of the client writes still take over 50 ms under 24 streams.

Two client nodes (b300-110 and b300-121, GPUs 0, 2, 5, 7 each, synchronized
start) show that this is the servers' limit, not the client's:

| 8 GiB GETs, two rounds | aggregate |
| --- | --- |
| 8 GPU streams from 110 alone | 109.8 / 114.1 GB/s |
| 8 GPU streams from 121 alone | 112.0 / 112.4 GB/s |
| 16 GPU streams, 8 from each node | 95.1 / 97.1 GB/s (about 48 GB/s per node) |
| 32 host streams, 16 from each node | 96.8 / 91.2 GB/s |

For a 1.5 TB model this means about 14 s of transfer for one engine node
from a warm page cache (2 TB of shards at 12+4, 500 GB per server, fits in
the servers' memory with reclaim off), and loads that run together share
the roughly 100-110 GB/s (two engines about 30 s each, four about 1 min).
A cold first load reads NVMe and was not measured on this layout.

### NIXL shard writes and cold loads (2026-11-05)

Internode shard writes now move over NIXL too: the writing node stages each
remote shard file in registered buffers and the disk's node pulls them with
a NIXL READ when the file is complete (`PUT /rustfs/rpc/write_file_nixl`),
writing through the same disk writer as the TCP path. Every PUT through the
four servers, from host memory and GPU 0, reads back byte-exact.

| PutObject, 4 GiB objects, 96 MiB parts | TCP shard writes | NIXL shard writes |
| --- | --- | --- |
| 1 stream | 2.1 GB/s | 2.3 GB/s |
| 4 streams | 6.5 GB/s | 8.2 GB/s |
| 8 streams | 8.5 GB/s | 8.6 GB/s |
| 16 streams | 9.0 GB/s | 6.5-9.5 GB/s |

The transport was not PUT's limit. During a 16-stream PUT each server keeps
40-45 cores of its runtime threads busy for about 2.4 GB/s of ingest, with no
thread waiting on the drives and the fsync pool idle; a 96 MiB part takes
about 400 ms with eight parts in flight per client. The part's MD5 (ETag)
accounts for about three of those cores; the rest (erasure encoding, bitrot
hashing, buffer copies or spinning) needs a profile with symbols, which the
`fast` build strips (`--profile profiling` keeps them).

Cold loads: a 128 GiB dataset (16 objects of 8 GiB written over NIXL) read
after evicting only RustFS's shard files from the page cache
(`dd iflag=nocache`, so the other workloads' cache stays), with the servers'
NVMe read counters confirming the reads came from the drives:

| reads of the dataset | warm | cold | NVMe read during the cold row |
| --- | --- | --- | --- |
| 16 host streams | 50.3 GB/s | 36.6 GB/s | 137.5 GB |
| 8 GPU streams (GPUs 0, 2, 5, 7) | 122.5 GB/s | 43.3 GB/s | 68.8 GB |
| 1 host stream (8 GiB) | | 26.4 GB/s | 8.6 GB |

A cold load runs at roughly a third of the warm rate into GPUs and three
quarters into host memory: about 35-40 s for a 1.5 TB model against about
14 s warm. The busiest nodes read about 12 GB/s from four drives (3 GB/s per
drive, well under their sequential rate); b300-121's two drives behind its
x2 uplink share 7.9 GB/s.

### Where a large PUT waits: huge-page compaction and 16 runtime workers (2026-11-06)

PutObject of 8 GiB objects in 96 MiB parts, eight parts in flight per stream, streams spread over the four IB-rail servers (12+4, 16 drives), with NIXL shard writes. Per-stage timings come from the PUT stage summary (`RUSTFS_OBS_PUT_STAGE_METRICS_ENABLED`, see the NIXL operations guide).

| Change | 16 streams | 1 stream |
| --- | --- | --- |
| before | 8.5 GB/s | 2.0 GB/s |
| `MIMALLOC_ALLOW_THP=0` | 12.5-14.1 GB/s | 2.3 GB/s |
| + 128 runtime workers (`RUSTFS_RUNTIME_WORKER_THREADS`) | 22-26 GB/s | 2.2 GB/s |
| same, PUT body read-ahead 1 instead of 4 | 22.0 GB/s; 28 streams 22.5 (27.0 with 4) | |

The first profile (system-wide `perf`) showed about a third of each server's CPU in the kernel's direct compaction: mimalloc asked for transparent huge pages for the encoder's per-stripe buffers and the kernel, in `madvise` mode, compacted memory synchronously to serve them. The `MIMALLOC_ALLOW_THP=0` environment variable removes it.

The second limit was not a resource. During a 16-stream PUT each server used 4-5 cores, its rails carried about 3 GB/s each, raw `ib_read_bw` reached 46-59 GB/s per rail, and the drives (5 GB/s each with direct writes) absorbed the 4.3 GB/s per node easily. NIXL itself was quick: at steady state a 16 MiB read completed 0.2-1.9 ms after it was posted, with about two transfers in flight per server. Yet the tasks waiting on those transfers resumed 9-12 ms later, and each part's metadata round trips and commit (`rename_part`) grew from tens to hundreds of milliseconds under load. The runtime had 16 worker threads (the default cap), and each part runs its MD5 and erasure encoding inline on them in bursts of 20-70 ms; a woken task waited behind those bursts. With 128 workers on the 256-core servers:

| Per 96 MiB part, 16 streams | 16 workers | 128 workers |
| --- | --- | --- |
| part total (`multipart_store_write`) | 923 ms | 499 ms |
| encode (MD5, erasure, shard writes) | 411 ms | 389 ms |
| commit (`rename_part`) | 405 ms | 95 ms |
| metadata reads before the write | 57 + 46 ms | 9 + 5 ms |
| wait per 16 MiB body chunk | 9.1 ms | 2.3 ms |

The part is now bound by its encode, where the serial MD5 (ETag) of the part dominates the producer side. Raising the large-PUT admission limit beyond its 32 parts per server did not add throughput, and at 128 it ended in 503s. Every PUT through the four servers at 128 workers, from host memory and GPU 0, reads back byte-exact.

### 4k-token KV objects: offloaded MD5, no fill-on-put, allocator purge (2026-11-06)

LLM KV-cache objects of 4,096 tokens (Kimi K3 FP8 MLA: 24 layers x 576 B per token, 56.6 MB), each a single PutObject over NIXL, on two servers (b300-139 and b300-141, eight drives, 6+2) with the client on b300-110 and 128 runtime workers. Durability `none` changed nothing at this size (PUT 9.24 against 9.46 GB/s with four clients), so the work went into the per-byte path.

The PUT stage summary split a single 57 MB PUT (205 ms) into about 116 ms of encoder waiting on its producer (the body read and the inline ETag MD5), 32 ms of fill-on-put (copying the body into the object data cache), about 28 ms of shard writes and 6 ms of commit.

| Change | 1 stream, 1 in flight | 1 client, 16 in flight | 4 clients x 16 in flight |
| --- | --- | --- | --- |
| before | 0.27 GB/s (205 ms) | 3.4 GB/s | 9.2-9.5 GB/s (64 objects each) |
| ETag MD5 on a blocking-pool thread | 0.38 GB/s (146 ms) | 4.1 GB/s | 11.0 GB/s (64 objects each) |
| + fill-on-put off (zero-copy RDMA body) | 0.48 GB/s (113 ms) | 4.5-6.2 GB/s | 12.2-15.6 GB/s (512 objects each) |
| + `MIMALLOC_PURGE_DELAY=-1` | 0.48-0.50 GB/s (110 ms) | 5.7-6.7 GB/s | 15.1-19.4 GB/s (512 objects each) |

Short runs of 64 objects per client end within about a second and understate the aggregate; the longer runs varied by about 20% between repeats. A system-wide profile during the 4-client PUT showed the remaining MD5 (17% of the node's CPU) and about 20% in the kernel's page allocator: faulting in and zeroing fresh buffers, TLB-shootdown IPIs and compaction under the zone lock, which the allocator's purge delay mostly removes.

Raising concurrency did not add throughput: 4 clients x 32 in flight with a large-PUT admission limit of 64 reached 18.9 GB/s at a p50 of 318 ms, against about 18 GB/s at 16 in flight. With the default limit (32 per server) the same load is refused with `SlowDown` (503, foreground write concurrency limit reached), which clients should retry with backoff. Every PUT verified byte-exact with its ETag equal to the MD5 of the object.

## Results by phase

| Phase | Build | Single RDMA GET into GPU | Single HTTP GET | 16 ranged RDMA GETs in flight | 32 in flight | CPU s/GiB |
| --- | --- | --- | --- | --- | --- | --- |
| 0 baseline | 039443eef | 2.24 GB/s | 2.30 GB/s | 9.14 GB/s (8 GiB, host) | 9.22 GB/s | 1.02 |
| 0 all switches | 039443eef | 3.11 GB/s | 2.65 GB/s | 8.97 GB/s | 8.76 GB/s | 0.77 |
| 1 loader, 16 in flight | 039443eef | (model load 7.79 GB/s) | | | | |
| 2 4 MiB shards | ec92a122b | 2.57 GB/s | 2.01 GB/s | 11.21 GB/s | 7.79 GB/s | 0.94 |
| 3 pipelined RDMA writes | 914bec288 | 3.17 GB/s | 2.04 GB/s | 9.00 GB/s | 8.42 GB/s | 0.84 |
| 3b buffer pool fix (prefetch 2) | fb18e317f | 2.74-2.98 GB/s | 1.55-2.03 GB/s | | | 0.85-1.04 |
| 3c off-thread bitrot verification | f570dfcd4 | 4.65 GB/s | 2.16 GB/s | 9.15 GB/s | 8.25 GB/s | 0.97 |
| 3d 64 MiB pipe + prefetch 2 | (phase 3d) | **5.48 GB/s** | 2.75 GB/s | 9.28 GB/s | | 0.96 |
| 4 object data cache (48 MiB KV chunk) | f570dfcd4 | warm hit 11.8 ms p50 (4.2 GB/s) vs 21.4 ms off | | | | |
| 4 fill on PUT | 53c178def | first GET after PUT 11.8 ms p50 (was 36.4) | | | | |
| 4 cuObject PUT | (put) | RDMA PUT 0.39 GB/s single, 1.35 GB/s x16 | | | | |
| 4c one node frozen (moderate timeouts) | 53c178def | no failures; worst stall 10.6 s (25 s with defaults) | | 2 clients x 32: 17 GB/s | | |
| staging port, GET sink + cold-fill fix (16 drives, slow port) | (this tree) | 4.36 GB/s | 0.82 GB/s | 14.25 GB/s (8 GiB, host) | 12.79 GB/s | 2.09 |
| 5 cuObject GET fan-out 8 (16 drives, slow port) | (this tree) | **8.79 GB/s** | 0.81 GB/s | 3.69 GB/s (no budget) | 3.46 GB/s | 2.61 |
| 5 fan-out 8 + fair-share budget (8 drives 6+2, x16 ports) | (this tree) | 7.18-8.07 GB/s | 0.82 GB/s | 20.86-22.28 GB/s | **26-27 GB/s** | 2.9 |
| 6 zero-copy emit (shard arena), fan-out 8, client GPU on the NIC's socket | (this tree) | **13.88 GB/s** | 0.82 GB/s | 24.56 GB/s | 24.5-28 GB/s | 3.8 (1.8 sequential) |
| 7 idle-pool pre-warm (two servers, four drives, 6+2) | (this tree) | 12.0-12.7 GB/s (first GET after restart 6.2, was 0.4) | 0.67 GB/s | 20.4 GB/s | 22.5 GB/s | 1.86 |
| 7b stream read-ahead (peer chunk 6.5 -> 2 ms) | (this tree) | 11.0-12.2 GB/s | 0.68 GB/s | 21.2 GB/s | 19.6 GB/s | 1.88 |
| 7c shard copy beside the hash | (this tree) | **10.7-14.2 GB/s** | 0.70 GB/s | 19.1 GB/s | 23.1 GB/s | 1.88 |
| 7d asynchronous cuObject transfers | (this tree) | **13.5-15.0 GB/s** | 0.68 GB/s | 21.4 GB/s (18.4 from one server) | **24.6 GB/s** (20.7 from one server) | 1.98 |
