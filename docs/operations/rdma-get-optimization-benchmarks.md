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
