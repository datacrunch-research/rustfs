# RDMA GET benchmark tools

Direct cuObject client checks and the four-tray benchmark runner used for
`docs/operations/rdma-get-optimization-benchmarks.md`.

- `helper.cpp` / `driver.py`: one GET with one `x-amz-rdma-token` into a GPU or
  pinned host buffer; verifies bytes against the HTTP body (`VERIFY=0` skips,
  `HTTPCMP=0` skips the HTTP timing run, `BUFSIZE` shrinks the token buffer).
- `puthelper.cpp` / `putdriver.py <endpoint> <bucket> <key> <size> [gpu|host]`: cuObject PUT; the helper fills a buffer with a seeded pattern and mints a `CUOBJ_PUT` token, the driver sends the bodiless signed PUT and verifies the stored object over HTTP.
- `mpdriver.py <endpoint> <bucket> <key> <part size> <parts> [gpu|host]`: multipart upload with one RDMA `UploadPart` per part; `pputs.py <endpoint> <n> <size> [gpu|host]`: n concurrent RDMA PUTs.
- `phelper.cpp` / `pdriver.py`: N ranged GETs in flight into one buffer, each
  slice registered on its own (a registration must stay under 4 GiB), spread
  over the listed hosts.
- `bench4.sh`: restarts the cluster with the caller's `RUSTFS_*` environment via
  `node4.sh`/`stop4.sh`, then records single-stream, HTTP, CPU per GiB and
  parallel results to `out/results.tsv`.
- `kvlat.py`: cold/warm latency of N single-stripe 48 MiB objects (`UPLOAD=1` creates them).
- `drill.py <victim ssh host> <endpoint host> [key]`: node-loss drill; freezes rustfs on the victim with SIGSTOP for `FREEZE_S` seconds while a GET and a PUT loop run, prints per-phase latency and the longest stall.
- `cpu.sh`: CPU ticks of the local rustfs processes (summed across trays).

Build the helpers with `build.sh`; run with `RDMA_BENCH_CUFILE_JSON` pointing
at a `cufile.json` whose `rdma_dev_addr_list` names the client's RDMA IP.

## Building the server for iteration

The release profile (thin LTO, one codegen unit) rebuilds the two largest
crates from scratch on every edit, 20 minutes or more on the GB300 trays.
For A/B runs use the `fast` profile (release optimization, no LTO, incremental),
linked with the toolchain's lld because the larger binary exceeds GNU ld's
direct-branch range on aarch64:

```bash
GCCLD=$(rustc --print sysroot)/lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld
RUSTFLAGS="-Clink-arg=-fuse-ld=lld -Clink-arg=-B$GCCLD" \
  cargo build --profile fast -p rustfs --features rdma,cuobject
RUSTFS_BIN=target/fast/rustfs bash node4.sh
```

Measured: full build 25 min, rebuild after an edit in `rustfs` or `ecstore`
under 3 min. Numbers published in the benchmark record come from the release
profile unless the row says otherwise.
