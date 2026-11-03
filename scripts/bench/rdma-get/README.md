# RDMA GET benchmark tools

Direct cuObject client checks and the four-tray benchmark runner used for
`docs/operations/rdma-get-optimization-benchmarks.md`.

- `helper.cpp` / `driver.py`: one GET with one `x-amz-rdma-token` into a GPU or
  pinned host buffer; verifies bytes against the HTTP body (`VERIFY=0` skips,
  `HTTPCMP=0` skips the HTTP timing run, `BUFSIZE` shrinks the token buffer).
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
