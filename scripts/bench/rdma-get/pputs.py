"""Aggregate RDMA PUT: N concurrent 48 MiB cuObject PUTs from one client (separate processes)."""
import os, re, statistics, subprocess, sys, time
S = os.path.dirname(os.path.abspath(__file__))
endpoint, n, size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
mem = sys.argv[4] if len(sys.argv) > 4 else "gpu"
t0 = time.time()
procs = [subprocess.Popen(["python3", S + "/putdriver.py", endpoint, "models", f"bench/pput/obj-{i:03d}.bin", str(size), mem], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=dict(os.environ, SEED=str(i + 1), VERIFY="0")) for i in range(n)]
outs = [p.communicate()[0] for p in procs]
dt = time.time() - t0
ok = sum("PUTRESULT OK" in o for o in outs)
secs = [float(re.search(r"seconds=([0-9.]+)", o).group(1)) for o in outs if "PUTRESULT" in o]
print(f"PPUT n={n} size={size} ok={ok} wall={dt:.2f}s put_median={statistics.median(secs):.3f}s put_max={max(secs):.3f}s aggregate_over_max_GBps={n*size/max(secs)/1e9:.2f}")
for o in outs:
    if "PUTRESULT OK" not in o: print(o[-400:])
