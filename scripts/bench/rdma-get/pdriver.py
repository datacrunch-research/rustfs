"""Parallel ranged cuObject GETs: N tokens over one GPU buffer, N requests in flight, spread over hosts."""
import os, subprocess, sys, time, hashlib, threading
import boto3
from botocore.config import Config
# A bodiless RDMA reply still carries the object's checksum header; do not let boto3 compare it with the empty body.
BOTO_CONFIG = Config(response_checksum_validation="when_required")
S = os.path.dirname(os.path.abspath(__file__))
bucket, key, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
hosts = sys.argv[4].split(",")
mem = sys.argv[5] if len(sys.argv) > 5 else "gpu"
verify = os.environ.get("VERIFY", "1") == "1"
def client(host):
    return boto3.client("s3", config=BOTO_CONFIG, endpoint_url=f"http://{host}:9000", aws_access_key_id="rustfsadmin", aws_secret_access_key="rustfsadmin", region_name="us-east-1")
c0 = client(hosts[0])
total = c0.head_object(Bucket=bucket, Key=key)["ContentLength"]
helper = subprocess.Popen([os.path.join(S, "phelper"), str(total), os.path.join(S, "pdump.bin"), str(n), mem], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
slices = []
while True:
    line = helper.stdout.readline().strip()
    if line == "READY": break
    _, off, ln, tok = line.split(" ")
    slices.append((int(off), int(ln), tok))
results = [None] * len(slices)
def run(i):
    off, ln, tok = slices[i]
    c = client(hosts[i % len(hosts)])
    c.meta.events.register("before-sign.s3.GetObject", lambda request, **kw: (request.headers.__setitem__("x-amz-rdma-token", tok), request.headers.__setitem__("x-amz-content-sha256", "UNSIGNED-PAYLOAD")))
    t = time.time()
    r = c.get_object(Bucket=bucket, Key=key, Range=f"bytes={off}-{off+ln-1}")
    r["Body"].read()
    h = r["ResponseMetadata"]["HTTPHeaders"]
    results[i] = (time.time() - t, h.get("x-amz-rdma-reply"), int(h.get("x-amz-rdma-bytes-transferred", "0")))
t0 = time.time()
threads = [threading.Thread(target=run, args=(i,)) for i in range(len(slices))]
for th in threads: th.start()
for th in threads: th.join()
dt = time.time() - t0
moved = sum(r[2] for r in results)
bad = [r for r in results if r[1] not in ("200", "206")]
helper.stdin.write("go\n"); helper.stdin.flush()
assert helper.stdout.readline().strip() == "DUMPED"; helper.wait()
ok = True
if verify:
    got = open(os.path.join(S, "pdump.bin"), "rb").read()
    ref = c0.get_object(Bucket=bucket, Key=key)["Body"].read()
    ok = got == ref
print(f"PRESULT {'OK' if ok and not bad else 'FAIL'} n={n} hosts={len(hosts)} bytes={moved} seconds={dt:.3f} GBps={moved/dt/1e9:.2f} slowest={max(r[0] for r in results):.3f}s bad={len(bad)}")
