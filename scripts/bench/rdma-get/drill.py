"""Node-loss drill: GET/PUT latency timeline while one storage node is frozen (SIGSTOP) and after it resumes."""
import os, subprocess, sys, time, threading
import boto3
from botocore.config import Config
S = os.path.dirname(os.path.abspath(__file__))
victim = sys.argv[1]            # ssh host to freeze
host = sys.argv[2]              # S3 endpoint host (client-facing node)
key = sys.argv[3] if len(sys.argv) > 3 else "bench/kv/chunk-0000.bin"
freeze_s = int(os.environ.get("FREEZE_S", "40")); total_s = int(os.environ.get("TOTAL_S", "100"))
try:
    cfg = Config(response_checksum_validation="when_required", read_timeout=120, connect_timeout=10, retries={"max_attempts": 1})
except TypeError:
    cfg = Config(read_timeout=120, connect_timeout=10, retries={"max_attempts": 1})
s3 = boto3.client("s3", config=cfg, endpoint_url=f"http://{host}:9000", aws_access_key_id="rustfsadmin", aws_secret_access_key="rustfsadmin", region_name="us-east-1")
size = s3.head_object(Bucket="models", Key=key)["ContentLength"]
events = []
def log(kind, t0, ok, extra=""):
    events.append((time.time() - start, kind, (time.time() - t0) * 1000, ok, extra))
helper = subprocess.Popen([os.path.join(S, "helper"), str(size), "/dev/null", "host"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
token = helper.stdout.readline().strip().split(" ", 1)[1]
s3.meta.events.register("before-sign.s3.GetObject", lambda request, **kw: (request.headers.__setitem__("x-amz-rdma-token", token), request.headers.__setitem__("x-amz-content-sha256", "UNSIGNED-PAYLOAD")))
start = time.time()
stop = threading.Event()
def getter():
    while not stop.is_set():
        t0 = time.time()
        try:
            r = s3.get_object(Bucket="models", Key=key); r["Body"].read()
            log("GET", t0, r["ResponseMetadata"]["HTTPHeaders"].get("x-amz-rdma-reply") == "200")
        except Exception as e:
            log("GET", t0, False, type(e).__name__)
        time.sleep(0.2)
def putter():
    i = 0
    body = os.urandom(8 << 20)
    while not stop.is_set():
        t0 = time.time()
        try:
            s3.put_object(Bucket="models", Key=f"bench/drill/put-{i}.bin", Body=body); log("PUT", t0, True)
        except Exception as e:
            log("PUT", t0, False, type(e).__name__)
        i += 1; time.sleep(1.0)
threads = [threading.Thread(target=getter), threading.Thread(target=putter)]
for t in threads: t.start()
time.sleep(10)
subprocess.run(["ssh", "-o", "BatchMode=yes", f"rain@{victim}", "pkill -STOP -x rustfs"], check=False); frozen_at = time.time() - start
time.sleep(freeze_s)
subprocess.run(["ssh", "-o", "BatchMode=yes", f"rain@{victim}", "pkill -CONT -x rustfs"], check=False); resumed_at = time.time() - start
time.sleep(max(0, total_s - freeze_s - 10)); stop.set()
for t in threads: t.join()
helper.stdin.write("go\n"); helper.stdin.flush(); helper.wait()
print(f"DRILL frozen_at={frozen_at:.1f}s resumed_at={resumed_at:.1f}s")
phase = lambda at: "before" if at < frozen_at else ("frozen" if at < resumed_at else "after")
import collections, statistics
agg = collections.defaultdict(list)
for at, kind, ms, ok, extra in events:
    agg[(phase(at), kind, "ok" if ok else "fail")].append(ms)
for k in sorted(agg):
    v = agg[k]; print(f"  {k[0]:7s} {k[1]} {k[2]:4s} n={len(v):4d} p50={statistics.median(v):8.1f}ms max={max(v):8.1f}ms")
worst = max((ms for at, kind, ms, ok, extra in events if phase(at) == "frozen" and ok), default=0)
print(f"  longest successful request while frozen: {worst/1000:.1f}s; failures while frozen: {[ (kind, extra) for at, kind, ms, ok, extra in events if phase(at)=='frozen' and not ok][:5]}")
