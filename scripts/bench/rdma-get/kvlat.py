"""KV-chunk latency: cold then warm RDMA GETs of N x 48 MiB objects into GPU memory; p50/p99 in ms."""
import os, subprocess, sys, time, statistics
import boto3
from botocore.config import Config
# A bodiless RDMA reply still carries the object's checksum header; do not let boto3 compare it with the empty body.
BOTO_CONFIG = Config(response_checksum_validation="when_required")
S = os.path.dirname(os.path.abspath(__file__))
host = sys.argv[1]; n = int(sys.argv[2]); warm_rounds = int(sys.argv[3]) if len(sys.argv) > 3 else 5
size = 48 << 20
s3 = boto3.client("s3", config=BOTO_CONFIG, endpoint_url=f"http://{host}:9000", aws_access_key_id="rustfsadmin", aws_secret_access_key="rustfsadmin", region_name="us-east-1")
keys = [f"bench/kv/chunk-{i:04d}.bin" for i in range(n)]
if os.environ.get("UPLOAD") == "1":
    from boto3.s3.transfer import TransferConfig
    for k in keys:
        s3.upload_fileobj(__import__("io").BytesIO(os.urandom(size)), "models", k, Config=TransferConfig(multipart_threshold=1 << 30))
    print("uploaded", n)
helper = subprocess.Popen([os.path.join(S, "helper"), str(size), "/dev/null", "gpu"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
line = helper.stdout.readline().strip(); token = line.split(" ", 1)[1]
s3.meta.events.register("before-sign.s3.GetObject", lambda request, **kw: (request.headers.__setitem__("x-amz-rdma-token", token), request.headers.__setitem__("x-amz-content-sha256", "UNSIGNED-PAYLOAD")))
def get(k):
    t = time.perf_counter(); r = s3.get_object(Bucket="models", Key=k); r["Body"].read()
    assert r["ResponseMetadata"]["HTTPHeaders"].get("x-amz-rdma-reply") == "200", k
    return (time.perf_counter() - t) * 1000
cold = [get(k) for k in keys]
warm = [get(k) for _ in range(warm_rounds) for k in keys]
def pct(v, p): v = sorted(v); return v[min(len(v) - 1, int(p * len(v)))]
print(f"KVLAT n={n} cold_p50={statistics.median(cold):.1f}ms cold_p99={pct(cold, .99):.1f}ms warm_p50={statistics.median(warm):.1f}ms warm_p99={pct(warm, .99):.1f}ms warm_GBps={size/ (statistics.median(warm)/1000) / 1e9:.2f}")
helper.stdin.write("go\n"); helper.stdin.flush(); helper.wait()
