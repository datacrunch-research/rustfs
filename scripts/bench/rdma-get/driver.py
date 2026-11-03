"""Direct cuObject GET check: signed GET with x-amz-rdma-token, compare buffer with HTTP GET."""
import os, subprocess, sys, time, hashlib
import boto3, botocore
from botocore.config import Config
# A bodiless RDMA reply still carries the object's checksum header; do not let boto3 compare it with the empty body.
BOTO_CONFIG = Config(response_checksum_validation="when_required")
S = os.path.dirname(os.path.abspath(__file__))
endpoint, bucket, key = sys.argv[1], sys.argv[2], sys.argv[3]
mem = sys.argv[4] if len(sys.argv) > 4 else "gpu"
rng = sys.argv[5] if len(sys.argv) > 5 else None  # "off-end" inclusive, "full" = no Range header
bufsize = int(os.environ.get("BUFSIZE", "0"))
verify = os.environ.get("VERIFY", "1") == "1"
httpcmp = os.environ.get("HTTPCMP", "1") == "1"
s3 = boto3.client("s3", config=BOTO_CONFIG, endpoint_url=endpoint, aws_access_key_id="rustfsadmin", aws_secret_access_key="rustfsadmin", region_name="us-east-1")
head = s3.head_object(Bucket=bucket, Key=key)
total = head["ContentLength"]
if rng and rng != "full":
    off, end = [int(x) for x in rng.split("-")]
else:
    off, end = 0, total - 1
size = end - off + 1
helper = subprocess.Popen([os.path.join(S, "helper"), str(bufsize or size), os.path.join(S, "dump.bin"), mem], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, )
line = helper.stdout.readline().strip()
assert line.startswith("TOKEN "), line
token = line.split(" ", 1)[1]
print("token", token)
def add_headers(request, **kwargs):
    request.headers["x-amz-rdma-token"] = token
    request.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
s3.meta.events.register("before-sign.s3.GetObject", add_headers)
t0 = time.time()
kw = {} if rng == "full" else {"Range": f"bytes={off}-{end}"}
try:
    resp = s3.get_object(Bucket=bucket, Key=key, **kw)
except botocore.exceptions.ClientError as e:
    print("CLIENT_ERROR", e.response["Error"]["Code"], e.response["ResponseMetadata"]["HTTPStatusCode"], e.response["Error"].get("Message"))
    helper.stdin.write("go\n"); helper.stdin.flush(); helper.wait(); sys.exit(3)
hdrs = resp["ResponseMetadata"]["HTTPHeaders"]
body = resp["Body"].read()
dt = time.time() - t0
print("status", resp["ResponseMetadata"]["HTTPStatusCode"], "body_len", len(body), "content-length", hdrs.get("content-length"),
      "reply", hdrs.get("x-amz-rdma-reply"), "transferred", hdrs.get("x-amz-rdma-bytes-transferred"), "etag", hdrs.get("etag"))
helper.stdin.write("go\n"); helper.stdin.flush()
assert helper.stdout.readline().strip() == "DUMPED"
helper.wait()
got = open(os.path.join(S, "dump.bin"), "rb").read() if verify else b""
s3.meta.events.unregister("before-sign.s3.GetObject", add_headers)
dt_http = float("nan")
if verify or httpcmp:
    t1 = time.time()
    ref = s3.get_object(Bucket=bucket, Key=key, **kw)["Body"].read()
    dt_http = time.time() - t1
    got = got[:len(ref)]
ok = (got == ref) if verify else True
print(f"RESULT {'OK' if ok else 'MISMATCH'} size={size} seconds={dt:.3f} GBps={size/dt/1e9:.2f} http_seconds={dt_http:.3f} http_GBps={size/dt_http/1e9:.2f} verified={verify}")
sys.exit(0 if ok else 1)
