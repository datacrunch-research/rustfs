"""cuObject PUT check: signed PUT with x-amz-rdma-token and no body, then verify the stored object."""
import os, subprocess, sys, time
import boto3
from botocore.config import Config
try:
    BOTO_CONFIG = Config(response_checksum_validation="when_required", request_checksum_calculation="when_required")
except TypeError:
    BOTO_CONFIG = Config()
S = os.path.dirname(os.path.abspath(__file__))
endpoint, bucket, key, size = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
mem = sys.argv[5] if len(sys.argv) > 5 else "gpu"
seed = int(os.environ.get("SEED", "7"))
s3 = boto3.client("s3", config=BOTO_CONFIG, endpoint_url=endpoint, aws_access_key_id="rustfsadmin", aws_secret_access_key="rustfsadmin", region_name="us-east-1")
dump = os.path.join(S, f"putdump-{os.getpid()}.bin")
helper = subprocess.Popen([os.path.join(S, "puthelper"), str(size), str(seed), mem, dump], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
line = helper.stdout.readline().strip(); assert line.startswith("TOKEN "), line
token = line.split(" ", 1)[1]
def add_headers(request, **kwargs):
    request.headers["x-amz-rdma-token"] = token
    request.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
s3.meta.events.register("before-sign.s3.PutObject", add_headers)
t0 = time.time()
r = s3.put_object(Bucket=bucket, Key=key, Body=b"")
dt = time.time() - t0
h = r["ResponseMetadata"]["HTTPHeaders"]
print("status", r["ResponseMetadata"]["HTTPStatusCode"], "etag", r.get("ETag"), "reply", h.get("x-amz-rdma-reply"), "transferred", h.get("x-amz-rdma-bytes-transferred"))
helper.stdin.write("go\n"); helper.stdin.flush(); helper.wait()
s3.meta.events.unregister("before-sign.s3.PutObject", add_headers)
exp = open(dump, "rb").read(); os.remove(dump)
if os.environ.get("VERIFY", "1") == "1":
    got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    ok = got == exp
else:
    got = b""; ok = True
print(f"PUTRESULT {'OK' if ok else 'MISMATCH'} size={size} seconds={dt:.3f} GBps={size/dt/1e9:.2f} stored={len(got)}")
sys.exit(0 if ok else 1)
