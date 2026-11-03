"""cuObject multipart PUT check: each part is an RDMA UploadPart with x-amz-rdma-token and no body."""
import os, subprocess, sys, time
import boto3
from botocore.config import Config
try:
    BOTO_CONFIG = Config(response_checksum_validation="when_required", request_checksum_calculation="when_required")
except TypeError:
    BOTO_CONFIG = Config()
S = os.path.dirname(os.path.abspath(__file__))
endpoint, bucket, key, part_size, parts = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
mem = sys.argv[6] if len(sys.argv) > 6 else "gpu"
s3 = boto3.client("s3", config=BOTO_CONFIG, endpoint_url=endpoint, aws_access_key_id="rustfsadmin", aws_secret_access_key="rustfsadmin", region_name="us-east-1")
upload = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
etags = []; dumps = []; t0 = time.time()
for n in range(1, parts + 1):
    dump = os.path.join(S, f"mpdump-{os.getpid()}-{n}.bin")
    helper = subprocess.Popen([os.path.join(S, "puthelper"), str(part_size), str(n), mem, dump], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    token = helper.stdout.readline().strip().split(" ", 1)[1]
    def add_headers(request, **kw):
        request.headers["x-amz-rdma-token"] = token; request.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
    s3.meta.events.register("before-sign.s3.UploadPart", add_headers)
    tp = time.time()
    r = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=n, Body=b"")
    s3.meta.events.unregister("before-sign.s3.UploadPart", add_headers)
    helper.stdin.write("go\n"); helper.stdin.flush(); helper.wait()
    h = r["ResponseMetadata"]["HTTPHeaders"]
    print(f"part {n}: etag={r['ETag']} reply={h.get('x-amz-rdma-reply')} transferred={h.get('x-amz-rdma-bytes-transferred')}")
    etags.append({"ETag": r["ETag"], "PartNumber": n}); dumps.append(dump)
    print(f"  part {n} upload: {time.time()-tp:.3f}s")
s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={"Parts": etags})
dt = time.time() - t0
got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
exp = b"".join(open(d, "rb").read() for d in dumps)
for d in dumps: os.remove(d)
ok = got == exp
print(f"MPRESULT {'OK' if ok else 'MISMATCH'} parts={parts} part_size={part_size} seconds={dt:.3f} GBps={len(exp)/dt/1e9:.2f}")
sys.exit(0 if ok else 1)
