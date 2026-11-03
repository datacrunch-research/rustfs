#!/usr/bin/env bash
# RDMA GET benchmark for the 4-tray GB300 cluster (see docs/operations/rdma-get-optimization-benchmarks.md).
# Restarts the 4-tray cluster with the caller's RUSTFS_* env, then runs the standard GET benchmarks.
# Usage: LABEL=name [RUSTFS_...=...] bench4.sh   -> appends one line per benchmark to $OUT/results.tsv
set -uo pipefail
# Requires: RDMA_BENCH_CUFILE_JSON (cufile.json naming the client RDMA IP), a python with boto3,
# the helpers built with build.sh, and the cluster scripts node4.sh/stop4.sh on every tray.
HERE=$(cd "$(dirname "$0")" && pwd); T=$HERE; OUT=${RDMA_BENCH_OUT:-$HERE/out}; mkdir -p "$OUT"
HOSTS=(pod4-gb300-3-tray06-f3 pod4-gb300-3-tray07-f3 pod4-gb300-3-tray18-f3)
LABEL=${LABEL:-unnamed}; ROUNDS=${ROUNDS:-3}; KEY=${KEY:-bench/rand-2g.bin}; KEY8=${KEY8:-bench/rand-8g.bin}; SKIP_RESTART=${SKIP_RESTART:-0}
ENVS=$(env | grep -E '^RUSTFS_' | tr '\n' ' ')
export CUFILE_ENV_PATH_JSON=${RDMA_BENCH_CUFILE_JSON:?set RDMA_BENCH_CUFILE_JSON}
if [ "$SKIP_RESTART" != 1 ]; then
  for h in "${HOSTS[@]}"; do ssh -o BatchMode=yes rain@$h "bash $T/stop4.sh" >/dev/null 2>&1; done; bash $T/stop4.sh >/dev/null 2>&1
  for h in "${HOSTS[@]}"; do ssh -o BatchMode=yes rain@$h "$ENVS bash $T/node4.sh" >/dev/null 2>&1; done
  env $ENVS bash $T/node4.sh >/dev/null 2>&1
  for i in $(seq 1 60); do curl -s -m 2 -o /dev/null -w "%{http_code}" http://10.13.84.16:9000/health/live 2>/dev/null | grep -q 200 && break; sleep 1; done; sleep 8
fi
cpu_all() { local t=$($T/cpu.sh); for h in "${HOSTS[@]}"; do t=$((t + $(ssh -o BatchMode=yes rain@$h "$T/cpu.sh"))); done; echo $t; }
cd "$HERE"
# warm-up
VERIFY=0 HTTPCMP=0 timeout 600 python3 driver.py http://10.13.84.16:9000 models $KEY gpu full >/dev/null 2>&1
# 1) single-stream RDMA GET into GPU, and HTTP GET, with CPU per GB
C0=$(cpu_all); best_r=0; best_h=0
for r in $(seq 1 $ROUNDS); do
  out=$(VERIFY=0 HTTPCMP=1 timeout 900 python3 driver.py http://10.13.84.16:9000 models $KEY gpu full 2>&1 | grep RESULT)
  rg=$(echo "$out" | sed -n 's/.* GBps=\([0-9.]*\) .*/\1/p'); hg=$(echo "$out" | sed -n 's/.*http_GBps=\([0-9.]*\) .*/\1/p')
  best_r=$(python3 -c "print(max($best_r,$rg))"); best_h=$(python3 -c "print(max($best_h,$hg))")
  echo "  single round $r: rdma=$rg GB/s http=$hg GB/s"
done
C1=$(cpu_all); size=$(python3 -c "import boto3;print(boto3.client('s3',endpoint_url='http://10.13.84.16:9000',aws_access_key_id='rustfsadmin',aws_secret_access_key='rustfsadmin',region_name='us-east-1').head_object(Bucket='models',Key='$KEY')['ContentLength'])")
cpu=$(python3 -c "print(f'{($C1-$C0)/100/(2*$ROUNDS*$size/2**30):.3f}')")
printf "%s\tsingle_rdma_gpu_GBps\t%s\n%s\tsingle_http_GBps\t%s\n%s\tcpu_s_per_GiB_mixed\t%s\n" "$LABEL" "$best_r" "$LABEL" "$best_h" "$LABEL" "$cpu" >> $OUT/results.tsv
# 2) parallel ranged RDMA GETs over the four hosts: 16 into GPU (2 GiB object), 16 and 32 into pinned host (8 GiB object)
for cfg in "16 $KEY gpu" "16 $KEY8 host" "32 $KEY8 host"; do set -- $cfg; n=$1; k=$2; m=$3; best=0
  for r in $(seq 1 $ROUNDS); do
    g=$(VERIFY=0 timeout 900 python3 pdriver.py models $k $n 10.13.84.16,10.13.84.15,10.13.84.12,10.13.84.6 $m 2>&1 | sed -n 's/.* GBps=\([0-9.]*\) .*/\1/p'); best=$(python3 -c "print(max($best,${g:-0}))")
  done
  echo "  parallel n=$n $k $m: $best GB/s"; printf "%s\tparallel%s_%s_%s_GBps\t%s\n" "$LABEL" "$n" "$(basename $k .bin)" "$m" "$best" >> $OUT/results.tsv
done
# 3) stage summary (if the node can write it)
if [ -f /var/log/rustfs/get-stage-summary.log ]; then tail -1 /var/log/rustfs/get-stage-summary.log > $OUT/stages-$LABEL.log; fi
echo "DONE $LABEL: single_rdma=$best_r http=$best_h cpu_s_per_GiB=$cpu"
