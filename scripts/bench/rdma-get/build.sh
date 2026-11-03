#!/usr/bin/env bash
# Builds the direct cuObject client helpers (needs the CUDA toolkit with libcuobjclient).
set -euo pipefail
cd "$(dirname "$0")"
CUDA=${CUDA_HOME:-/usr/local/cuda}
for h in helper phelper; do
  g++ -O1 -std=c++17 -I"$CUDA/include" $h.cpp -o $h -L"$CUDA/lib64" -lcuobjclient -lcudart -Wl,-rpath,"$CUDA/lib64"
done
echo built
