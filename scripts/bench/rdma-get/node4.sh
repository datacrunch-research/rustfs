#!/usr/bin/env bash
# One RustFS node per tray (4 local disks), all on port 9000: tray06, tray07, tray17, tray18.
set -euo pipefail
BIN="${RUSTFS_BIN:-/mnt/shared/home/rain/rustfs/target/release/rustfs}"
ROOT=/tmp/rustfs-rdma-4tray
TRANSPORT="${RUSTFS_TEST_TRANSPORT:-rdma}"
HOSTS=(10.13.84.12 10.13.84.6 10.13.84.16 10.13.84.15)
VOLUMES=""
for ip in "${HOSTS[@]}"; do for d in 1 2 3 4; do VOLUMES="$VOLUMES http://$ip:9000$ROOT/disk$d"; done; done
VOLUMES="${VOLUMES# }"
MYIP=$(ip -br addr | awk '/bond0\.225/ {print $3}' | cut -d/ -f1)
mkdir -p "$ROOT"/disk{1,2,3,4}
cd "$ROOT"
RUSTFS_VOLUMES="$VOLUMES" RUSTFS_ADDRESS="$MYIP:9000" RUSTFS_ACCESS_KEY=rustfsadmin RUSTFS_SECRET_KEY=rustfsadmin \
RUSTFS_CONSOLE_ENABLE=false RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true RUSTFS_RPC_SECRET=rdma-4tray-shared-secret \
RUSTFS_INTERNODE_DATA_TRANSPORT="$TRANSPORT" RUSTFS_RDMA_PORT_OFFSET=1000 RUSTFS_OBS_ENDPOINT="" \
RUSTFS_CUOBJECT_ENABLE="${RUSTFS_CUOBJECT_ENABLE:-true}" RUSTFS_CUOBJECT_ADDRS="$MYIP" \
RUSTFS_CUOBJECT_CHUNK_BYTES="${RUSTFS_CUOBJECT_CHUNK_BYTES:-67108864}" RUSTFS_CUOBJECT_POOL_BYTES="${RUSTFS_CUOBJECT_POOL_BYTES:-1073741824}" \
LD_LIBRARY_PATH="${CUOBJSERVER_LIB:-/mnt/shared/home/rain/rustfs/target/cuobjserver/lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
RUST_LOG="${RUST_LOG:-info,rustfs_ecstore::cluster::rpc::rdma=debug,rustfs::app::object::cuobject=debug}" \
nohup "$BIN" > "$ROOT/node.log" 2>&1 &
echo $! > "$ROOT/node.pid"
echo "$(hostname) ($MYIP): started"
