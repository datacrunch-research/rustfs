#!/usr/bin/env bash
ROOT=/tmp/rustfs-rdma-4tray
[ -f "$ROOT/node.pid" ] && kill "$(cat "$ROOT/node.pid")" 2>/dev/null; rm -f "$ROOT/node.pid"
sleep 2; pkill -x rustfs 2>/dev/null || true; echo "$(hostname): stopped"
