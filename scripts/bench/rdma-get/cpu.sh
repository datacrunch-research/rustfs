#!/usr/bin/env bash
# Sums utime+stime (clock ticks) of the rustfs processes on this host.
tot=0; for p in $(pgrep -x rustfs); do set -- $(cat /proc/$p/stat | tr -d '()' | awk '{print $14, $15}'); tot=$((tot + $1 + $2)); done; echo $tot
