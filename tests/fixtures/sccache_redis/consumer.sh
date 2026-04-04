#!/usr/bin/env bash
set -euo pipefail

for i in $(seq 1 30); do
    if redis-cli -h redis -p 6379 ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

export SCCACHE_REDIS=redis://redis:6379/
export SCCACHE_DIR=/tmp/sccache-local
export SCCACHE_IDLE_TIMEOUT=0

mkdir -p /tmp/build
cp /src/hello.c /tmp/build/hello.c

sccache --start-server
sccache --zero-stats
sccache gcc -c /tmp/build/hello.c -o /tmp/build/hello.o
sleep 2
sccache --show-stats --stats-format=json > /output/sccache-stats.json
