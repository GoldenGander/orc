#!/usr/bin/env bash
set -euo pipefail

# Wait for Redis to be reachable
for i in $(seq 1 30); do
    if redis-cli -h redis -p 6379 ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
redis-cli -h redis -p 6379 ping

# Start sccache with Redis backend (SCCACHE_REDIS set via container env_vars)
sccache --start-server || true
sccache --zero-stats

# Build the Qt WASM sample project with sccache as compiler launcher
rm -rf /project/build
mkdir -p /project/build
cd /project/build

/opt/Qt/bin/qt-cmake \
    -DQT_CHAINLOAD_TOOLCHAIN_FILE=/opt/wasm-deps/share/cmake/wasm.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER_LAUNCHER=sccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=sccache \
    -G Ninja \
    /src/sample

ninja

# Collect sccache stats as JSON artifact
sccache --show-stats --stats-format json > /output/sccache_stats.json

# Copy WASM build artifacts to output
cp /project/build/app/helloworld.js /output/helloworld.js
cp /project/build/app/helloworld.wasm /output/helloworld.wasm

chmod 644 /output/*

sccache --stop-server || true
