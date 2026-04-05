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

# wasm-opt lives in the emsdk bin dir, not in PATH. Locate it explicitly so we
# can intercept the absolute-path invocations made by em++ during linking.
real_wasm_opt=$(find /emsdk/upstream/bin /emsdk/upstream/emscripten -maxdepth 2 -name 'wasm-opt' -type f 2>/dev/null | head -1)

# Expose sccache to Emscripten through its built-in ccache hook.
shim_dir="/tmp/sccache-shims"
shim_log="/output/compiler_shim.log"
rm -rf "${shim_dir}"
mkdir -p "${shim_dir}"

cat > "${shim_dir}/ccache" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s %s\n' "$1" "${*:2}" >> /output/compiler_shim.log
exec /usr/local/bin/sccache "$@"
EOF

chmod +x "${shim_dir}/ccache"
export PATH="${shim_dir}:${PATH}"
export _EMCC_CCACHE=1

# Wrap wasm-opt at its real absolute path so em++ calls are intercepted.
# em++ resolves wasm-opt from emsdk and calls it directly, bypassing PATH.
if [ -n "${real_wasm_opt}" ]; then
    wasm_opt_real="${real_wasm_opt}.real"
    mv "${real_wasm_opt}" "${wasm_opt_real}"
    cat > "${real_wasm_opt}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf 'wasm-opt %s\n' "\${*}" >> /output/compiler_shim.log
exec /usr/local/bin/sccache "${wasm_opt_real}" "\$@"
EOF
    chmod +x "${real_wasm_opt}"
fi

# Build the Qt WASM sample project with Emscripten routing compiler calls through the shim.
rm -rf /project/build
mkdir -p /project/build
cd /project/build

/opt/Qt/bin/qt-cmake \
    -DQT_CHAINLOAD_TOOLCHAIN_FILE=/opt/wasm-deps/share/cmake/wasm.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -G Ninja \
    /src/sample

ninja -v 2>&1 | tee /output/ninja_verbose.log

# Collect sccache stats as JSON artifact
sccache --show-stats --stats-format json > /output/sccache_stats.json

# Copy WASM build artifacts to output
cp /project/build/app/helloworld.js /output/helloworld.js
cp /project/build/app/helloworld.wasm /output/helloworld.wasm

chmod 644 /output/*

sccache --stop-server || true
