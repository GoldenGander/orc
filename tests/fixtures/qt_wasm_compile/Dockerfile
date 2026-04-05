FROM rust:1.85-bookworm AS sccache-builder
ARG SCCACHE_REPO
ARG SCCACHE_REV

RUN set -eux; \
    test -n "${SCCACHE_REPO}"; \
    test -n "${SCCACHE_REV}"; \
    cargo install --git "${SCCACHE_REPO}" --rev "${SCCACHE_REV}" --locked sccache

FROM forderud/qtwasm:latest

ARG SCCACHE_REPO
ARG SCCACHE_REV

# Install redis-tools for connectivity checks. This fixture uses a forked
# sccache binary pinned to a git revision rather than the upstream release.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        redis-tools \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=sccache-builder /usr/local/cargo/bin/sccache /usr/local/bin/sccache
