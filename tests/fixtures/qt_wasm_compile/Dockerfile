FROM forderud/qtwasm:latest

# Install redis-tools for connectivity checks and sccache for compiler caching
RUN apt-get update && apt-get install -y --no-install-recommends redis-tools && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ADD https://github.com/mozilla/sccache/releases/download/v0.9.1/sccache-v0.9.1-x86_64-unknown-linux-musl.tar.gz /tmp/sccache.tar.gz
RUN tar xzf /tmp/sccache.tar.gz -C /tmp && \
    mv /tmp/sccache-v0.9.1-x86_64-unknown-linux-musl/sccache /usr/local/bin/ && \
    chmod +x /usr/local/bin/sccache && \
    rm -rf /tmp/sccache*
