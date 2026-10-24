# syntax=docker/dockerfile:1

FROM rust:1-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin omni_chat

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/omni_chat /usr/local/bin/omni_chat

ENV RUST_LOG=info
# UI (8080) + 3 node APIs (8081-8083) by default; bump the range if you raise --nodes.
EXPOSE 8080 8081 8082 8083

ENTRYPOINT ["omni_chat"]
CMD ["demo", "--nodes", "3", "--base-port", "8080"]
