FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.toml
COPY src/ src/

RUN cargo build --release

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/clotho-data-proxy /usr/local/bin/clotho-data-proxy

EXPOSE 9090
ENV PORT=9090
ENV RUST_LOG=clotho_data_proxy=info

ENTRYPOINT ["clotho-data-proxy"]
