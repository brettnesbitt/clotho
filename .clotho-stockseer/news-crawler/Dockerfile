FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy SDK and macros (shared deps)
COPY clotho-sdk/ ./clotho-sdk/
COPY clotho-macros/ ./clotho-macros/

# Copy pipeline source
COPY .clotho-stockseer/news-crawler/ ./.clotho-stockseer/news-crawler/

WORKDIR /build/.clotho-stockseer/news-crawler
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/.clotho-stockseer/news-crawler/target/release/news-crawler /usr/local/bin/

ENV RUST_LOG=info
ENTRYPOINT ["news-crawler"]
