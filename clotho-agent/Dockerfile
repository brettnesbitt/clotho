# Build stage
FROM rust:1.94-slim-bookworm AS builder
WORKDIR /app

# Install system deps for openssl-sys
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release 2>/dev/null || true

# Build actual binary
COPY src ./src
RUN touch src/main.rs
RUN cargo build --release

# Runtime stage - distroless for minimal attack surface
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /app/target/release/clotho-agent /clotho-agent
EXPOSE 8125/udp
EXPOSE 8126/tcp
CMD ["/clotho-agent"]
