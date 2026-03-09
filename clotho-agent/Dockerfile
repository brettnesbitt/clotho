# Build stage
FROM rust:1.80-slim as builder
WORKDIR /app

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
CMD ["/clotho-agent"]
