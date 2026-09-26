#!/usr/bin/env bash
# Build and push an arm64 image for the Pi cluster without emulating rustc.
# Usage: ./build-arm64.sh <image:tag>
# Needs: rustup target add aarch64-unknown-linux-gnu; brew install zig;
#        cargo install cargo-zigbuild; Docker running; gcloud auth configure-docker.
set -euo pipefail
IMG="${1:?usage: build-arm64.sh <image:tag>}"
cd "$(dirname "$0")"
# macOS's 256-fd soft limit breaks linking many rlibs (ProcessFdQuotaExceeded).
HARD="$(ulimit -Hn)"
if [ "$HARD" = "unlimited" ] || [ "$HARD" -ge 8192 ]; then ulimit -n 8192; else ulimit -n "$HARD"; fi
# .2.36 pins glibc to bookworm-slim's version; newer symbols would fail at start.
cargo zigbuild --release --target aarch64-unknown-linux-gnu.2.36
mkdir -p dist && cp target/aarch64-unknown-linux-gnu/release/clotho-data-proxy dist/
docker buildx build --platform linux/arm64 -f Dockerfile.cross -t "$IMG" --push .
